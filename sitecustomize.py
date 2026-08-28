"""PSY29 runtime hardening loaded automatically by Python at process startup."""
from __future__ import annotations

import asyncio
import os
import random
import sys
import threading
import time

_PATCHED = False
_ORIGINAL_START = threading.Thread.start

# Dhan can return HTTP 429 when WebSocket handshakes are retried too quickly.
# The application-level reconnect loop is intentionally retained; this guard
# prevents a broker rate-limit from becoming a 3-second reconnect storm.
try:
    import websockets
    _ORIGINAL_WS_CONNECT = websockets.connect

    class _DhanRateLimitedConnect:
        def __init__(self, *args, **kwargs):
            self._args = args
            self._kwargs = kwargs
            self._connection = None

        async def __aenter__(self):
            delay = 15.0
            for attempt in range(8):
                self._connection = _ORIGINAL_WS_CONNECT(*self._args, **self._kwargs)
                try:
                    return await self._connection.__aenter__()
                except Exception as exc:
                    text = str(exc).lower()
                    is_429 = "429" in text or "too many requests" in text
                    try:
                        await self._connection.__aexit__(type(exc), exc, exc.__traceback__)
                    except Exception:
                        pass
                    if not is_429 or attempt == 7:
                        raise
                    await asyncio.sleep(min(60.0, delay) + random.uniform(0.0, 2.0))
                    delay = min(60.0, delay * 2.0)

        async def __aexit__(self, exc_type, exc, tb):
            if self._connection is None:
                return False
            return await self._connection.__aexit__(exc_type, exc, tb)

    def _guarded_ws_connect(*args, **kwargs):
        return _DhanRateLimitedConnect(*args, **kwargs)

    websockets.connect = _guarded_ws_connect
except Exception:
    pass


def _install_dhan_auth_guard():
    """Serialize TOTP login and never hammer Dhan with the same OTP window."""
    app = sys.modules.get("__main__")
    if app is None or not hasattr(app, "generate_access_token"):
        return
    if getattr(app, "_psy29_dhan_auth_guard_installed", False):
        return

    original_generate = app.generate_access_token
    auth_lock = threading.Lock()

    def guarded_generate_access_token():
        with auth_lock:
            secret = os.getenv("DHAN_TOTP_SECRET")
            if secret:
                normalized = "".join(secret.split()).upper()
                if normalized != secret:
                    os.environ["DHAN_TOTP_SECRET"] = normalized

            try:
                return original_generate()
            except Exception as exc:
                if "Invalid TOTP" not in str(exc):
                    raise

                # Dhan TOTP is a 30-second code. A restarted Render instance
                # can collide with another instance that just consumed the
                # current code. Wait for the next code before one retry rather
                # than repeatedly submitting the same code every few seconds.
                wait = max(1.0, 31.0 - (time.time() % 30.0))
                log = getattr(app, "log", None)
                if log:
                    log.warning("Dhan TOTP rejected; waiting for next TOTP window before one retry")
                time.sleep(wait)
                try:
                    return original_generate()
                except Exception as retry_exc:
                    if "Invalid TOTP" in str(retry_exc) and log:
                        log.error("Dhan TOTP retry failed; preserving authentication failure state")
                    raise

    app.generate_access_token = guarded_generate_access_token
    app._psy29_dhan_auth_guard_installed = True


def _supervise_psy29_collector():
    app = sys.modules.get("__main__")
    if app is None:
        return

    state = app.state
    lock = app.lock
    log = app.log
    now_ist = app.now_ist
    in_session = app.in_session
    is_weekday = app.is_weekday
    generate_access_token = app.generate_access_token
    initialize_session = app.initialize_session
    websocket_loop = app.websocket_loop
    asyncio = app.asyncio
    backoff = 5

    while True:
        try:
            now = now_ist()
            if not is_weekday(now):
                with lock:
                    state["market_session_status"] = "WEEKEND"
                    state["source_status"] = "WAITING_FOR_SESSION"
                time.sleep(60)
                continue

            if not in_session(now):
                with lock:
                    state["market_session_status"] = app.session_status(now)
                    state["source_status"] = "WAITING_FOR_SESSION"
                time.sleep(30)
                continue

            with lock:
                state["market_session_status"] = "OPEN"
                state["source_status"] = "AUTHENTICATING"

            token, expiry = generate_access_token()
            with lock:
                state["access_token_expiry"] = expiry
                state["source_status"] = "INITIALIZING"

            initialize_session(token)

            if not in_session(now_ist()):
                with lock:
                    state["source_status"] = "POST_CLOSE"
                continue

            with lock:
                state["source_status"] = "CONNECTING"
            asyncio.run(websocket_loop(token))

            if in_session(now_ist()):
                with lock:
                    state["source_status"] = "RECONNECTING"
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
            else:
                with lock:
                    state["source_status"] = "POST_CLOSE"
                backoff = 5
                time.sleep(30)

        except Exception as exc:
            log.exception("PSY29 collector supervisor failure: %s", exc)
            with lock:
                state["source_status"] = "RECONNECTING" if in_session(now_ist()) else "WAITING_FOR_SESSION"
                state["last_update"] = now_ist().isoformat()
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _start_psy29_github_master_publisher():
    if not os.getenv("GITHUB_LIVE_DATA_TOKEN"):
        return
    try:
        from psy29.github_master_publisher_runtime import start
        start()
    except Exception as exc:
        print(f"PSY29 GitHub master publisher import failed: {type(exc).__name__}", flush=True)


def _patched_start(self, *args, **kwargs):
    target = getattr(self, "_target", None)
    name = getattr(self, "name", "")
    if name == "psy29-collector" and getattr(target, "__name__", "") == "collector_thread":
        self._target = _supervise_psy29_collector
        self._args = ()
        self._kwargs = {}
    if name == "psy29-supervisor":
        _install_dhan_auth_guard()
        _start_psy29_github_master_publisher()
    return _ORIGINAL_START(self, *args, **kwargs)


if not _PATCHED:
    threading.Thread.start = _patched_start
    _PATCHED = True


def _github_master_bootstrap():
    # Render starts runner.py as __main__; this is only a fallback for startup
    # paths where the supervisor thread is not created immediately.
    for _ in range(120):
        process = sys.modules.get("__main__")
        if process is not None and hasattr(process, "_machine_payload"):
            _start_psy29_github_master_publisher()
            return
        time.sleep(0.5)


def _runner_supervisor_bootstrap():
    """Guarantee the live-data supervisor exists even if FastAPI startup hooks race."""
    for _ in range(120):
        process = sys.modules.get("__main__")
        if process is not None and hasattr(process, "_supervisor"):
            if any(t.name == "psy29-supervisor" and t.is_alive() for t in threading.enumerate()):
                return
            if getattr(process, "_psy29_supervisor_bootstrap_started", False):
                return
            process._psy29_supervisor_bootstrap_started = True
            threading.Thread(target=process._supervisor, daemon=True, name="psy29-supervisor").start()
            return
        time.sleep(0.5)


threading.Thread(target=_github_master_bootstrap, name="psy29-github-master-bootstrap", daemon=True).start()
threading.Thread(target=_runner_supervisor_bootstrap, name="psy29-supervisor-bootstrap", daemon=True).start()
