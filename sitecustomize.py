"""PSY29 runtime hardening loaded automatically by Python at process startup."""
import sys
import threading
import time

_PATCHED = False
_ORIGINAL_START = threading.Thread.start


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


def _patched_start(self, *args, **kwargs):
    target = getattr(self, "_target", None)
    name = getattr(self, "name", "")
    if name == "psy29-collector" and getattr(target, "__name__", "") == "collector_thread":
        self._target = _supervise_psy29_collector
        self._args = ()
        self._kwargs = {}
    return _ORIGINAL_START(self, *args, **kwargs)


if not _PATCHED:
    threading.Thread.start = _patched_start
    _PATCHED = True
