from __future__ import annotations

import importlib.abc
import importlib.machinery
import sys
from datetime import datetime, timedelta, time as dtime
from typing import Any

OPEN_MINUTE = 9 * 60 + 15
CLOSE_MINUTE = 15 * 60 + 30
CANDLE_SOURCE = "DHAN_V2_REST_INTRADAY_1M_COMPLETED"
Dhan_LTT_LOCAL_EPOCH_OFFSET = 5 * 3600 + 30 * 60


def _patch_main(main: Any) -> None:
    main.MARKET_OPEN = (9, 15)
    main.MARKET_CLOSE = (15, 30)
    try:
        import psy29.data_integrity as integrity
        integrity.IST_OPEN = dtime(9, 15)
        integrity.IST_CLOSE = dtime(15, 30)
    except Exception:
        pass


def _patch_runner(runner: Any) -> None:
    main = runner.main
    _patch_main(main)
    if getattr(runner, "_psy29_runtime_hardened", False):
        return

    # Dhan's live-feed LTT has been observed on this service as a local-IST epoch
    # (5h30 ahead of Unix UTC epoch). Correct it only when the raw timestamp is
    # outside the NSE session and the -05:30 interpretation lands inside it.
    original_parse = runner.parse_packets
    def parse_packets_fixed(message):
        for tick in original_parse(message):
            raw_ltt = int(tick["ltt"])
            raw_dt = datetime.fromtimestamp(raw_ltt, main.IST)
            shifted_ltt = raw_ltt - Dhan_LTT_LOCAL_EPOCH_OFFSET
            shifted_dt = datetime.fromtimestamp(shifted_ltt, main.IST)
            raw_in_session = main.MARKET_OPEN <= (raw_dt.hour, raw_dt.minute) < main.MARKET_CLOSE
            shifted_in_session = main.MARKET_OPEN <= (shifted_dt.hour, shifted_dt.minute) < main.MARKET_CLOSE
            if (not raw_in_session) and shifted_in_session and shifted_dt.date() == raw_dt.date():
                tick["ltt"] = shifted_ltt
                tick["timestamp_normalization"] = "DHAN_LOCAL_EPOCH_TO_UTC"
            yield tick
    runner.parse_packets = parse_packets_fixed

    # Never reject a genuine Dhan tick merely because the feed clock is ahead.
    # Candle publication remains gated by completed source minutes; no synthetic
    # candle is created from Render/server time.
    def valid_tick_clock_safe(tick, now):
        import math
        p = float(tick["ltp"])
        v = int(tick["volume"])
        q = max(0, int(tick["ltq"]))
        if not math.isfinite(p) or p <= 0 or p > 10_000_000:
            raise runner.DataIntegrityError("non-finite/out-of-range equity price")
        if v < 0:
            raise runner.DataIntegrityError("invalid cumulative volume")
        dt = datetime.fromtimestamp(int(tick["ltt"]), main.IST)
        if dt.date() != now.date():
            raise runner.DataIntegrityError("tick timestamp outside trading date")
        if not main.MARKET_OPEN <= (dt.hour, dt.minute) < main.MARKET_CLOSE:
            raise runner.DataIntegrityError("tick timestamp outside NSE session")
        return p, v, q, dt
    runner.valid_tick = valid_tick_clock_safe

    main.log.info("PSY29 runtime hardening loaded: Dhan LTT normalization + completed-candle gate")
    runner._psy29_runtime_hardened = True


class _PostImportLoader(importlib.abc.Loader):
    def __init__(self, original, fullname):
        self.original = original
        self.fullname = fullname

    def create_module(self, spec):
        create = getattr(self.original, "create_module", None)
        return create(spec) if create else None

    def exec_module(self, module):
        self.original.exec_module(module)
        if self.fullname == "runner":
            _patch_runner(module)

    def __getattr__(self, name):
        return getattr(self.original, name)


class _PostImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != "runner":
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None or spec.loader is None:
            return spec
        spec.loader = _PostImportLoader(spec.loader, fullname)
        return spec


def _patch_uvicorn_run() -> None:
    try:
        import uvicorn
    except Exception:
        return
    if getattr(uvicorn, "_psy29_run_hardened", False):
        return
    original_run = uvicorn.run
    def hardened_run(*args, **kwargs):
        process = sys.modules.get("__main__")
        if process is not None and str(getattr(process, "__file__", "")).endswith("runner.py"):
            _patch_runner(process)
        return original_run(*args, **kwargs)
    uvicorn.run = hardened_run
    uvicorn._psy29_run_hardened = True


def install() -> None:
    main = sys.modules.get("main")
    if main is not None:
        _patch_main(main)
    _patch_uvicorn_run()
    process = sys.modules.get("__main__")
    if process is not None and str(getattr(process, "__file__", "")).endswith("runner.py"):
        _patch_runner(process)
    if not any(isinstance(f, _PostImportFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _PostImportFinder())
