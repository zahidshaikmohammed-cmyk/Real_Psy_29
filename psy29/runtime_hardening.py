from __future__ import annotations

import importlib.abc
import importlib.machinery
import sys
from datetime import datetime, time as dtime
from typing import Any

OPEN_MINUTE = 9 * 60 + 15
CLOSE_MINUTE = 15 * 60 + 30
CANDLE_SOURCE = "DHAN_V2_REST_INTRADAY_1M_COMPLETED"

def _minute_of(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute

def _strict_completed_rows(rows: list[dict], trading_date: str, cutoff_epoch: int, validate_ohlcv_row) -> list[dict]:
    if not isinstance(rows, list): raise ValueError("intraday response is not a list")
    valid: list[dict] = []; previous = None; cutoff_seen = False
    for row in rows:
        if not isinstance(row, dict): raise ValueError("malformed intraday candle")
        epoch = int(row["epoch"])
        if epoch > cutoff_epoch: cutoff_seen = True; continue
        if cutoff_seen: raise ValueError("intraday candles are not chronological")
        validate_ohlcv_row(row, trading_date, session_only=True)
        if previous is not None:
            if epoch == previous: raise ValueError("duplicate Dhan candle")
            if epoch < previous: raise ValueError("non-chronological Dhan candle")
            if epoch != previous + 60: raise ValueError(f"missing 1m candle between {previous} and {epoch}")
        previous = epoch; valid.append(dict(row))
    if not valid: return []
    expected_first = int(datetime.fromisoformat(f"{trading_date}T09:15:00+05:30").timestamp())
    if valid[0]["epoch"] != expected_first: raise ValueError("canonical 1m history does not start at 09:15")
    if valid[-1]["epoch"] != cutoff_epoch: raise ValueError("canonical 1m history does not reach requested completed minute")
    if len(valid) != len(range(expected_first, cutoff_epoch + 60, 60)): raise ValueError("canonical 1m history has a missing minute")
    return valid

def _patch_main(main: Any) -> None:
    main.MARKET_OPEN = (9, 15); main.MARKET_CLOSE = (15, 30)
    try:
        import psy29.data_integrity as integrity
        integrity.IST_OPEN = dtime(9, 15); integrity.IST_CLOSE = dtime(15, 30)
    except Exception: pass
    original_normalize = getattr(main, "normalize_market", None)
    if original_normalize is not None and not getattr(main, "_psy29_normalize_hardened", False):
        def normalize_market_compat(raw):
            payload = dict(raw); payload.pop("candle_policy", None); return original_normalize(payload)
        main.normalize_market = normalize_market_compat; main._psy29_normalize_hardened = True

def _patch_runner(runner: Any) -> None:
    main = runner.main; _patch_main(main)
    if getattr(runner, "_psy29_runtime_hardened", False): return
    def completed_rows(rows, trading_date, cutoff_epoch):
        try: return _strict_completed_rows(rows, trading_date, cutoff_epoch, runner.validate_ohlcv_row)
        except Exception as exc: main.log.warning("Canonical candle gate rejected history: %s", exc); return []
    runner._completed_rows = completed_rows
    original_seed = runner._seed_state
    def strict_seed(token, security_map):
        original_seed(token, security_map)
        with main.lock:
            stocks = main.state.get("stocks", {}); failures = []
            for symbol in main.STOCKS:
                stock = stocks.get(symbol) or {}; prev = stock.get("previous_day") or {}; price = stock.get("current_price"); volume = stock.get("volume")
                if price is None or volume is None or int(volume) <= 0 or any(prev.get(k) is None for k in ("high", "low", "close")): failures.append(symbol)
            if failures:
                main.state["source_status"] = "ERROR"; raise runner.DataIntegrityError("strict seed rejected incomplete stocks: " + ",".join(failures))
    runner._seed_state = strict_seed
    original_payload = runner._machine_payload
    def hardened_payload():
        payload = original_payload(); now = main.now_ist()
        if main.in_session(now) and _minute_of(now) >= 9 * 60 + 16:
            unsafe = []; cutoff = int(now.replace(second=0, microsecond=0).timestamp()) - 60; expected_first = int(now.replace(hour=9, minute=15, second=0, microsecond=0).timestamp())
            for symbol, stock in (payload.get("stocks") or {}).items():
                rows = list((stock.get("candles") or {}).get("1m", []))
                if not rows: unsafe.append(f"{symbol}: missing canonical 1m history"); continue
                epochs = [int(r.get("epoch")) for r in rows]
                if epochs != list(range(epochs[0], epochs[-1] + 60, 60)): unsafe.append(f"{symbol}: canonical 1m gap")
                if epochs[-1] > cutoff: unsafe.append(f"{symbol}: uncompleted 1m candle exposed")
                if epochs[0] != expected_first: unsafe.append(f"{symbol}: canonical history does not start at 09:15")
                if epochs[-1] < cutoff: unsafe.append(f"{symbol}: canonical history is behind the latest completed minute")
            if unsafe:
                payload["data_source_status"] = "DATA_UNSAFE"
                payload["diagnostic"] = {"status":"ERROR","error_code":"DATA_INTEGRITY_FAILURE","error_message":"; ".join(unsafe),"stage":"PUBLIC_GATE","affected_stocks":[x.split(":",1)[0] for x in unsafe],"recovery_action":"REFETCH_COMPLETED_DHAN_1M_CANDLES","data_safe":False}
        return payload
    runner._machine_payload = hardened_payload; runner._psy29_runtime_hardened = True
    main.log.info("PSY29 runtime hardening loaded: strict 1m continuity, strict seed, 15:30 session, safe public gate")

def _patch_builder(builder: Any) -> None:
    if getattr(builder, "_psy29_builder_hardened", False): return
    def execution_quality(stock, candles, integrity, indicators, reference_ts):
        reasons, missing = [], []
        if builder._first(stock, "last_tick", "ltp", "last_price", "current_price") is None: reasons.append("missing live tick"); missing.append("last_tick")
        for tf in builder._TIMEFRAMES:
            if not candles.get(tf): reasons.append(f"missing {tf} timeframe"); missing.append(f"candles.{tf}")
            elif integrity[tf]["status"] == "GAP": reasons.append(f"candle gap in {tf}")
        reasons.append("unavailable liquidity depth"); missing.extend(["bid","ask","bid_quantity","ask_quantity"])
        if builder._relative_strength(stock)["status"] == "UNAVAILABLE": reasons.append("unavailable benchmark"); missing.append("relative_strength.benchmark")
        indicator_core = True
        for tf in ("1m","5m"):
            values = indicators.get(tf,{})
            for name in ("VWAP","EMA9","EMA20"):
                if values.get(name,{}).get("status") == "INSUFFICIENT_DATA": indicator_core = False
        has_core = builder._first(stock,"last_tick","ltp","last_price","current_price") is not None and indicator_core; integrity_gap = any(integrity[tf]["status"] == "GAP" for tf in builder._TIMEFRAMES)
        return {"status":"READY" if has_core and not integrity_gap else ("DEGRADED" if candles else "UNAVAILABLE"),"reasons":reasons,"missing_fields":sorted(set(missing)),"source_timestamp":reference_ts.isoformat() if reference_ts else None}
    builder._execution_quality = execution_quality; builder._psy29_builder_hardened = True

class _PostImportLoader(importlib.abc.Loader):
    def __init__(self, original, fullname): self.original=original; self.fullname=fullname
    def create_module(self, spec):
        create=getattr(self.original,"create_module",None); return create(spec) if create else None
    def exec_module(self, module):
        self.original.exec_module(module)
        if self.fullname == "runner": _patch_runner(module)
        elif self.fullname == "psy29.execution_master_builder": _patch_builder(module)
    def __getattr__(self,name): return getattr(self.original,name)

class _PostImportFinder(importlib.abc.MetaPathFinder):
    TARGETS={"runner","psy29.execution_master_builder"}
    def find_spec(self, fullname, path=None, target=None):
        if fullname not in self.TARGETS: return None
        spec=importlib.machinery.PathFinder.find_spec(fullname,path)
        if spec is None or spec.loader is None: return spec
        spec.loader=_PostImportLoader(spec.loader,fullname); return spec

def _patch_uvicorn_run() -> None:
    try: import uvicorn
    except Exception: return
    if getattr(uvicorn,"_psy29_run_hardened",False): return
    original_run=uvicorn.run
    def hardened_run(*args,**kwargs):
        process=sys.modules.get("__main__")
        if process is not None and str(getattr(process,"__file__","" )).endswith("runner.py"): _patch_runner(process)
        return original_run(*args,**kwargs)
    uvicorn.run=hardened_run; uvicorn._psy29_run_hardened=True

# Preserve the existing hardening and additionally remove the unsafe future-tick rejection.
_original_patch_runner = _patch_runner
def _patch_runner(runner: Any) -> None:
    _original_patch_runner(runner)
    main = runner.main
    def valid_tick_clock_skew_tolerant(tick, now):
        import math
        p=float(tick["ltp"]); v=int(tick["volume"]); q=max(0,int(tick["ltq"]))
        if not math.isfinite(p) or p<=0 or p>10_000_000: raise runner.DataIntegrityError("non-finite/out-of-range equity price")
        if v<0: raise runner.DataIntegrityError("invalid cumulative volume")
        dt=datetime.fromtimestamp(int(tick["ltt"]), main.IST)
        if dt.date()!=now.date(): raise runner.DataIntegrityError("tick timestamp outside trading date")
        if not main.MARKET_OPEN <= (dt.hour,dt.minute) < main.MARKET_CLOSE: raise runner.DataIntegrityError("tick timestamp outside NSE session")
        return p,v,q,dt
    runner.valid_tick = valid_tick_clock_skew_tolerant
    main.log.info("PSY29 live tick gate loaded: Dhan LTT clock skew no longer discards genuine session ticks")

def install() -> None:
    main=sys.modules.get("main")
    if main is not None: _patch_main(main)
    _patch_uvicorn_run()
    process=sys.modules.get("__main__")
    if process is not None and str(getattr(process,"__file__","")).endswith("runner.py"): _patch_runner(process)
    if not any(isinstance(f,_PostImportFinder) for f in sys.meta_path): sys.meta_path.insert(0,_PostImportFinder())
