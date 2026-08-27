from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _candle_ts(c: dict[str, Any]) -> datetime | None:
    for key in ("timestamp", "time", "datetime", "ts"):
        if key in c:
            return _parse_ts(c[key])
    return None


def _num(c: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = c.get(key)
        if isinstance(value, (int, float)) and value == value:
            return float(value)
        if isinstance(value, str):
            try:
                x = float(value)
                if x == x:
                    return x
            except ValueError:
                pass
    return None


def _candles(stock: dict[str, Any], tf: str) -> list[dict[str, Any]]:
    raw = (stock.get("candles") or {}).get(tf, [])
    return [c for c in raw if isinstance(c, dict) and _candle_ts(c) is not None]


def _completed(candles: list[dict[str, Any]], source_ts: datetime | None, interval: int) -> dict[str, Any]:
    if not candles:
        return {"latest_completed_timestamp": None, "latest_completed_epoch": None, "candle_complete": None, "status": "UNAVAILABLE"}
    completed = []
    for c in candles:
        ts = _candle_ts(c)
        if ts is None:
            continue
        if source_ts is None or ts.timestamp() + interval <= source_ts.timestamp():
            completed.append((ts, c))
    if not completed:
        return {"latest_completed_timestamp": None, "latest_completed_epoch": None, "candle_complete": False, "status": "FORMING"}
    ts = max(x[0] for x in completed)
    return {"latest_completed_timestamp": ts.isoformat(), "latest_completed_epoch": int(ts.timestamp()), "candle_complete": True, "status": "OK"}


def _integrity(candles: list[dict[str, Any]], interval: int) -> dict[str, Any]:
    if not candles:
        return {"count": 0, "latest_timestamp": None, "gaps": [], "status": "UNAVAILABLE"}
    ordered = sorted((_candle_ts(c) for c in candles if _candle_ts(c)), key=lambda x: x)
    gaps = []
    for a, b in zip(ordered, ordered[1:]):
        delta = int(round((b - a).total_seconds()))
        if delta > interval:
            gaps.append({"from": a.isoformat(), "to": b.isoformat(), "missing_intervals": delta // interval - 1})
    return {"count": len(ordered), "latest_timestamp": ordered[-1].isoformat(), "gaps": gaps, "status": "GAP" if gaps else "OK"}


def _indicators(candles: list[dict[str, Any]], source_ts: datetime | None, interval: int) -> dict[str, Any]:
    usable = []
    for c in candles:
        ts = _candle_ts(c)
        if ts is None or (source_ts and ts.timestamp() + interval > source_ts.timestamp()):
            continue
        close = _num(c, "close", "c")
        high = _num(c, "high", "h")
        low = _num(c, "low", "l")
        volume = _num(c, "volume", "v")
        if close is not None and high is not None and low is not None and volume is not None:
            usable.append((ts, close, high, low, volume))
    usable.sort()
    if not usable:
        return {"vwap": None, "ema9": None, "ema20": None, "status": "INSUFFICIENT_DATA"}
    pv = sum(((h + l + c) / 3) * v for _, c, h, l, v in usable)
    vol = sum(v for *_, v in usable)
    vwap = pv / vol if vol else None
    closes = [x[1] for x in usable]
    ema9 = ema20 = None
    if len(closes) >= 9:
        ema9 = closes[0]
        k = 2 / 10
        for x in closes[1:]: ema9 = x * k + ema9 * (1-k)
    if len(closes) >= 20:
        ema20 = closes[0]
        k = 2 / 21
        for x in closes[1:]: ema20 = x * k + ema20 * (1-k)
    return {"vwap": vwap, "ema9": ema9, "ema20": ema20, "status": "OK"}


def _opening_range(stock: dict[str, Any], source_ts: datetime | None) -> dict[str, Any]:
    candles = _candles(stock, "1m")
    values = []
    for c in candles:
        ts = _candle_ts(c)
        if not ts: continue
        local = ts.astimezone(timezone.utc)
        # Session timestamps are expected from Layer 1; use the documented 09:15-09:30 window.
        if local.hour == 3 and 45 <= local.minute < 60:
            h, l = _num(c, "high", "h"), _num(c, "low", "l")
            if h is not None and l is not None: values.append((ts, h, l))
    complete = source_ts is not None and source_ts.astimezone(timezone.utc).hour >= 4 and source_ts.astimezone(timezone.utc).minute >= 0
    high = max((x[1] for x in values), default=None)
    low = min((x[2] for x in values), default=None)
    return {"period_start": "09:15", "period_end": "09:30", "status": "COMPLETE" if complete else "FORMING", "high": high, "low": low, "range": (high-low) if high is not None and low is not None else None, "breakout_status": "UNKNOWN"}


def _levels(candles: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for c in candles:
        h, l = _num(c, "high", "h"), _num(c, "low", "l")
        if h is not None and l is not None: rows.append((h, l))
    highs = sorted({h for h, _ in rows}, reverse=True)[:5]
    lows = sorted({l for _, l in rows})[:5]
    return {"supports": lows, "resistances": highs, "swing_highs": highs, "swing_lows": lows, "breakout_levels": highs + lows, "retest_levels": highs + lows}


def build_execution_enrichment(payload: dict[str, Any]) -> dict[str, Any]:
    source = deepcopy(payload)
    source_dt = _parse_ts(source.get("timestamp"))
    stocks = {}
    for symbol, stock in (source.get("stocks") or {}).items():
        stock = stock if isinstance(stock, dict) else {}
        comp = {tf: _completed(_candles(stock, tf), source_dt, seconds) for tf, seconds in TIMEFRAMES.items()}
        integrity = {tf: _integrity(_candles(stock, tf), seconds) for tf, seconds in TIMEFRAMES.items()}
        indicators = {tf: _indicators(_candles(stock, tf), source_dt, seconds) for tf, seconds in TIMEFRAMES.items()}
        ltp = _num(stock, "current_price", "last_price", "ltp")
        volume = _num(stock, "volume", "total_volume")
        stocks[symbol] = {
            "completed_candles": comp,
            "candle_integrity": integrity,
            "indicators": indicators,
            "opening_range": _opening_range(stock, source_dt),
            "support_resistance": _levels(_candles(stock, "5m")),
            "liquidity": {"volume": volume, "average_volume": None, "bid": None, "ask": None, "spread": None, "bid_quantity": None, "ask_quantity": None, "imbalance": None, "status": "PARTIAL" if volume is not None else "UNAVAILABLE"},
            "relative_strength": {"value": None, "benchmark": None, "timeframe": None, "status": "UNAVAILABLE"},
            "sector_context": {"sector": None, "sector_index": None, "sector_timestamp": None, "sector_regime": None, "sector_vwap": None, "sector_strength": None, "status": "UNAVAILABLE"},
            "execution_quality": {"status": "READY" if ltp is not None and all(v["status"] == "OK" for v in integrity.values()) else "DEGRADED", "reasons": [], "missing_fields": [], "source_timestamp": source.get("timestamp")},
            "execution_summary": {"data_status": source.get("data_source_status", "UNAVAILABLE"), "latest_completed_1m": comp["1m"]["latest_completed_timestamp"], "latest_completed_5m": comp["5m"]["latest_completed_timestamp"], "trend_alignment": "UNKNOWN", "price_vs_vwap": "UNKNOWN", "price_vs_ema9": "UNKNOWN", "price_vs_ema20": "UNKNOWN", "structure_status": "UNKNOWN", "liquidity_status": "PARTIAL" if volume is not None else "UNAVAILABLE", "candle_integrity_status": "GAP" if any(v["status"] == "GAP" for v in integrity.values()) else "OK", "execution_quality": "READY" if ltp is not None else "DEGRADED"},
        }
    generated = {"trading_date": source.get("trading_date"), "timestamp": source.get("timestamp"), "data_source_status": source.get("data_source_status"), "market_session_status": source.get("market_session_status"), "data_age_seconds": None, "status": "AVAILABLE" if source.get("data_source_status") == "LIVE" else "UNAVAILABLE"}
    return {"source": generated, "market_context": {"nifty": None, "banknifty": None, "india_vix": None, "breadth": None, "market_regime": None, "timestamp": None, "status": "UNAVAILABLE"}, "stocks": stocks}


def enrich_payload(payload: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(payload)
    result["execution_enrichment"] = build_execution_enrichment(payload)
    return result
