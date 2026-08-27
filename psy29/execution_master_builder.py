"""Standalone PSY29 execution-master enrichment.

This module is deliberately independent of the live acquisition path.
It reads a PSY29 master payload and returns a deep-copied, additive
execution enrichment. It performs no network I/O.
"""

from __future__ import annotations

import copy
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

IST = timezone(timedelta(hours=5, minutes=30))
_TIMEFRAMES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
_SCHEMA_VERSION = "1.0"


def load_master(source: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(source, Mapping):
        return copy.deepcopy(dict(source))
    return json.loads(Path(source).read_text(encoding="utf-8"))


def build_execution_master(
    source: Mapping[str, Any] | str | Path,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Return an enriched COPY of *source*; never mutate the input."""
    master = load_master(source)
    enrichment = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at,
        "market_context": _market_context(master),
        "sectors": _sector_context(master),
        "stocks": {},
    }

    for symbol, stock in _stock_items(master):
        enrichment["stocks"][symbol] = _stock_enrichment(master, stock)

    master["execution_enrichment"] = enrichment
    return master


def write_execution_master(
    source: Mapping[str, Any] | str | Path,
    output_path: str | Path,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    result = build_execution_master(source, generated_at=generated_at)
    Path(output_path).write_text(
        json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return result


def _stock_items(master: Mapping[str, Any]):
    stocks = master.get("stocks")
    if not isinstance(stocks, Mapping):
        data = master.get("data")
        stocks = data.get("stocks") if isinstance(data, Mapping) else None
    return stocks.items() if isinstance(stocks, Mapping) else []


def _market_context(master: Mapping[str, Any]) -> dict[str, Any]:
    ctx = master.get("market_context")
    if not isinstance(ctx, Mapping):
        ctx = master.get("market")
    ctx = ctx if isinstance(ctx, Mapping) else {}

    def available(name: str):
        value = ctx.get(name)
        if value is None:
            return {"status": "UNAVAILABLE", "value": None}
        return {"status": "AVAILABLE", "value": copy.deepcopy(value)}

    breadth = ctx.get("BREADTH")
    if breadth is None:
        breadth_out = {"status": "UNAVAILABLE", "value": None}
    else:
        breadth_out = {"status": "AVAILABLE", "value": copy.deepcopy(breadth)}

    return {
        "NIFTY": available("NIFTY"),
        "BANKNIFTY": available("BANKNIFTY"),
        "INDIA_VIX": available("INDIA_VIX"),
        "BREADTH": breadth_out,
        "MARKET_REGIME": available("MARKET_REGIME"),
        "timestamp": _first(ctx, "timestamp", master.get("timestamp")),
    }


def _sector_context(master: Mapping[str, Any]) -> dict[str, Any]:
    static = master.get("sector_mapping")
    static = static if isinstance(static, Mapping) else {}
    items: dict[str, Any] = {}
    for symbol, stock in _stock_items(master):
        sector = static.get(symbol)
        if sector is None and isinstance(stock, Mapping):
            sector = stock.get("sector")
        items[symbol] = {
            "sector": sector,
            "sector_index": None,
            "sector_regime": None,
            "sector_vwap": None,
            "sector_strength": None,
            "status": "UNAVAILABLE",
        }
    return {"status": "AVAILABLE" if items else "UNAVAILABLE", "items": items}


def _stock_enrichment(master: Mapping[str, Any], stock: Any) -> dict[str, Any]:
    stock = stock if isinstance(stock, Mapping) else {}
    candles = _candle_map(stock)
    reference_ts = _reference_timestamp(master, stock)

    return {
        "last_tick_metadata": _last_tick(stock),
        "data_age": _data_age(stock, reference_ts),
        "completed_candles": _completed_candles(candles, reference_ts),
        "indicators": {
            tf: _indicators(candles.get(tf, []), tf, reference_ts)
            for tf in _TIMEFRAMES
        },
        "support_resistance": _support_resistance(candles.get("1m", [])),
        "liquidity": _liquidity(stock),
        "relative_strength": _relative_strength(stock),
        "execution_quality": _execution_quality(stock, candles, reference_ts),
        "candle_integrity": {
            tf: _candle_integrity(candles.get(tf, []), tf)
            for tf in _TIMEFRAMES
        },
        "opening_range": _opening_range(stock, candles.get("1m", []), reference_ts),
    }


def _last_tick(stock: Mapping[str, Any]) -> dict[str, Any]:
    tick = _first(stock, "last_tick", "ltp", "last_price")
    ts = _first(stock, "last_tick_timestamp", "last_timestamp", "timestamp")
    epoch = _first(stock, "last_tick_epoch", "epoch")
    if tick is None and ts is None and epoch is None:
        return {"last_tick": None, "timestamp": None, "epoch": None, "status": "UNAVAILABLE"}
    return {"last_tick": tick, "timestamp": ts, "epoch": epoch, "status": "AVAILABLE"}


def _data_age(stock: Mapping[str, Any], reference_ts: datetime | None):
    ts = _parse_ts(_first(stock, "last_tick_timestamp", "last_timestamp", "timestamp"))
    if ts is None or reference_ts is None:
        return {"seconds": None, "reference_timestamp": None, "status": "UNAVAILABLE"}
    return {
        "seconds": max(0.0, (reference_ts - ts).total_seconds()),
        "reference_timestamp": reference_ts.isoformat(),
        "status": "CALCULATED",
    }


def _completed_candles(candles: Mapping[str, list], reference_ts: datetime | None):
    out = {}
    for tf, minutes in _TIMEFRAMES.items():
        rows = _normalise_candles(candles.get(tf, []))
        completed = [
            r for r in rows
            if reference_ts is not None
            and _parse_ts(r.get("timestamp")) is not None
            and _parse_ts(r["timestamp"]) + timedelta(minutes=minutes) <= reference_ts
        ]
        latest = completed[-1] if completed else None
        out[tf] = {
            "latest_completed_timestamp": latest.get("timestamp") if latest else None,
            "latest_completed_epoch": latest.get("epoch") if latest else None,
            "candle_complete": True if latest else (False if rows else None),
            "status": "AVAILABLE" if latest else ("UNAVAILABLE" if not rows else "FORMING"),
        }
    return out


def _indicators(rows: list, tf: str, reference_ts: datetime | None):
    usable = []
    for r in _normalise_candles(rows):
        ts = _parse_ts(r.get("timestamp"))
        if ts is None or reference_ts is None or ts + timedelta(minutes=_TIMEFRAMES[tf]) <= reference_ts:
            if ts is not None and reference_ts is None:
                continue
            if ts is not None and reference_ts is not None and ts + timedelta(minutes=_TIMEFRAMES[tf]) > reference_ts:
                continue
        close = _num(r.get("close"))
        high, low = _num(r.get("high")), _num(r.get("low"))
        volume = _num(r.get("volume"))
        if close is not None:
            usable.append((r, close, high, low, volume))

    if not usable:
        return {k: {"value": None, "status": "INSUFFICIENT_DATA"} for k in ("VWAP", "EMA9", "EMA20")}

    pv = 0.0
    vol = 0.0
    for _, close, high, low, volume in usable:
        typical = (high + low + close) / 3.0 if high is not None and low is not None else close
        if volume is not None and volume >= 0:
            pv += typical * volume
            vol += volume
    vwap = pv / vol if vol > 0 else None

    closes = [x[1] for x in usable]
    return {
        "VWAP": {"value": vwap, "status": "CALCULATED" if vwap is not None else "INSUFFICIENT_DATA"},
        "EMA9": {"value": _ema(closes, 9), "status": "CALCULATED" if len(closes) >= 9 else "INSUFFICIENT_DATA"},
        "EMA20": {"value": _ema(closes, 20), "status": "CALCULATED" if len(closes) >= 20 else "INSUFFICIENT_DATA"},
    }


def _ema(values: list[float], period: int):
    if len(values) < period:
        return None
    ema = sum(values[:period]) / period
    alpha = 2.0 / (period + 1)
    for value in values[period:]:
        ema = alpha * value + (1 - alpha) * ema
    return ema


def _support_resistance(rows: list):
    rows = _normalise_candles(rows)
    highs = [_num(r.get("high")) for r in rows]
    lows = [_num(r.get("low")) for r in rows]
    swing_highs, swing_lows = [], []
    for i in range(1, len(rows) - 1):
        h = highs[i]
        l = lows[i]
        if h is not None and highs[i - 1] is not None and highs[i + 1] is not None and h > highs[i - 1] and h > highs[i + 1]:
            swing_highs.append(h)
        if l is not None and lows[i - 1] is not None and lows[i + 1] is not None and l < lows[i - 1] and l < lows[i + 1]:
            swing_lows.append(l)
    return {
        "supports": _unique_sorted(swing_lows),
        "resistances": _unique_sorted(swing_highs),
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
        "breakout_levels": _unique_sorted(swing_highs),
        "retest_levels": _unique_sorted(swing_lows),
    }


def _liquidity(stock: Mapping[str, Any]):
    volume = _num(_first(stock, "volume"))
    avg = _num(_first(stock, "average_volume", "avg_volume"))
    return {
        "volume": volume,
        "average_volume": avg,
        "bid": None,
        "ask": None,
        "spread": None,
        "bid_quantity": None,
        "ask_quantity": None,
        "imbalance": None,
        "status": "PARTIAL" if volume is not None or avg is not None else "UNAVAILABLE",
    }


def _relative_strength(stock: Mapping[str, Any]):
    rs = stock.get("relative_strength")
    if isinstance(rs, Mapping) and rs.get("benchmark") is not None and rs.get("value") is not None:
        return {
            "value": rs["value"],
            "benchmark": rs["benchmark"],
            "timeframe": rs.get("timeframe"),
            "status": "CALCULATED",
        }
    return {"value": None, "benchmark": None, "timeframe": None, "status": "UNAVAILABLE"}


def _execution_quality(stock, candles, reference_ts):
    reasons, missing = [], []
    if _first(stock, "last_tick", "ltp", "last_price") is None:
        reasons.append("missing live tick")
        missing.append("last_tick")
    for tf in _TIMEFRAMES:
        if not candles.get(tf):
            reasons.append(f"missing {tf} timeframe")
            missing.append(f"candles.{tf}")
        elif _candle_integrity(candles[tf], tf)["gaps"]:
            reasons.append(f"candle gap in {tf}")
    reasons.append("unavailable liquidity depth")
    missing.extend(["bid", "ask", "bid_quantity", "ask_quantity"])
    if _relative_strength(stock)["status"] == "UNAVAILABLE":
        reasons.append("unavailable benchmark")
        missing.append("relative_strength.benchmark")
    return {
        "status": "AVAILABLE" if not reasons else "PARTIAL",
        "reasons": reasons,
        "missing_fields": sorted(set(missing)),
        "timestamp": reference_ts.isoformat() if reference_ts else None,
    }


def _candle_integrity(rows: list, tf: str):
    norm = _normalise_candles(rows)
    stamps = [_parse_ts(r.get("timestamp")) for r in norm]
    stamps = [x for x in stamps if x is not None]
    gaps = []
    step = timedelta(minutes=_TIMEFRAMES[tf])
    for a, b in zip(stamps, stamps[1:]):
        if b - a != step:
            gaps.append({"from": a.isoformat(), "to": b.isoformat()})
    return {
        "count": len(norm),
        "latest_timestamp": norm[-1].get("timestamp") if norm else None,
        "gaps": gaps,
        "status": "GAP" if gaps else ("AVAILABLE" if norm else "UNAVAILABLE"),
    }


def _opening_range(stock, rows, reference_ts):
    original = stock.get("opening_range")
    original = original if isinstance(original, Mapping) else {}
    high = _num(original.get("high"))
    low = _num(original.get("low"))
    start = _first(original, "period_start", "start")
    end = _first(original, "period_end", "end")
    if high is None or low is None:
        return {
            "period_start": start, "period_end": end, "status": "UNAVAILABLE",
            "high": high, "low": low, "range": None, "breakout_status": "UNAVAILABLE",
        }

    candle_times = [_parse_ts(r.get("timestamp")) for r in _normalise_candles(rows)]
    candle_times = [x for x in candle_times if x is not None]
    if start is None and candle_times:
        start = candle_times[0].isoformat()
    if end is None and candle_times:
        inferred_end = candle_times[0] + timedelta(minutes=15)
        if reference_ts is not None and reference_ts >= inferred_end:
            end = inferred_end.isoformat()

    complete = original.get("status") not in {"FORMING", "INCOMPLETE"}
    if end and reference_ts:
        parsed_end = _parse_ts(end)
        if parsed_end is not None:
            complete = reference_ts >= parsed_end

    return {
        "period_start": start, "period_end": end,
        "status": "COMPLETE" if complete else "FORMING",
        "high": high, "low": low, "range": high - low,
        "breakout_status": "UNAVAILABLE",
    }


def _candle_map(stock):
    raw = stock.get("candles", {})
    if not isinstance(raw, Mapping):
        return {}
    out = {}
    for tf in _TIMEFRAMES:
        value = raw.get(tf, [])
        out[tf] = value if isinstance(value, list) else []
    return out


def _normalise_candles(rows):
    out = []
    for r in rows:
        if isinstance(r, Mapping):
            out.append(r)
        elif isinstance(r, (list, tuple)) and len(r) >= 6:
            out.append({"timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]})
    return out


def _reference_timestamp(master, stock):
    for value in (_first(master, "timestamp", "last_update"),):
        parsed = _parse_ts(value)
        if parsed:
            return parsed
    return None


def _parse_ts(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=IST)
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=IST)
    except ValueError:
        return None


def _num(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _first(mapping, *keys):
    for key in keys:
        if isinstance(mapping, Mapping) and mapping.get(key) is not None:
            return mapping[key]
    return None


def _unique_sorted(values):
    return sorted(set(v for v in values if v is not None))


__all__ = ["build_execution_master", "load_master", "write_execution_master"]
