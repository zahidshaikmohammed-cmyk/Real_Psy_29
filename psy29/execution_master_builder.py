"""Standalone, source-bounded PSY29 execution enrichment.

Layer 2 only consumes an already-produced Layer-1 master payload.  It performs
no network I/O, never mutates the input, and never repairs or fabricates market
data.  Every value in the enrichment is either copied from the source or
calculated deterministically from source fields/candles.
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
_SCHEMA_VERSION = "1.1"


def load_master(source: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(source, Mapping):
        return copy.deepcopy(dict(source))
    loaded = json.loads(Path(source).read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("Master JSON root must be an object.")
    return loaded


def build_execution_master(
    source: Mapping[str, Any] | str | Path,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Return an enriched deep copy of *source*; never mutate the input."""
    master = load_master(source)
    enrichment = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": _source_metadata(master),
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
    Path(output_path).write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return result


def _stock_items(master: Mapping[str, Any]):
    stocks = master.get("stocks")
    if not isinstance(stocks, Mapping):
        data = master.get("data")
        stocks = data.get("stocks") if isinstance(data, Mapping) else None
    return stocks.items() if isinstance(stocks, Mapping) else []


def _source_metadata(master: Mapping[str, Any]) -> dict[str, Any]:
    timestamp = master.get("timestamp")
    return {
        "trading_date": master.get("trading_date"),
        "timestamp": timestamp,
        "data_source_status": master.get("data_source_status"),
        "market_session_status": master.get("market_session_status"),
        "data_age_seconds": _age_from_now(timestamp),
        "status": "AVAILABLE" if timestamp else "UNAVAILABLE",
    }


def _age_from_now(value: Any) -> float | None:
    ts = _parse_ts(value)
    if ts is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _market_context(master: Mapping[str, Any]) -> dict[str, Any]:
    ctx = master.get("market_context")
    if not isinstance(ctx, Mapping):
        ctx = master.get("market")
    ctx = ctx if isinstance(ctx, Mapping) else {}

    def item(name: str):
        value = ctx.get(name)
        return {"status": "AVAILABLE", "value": copy.deepcopy(value)} if value is not None else {"status": "UNAVAILABLE", "value": None}

    return {
        "NIFTY": item("NIFTY"),
        "BANKNIFTY": item("BANKNIFTY"),
        "INDIA_VIX": item("INDIA_VIX"),
        "BREADTH": item("BREADTH"),
        "MARKET_REGIME": item("MARKET_REGIME"),
        "timestamp": _first(ctx, "timestamp") or master.get("timestamp"),
        "status": "AVAILABLE" if ctx else "UNAVAILABLE",
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
            "sector_timestamp": None,
            "sector_regime": None,
            "sector_vwap": None,
            "sector_strength": None,
            "status": "UNAVAILABLE",
        }
    return {"status": "AVAILABLE" if any(v["sector"] is not None for v in items.values()) else "UNAVAILABLE", "items": items}


def _stock_enrichment(master: Mapping[str, Any], stock: Any) -> dict[str, Any]:
    stock = stock if isinstance(stock, Mapping) else {}
    candles = _candle_map(stock)
    reference_ts = _reference_timestamp(master, stock)
    completed = {tf: _completed_candles(candles.get(tf, []), reference_ts, minutes) for tf, minutes in _TIMEFRAMES.items()}
    integrity = {tf: _candle_integrity(candles.get(tf, []), tf) for tf in _TIMEFRAMES}
    indicators = {tf: _indicators(candles.get(tf, []), tf, reference_ts) for tf in _TIMEFRAMES}
    opening = _opening_range(stock, candles.get("1m", []), reference_ts, master.get("trading_date"))
    quality = _execution_quality(stock, candles, integrity, indicators, reference_ts)
    return {
        "last_tick_metadata": _last_tick(stock),
        "data_age": _data_age(stock, reference_ts),
        "completed_candles": completed,
        "indicators": indicators,
        "opening_range": opening,
        "support_resistance": _support_resistance(candles.get("1m", []), reference_ts),
        "liquidity": _liquidity(stock, candles.get("1m", []), reference_ts),
        "relative_strength": _relative_strength(stock),
        "sector_context": _stock_sector(stock, master),
        "execution_quality": quality,
        "candle_integrity": integrity,
        "execution_summary": _execution_summary(stock, completed, indicators, opening, integrity, quality),
    }


def _stock_sector(stock: Mapping[str, Any], master: Mapping[str, Any]) -> dict[str, Any]:
    symbol = None
    for key in ("symbol", "trading_symbol", "security_symbol"):
        if stock.get(key) is not None:
            symbol = stock[key]
            break
    mapping = master.get("sector_mapping")
    mapping = mapping if isinstance(mapping, Mapping) else {}
    sector = mapping.get(symbol) if symbol is not None else stock.get("sector")
    if sector is None:
        sector = stock.get("sector")
    return {"sector": sector, "sector_index": None, "sector_timestamp": None, "sector_regime": None, "sector_vwap": None, "sector_strength": None, "status": "UNAVAILABLE"}


def _reference_timestamp(master: Mapping[str, Any], stock: Mapping[str, Any]) -> datetime | None:
    return _parse_ts(_first(stock, "timestamp", "last_tick_timestamp", "last_timestamp") or master.get("timestamp"))


def _last_tick(stock: Mapping[str, Any]) -> dict[str, Any]:
    tick = _first(stock, "last_tick", "ltp", "last_price", "current_price")
    ts = _first(stock, "last_tick_timestamp", "last_timestamp")
    epoch = _first(stock, "last_tick_epoch", "epoch")
    return {"last_tick": tick, "timestamp": ts, "epoch": epoch, "status": "AVAILABLE" if tick is not None or ts is not None or epoch is not None else "UNAVAILABLE"}


def _data_age(stock: Mapping[str, Any], reference_ts: datetime | None):
    tick_ts = _parse_ts(_first(stock, "last_tick_timestamp", "last_timestamp", "timestamp"))
    if tick_ts is None or reference_ts is None:
        return {"seconds": None, "reference_timestamp": None, "status": "UNAVAILABLE"}
    return {"seconds": max(0.0, (reference_ts - tick_ts).total_seconds()), "reference_timestamp": reference_ts.isoformat(), "status": "CALCULATED"}


def _completed_candles(rows: list, reference_ts: datetime | None, minutes: int):
    normal = _normalise_candles(rows)
    complete = []
    if reference_ts is not None:
        for row in normal:
            ts = _parse_ts(row.get("timestamp"))
            if ts is not None and ts + timedelta(minutes=minutes) <= reference_ts:
                complete.append(row)
    latest = max(complete, key=lambda r: _parse_ts(r["timestamp"])) if complete else None
    return {
        "latest_completed_timestamp": latest.get("timestamp") if latest else None,
        "latest_completed_epoch": latest.get("epoch") if latest else None,
        "candle_complete": True if latest else (False if normal and reference_ts is not None else None),
        "status": "AVAILABLE" if latest else ("FORMING" if normal and reference_ts is not None else "UNAVAILABLE"),
    }


def _indicators(rows: list, tf: str, reference_ts: datetime | None):
    usable = []
    interval = timedelta(minutes=_TIMEFRAMES[tf])
    for row in _normalise_candles(rows):
        ts = _parse_ts(row.get("timestamp"))
        if ts is None or reference_ts is None or ts + interval > reference_ts:
            continue
        close, high, low, volume = (_num(row.get(k)) for k in ("close", "high", "low", "volume"))
        if None not in (close, high, low, volume) and volume >= 0:
            usable.append((ts, close, high, low, volume))
    usable.sort(key=lambda x: x[0])
    if not usable:
        return {k: {"value": None, "status": "INSUFFICIENT_DATA"} for k in ("VWAP", "EMA9", "EMA20")}
    total_volume = sum(x[4] for x in usable)
    vwap = sum(((x[2] + x[3] + x[1]) / 3.0) * x[4] for x in usable) / total_volume if total_volume > 0 else None
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


def _support_resistance(rows: list, reference_ts: datetime | None):
    normal = _normalise_candles(rows)
    if reference_ts is not None:
        normal = [r for r in normal if (_parse_ts(r.get("timestamp")) or reference_ts) + timedelta(minutes=1) <= reference_ts]
    highs = [_num(r.get("high")) for r in normal]
    lows = [_num(r.get("low")) for r in normal]
    swing_highs, swing_lows = [], []
    for i in range(1, len(normal) - 1):
        h, l = highs[i], lows[i]
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


def _liquidity(stock: Mapping[str, Any], rows: list, reference_ts: datetime | None):
    volume = _num(_first(stock, "volume", "total_volume"))
    avg = _num(_first(stock, "average_volume", "avg_volume"))
    if avg is None:
        completed = _completed_volume(rows, reference_ts)
        if completed:
            avg = sum(completed) / len(completed)
    return {"volume": volume, "average_volume": avg, "bid": None, "ask": None, "spread": None, "bid_quantity": None, "ask_quantity": None, "imbalance": None, "status": "PARTIAL" if volume is not None or avg is not None else "UNAVAILABLE"}


def _completed_volume(rows: list, reference_ts: datetime | None) -> list[float]:
    out = []
    if reference_ts is None:
        return out
    for r in _normalise_candles(rows):
        ts = _parse_ts(r.get("timestamp"))
        v = _num(r.get("volume"))
        if ts is not None and v is not None and ts + timedelta(minutes=1) <= reference_ts:
            out.append(v)
    return out


def _relative_strength(stock: Mapping[str, Any]):
    rs = stock.get("relative_strength")
    if isinstance(rs, Mapping) and rs.get("value") is not None and rs.get("benchmark") is not None:
        return {"value": rs["value"], "benchmark": rs["benchmark"], "timeframe": rs.get("timeframe"), "status": "CALCULATED"}
    return {"value": None, "benchmark": None, "timeframe": None, "status": "UNAVAILABLE"}


def _execution_quality(stock, candles, integrity, indicators, reference_ts):
    reasons, missing = [], []
    if _first(stock, "last_tick", "ltp", "last_price", "current_price") is None:
        reasons.append("missing live tick")
        missing.append("last_tick")
    for tf in _TIMEFRAMES:
        if not candles.get(tf):
            reasons.append(f"missing {tf} timeframe")
            missing.append(f"candles.{tf}")
        elif integrity[tf]["status"] == "GAP":
            reasons.append(f"candle gap in {tf}")
    reasons.append("unavailable liquidity depth")
    missing.extend(["bid", "ask", "bid_quantity", "ask_quantity"])
    if _relative_strength(stock)["status"] == "UNAVAILABLE":
        reasons.append("unavailable benchmark")
        missing.append("relative_strength.benchmark")
    has_core = _first(stock, "last_tick", "ltp", "last_price", "current_price") is not None and all(indicators[tf]["status"] != "INSUFFICIENT_DATA" for tf in ("1m", "5m"))
    return {"status": "READY" if has_core and not any(integrity[tf]["status"] == "GAP" for tf in _TIMEFRAMES) else ("DEGRADED" if candles else "UNAVAILABLE"), "reasons": reasons, "missing_fields": sorted(set(missing)), "source_timestamp": reference_ts.isoformat() if reference_ts else None}


def _execution_summary(stock, completed, indicators, opening, integrity, quality):
    price = _num(_first(stock, "current_price", "last_tick", "ltp", "last_price"))
    def relation(key):
        value = indicators["1m"][key]["value"]
        if price is None or value is None:
            return "UNKNOWN"
        return "ABOVE" if price > value else "BELOW" if price < value else "AT"
    ema9 = indicators["1m"]["EMA9"]["value"]
    ema20 = indicators["1m"]["EMA20"]["value"]
    trend = "UNKNOWN"
    if ema9 is not None and ema20 is not None:
        trend = "BULLISH" if ema9 > ema20 else "BEARISH" if ema9 < ema20 else "NEUTRAL"
    gap = any(integrity[tf]["status"] == "GAP" for tf in _TIMEFRAMES)
    return {
        "data_status": stock.get("data_source_status", "AVAILABLE"),
        "latest_completed_1m": completed["1m"]["latest_completed_timestamp"],
        "latest_completed_5m": completed["5m"]["latest_completed_timestamp"],
        "trend_alignment": trend,
        "price_vs_vwap": relation("VWAP"),
        "price_vs_ema9": relation("EMA9"),
        "price_vs_ema20": relation("EMA20"),
        "structure_status": "GAP" if gap else ("OPENING_RANGE_COMPLETE" if opening["status"] == "COMPLETE" else "UNKNOWN"),
        "liquidity_status": "PARTIAL" if _num(_first(stock, "volume", "total_volume")) is not None else "UNAVAILABLE",
        "candle_integrity_status": "GAP" if gap else ("OK" if any(integrity[tf]["status"] == "OK" for tf in _TIMEFRAMES) else "UNAVAILABLE"),
        "execution_quality": quality["status"],
    }


def _candle_integrity(rows: list, tf: str):
    normal = _normalise_candles(rows)
    stamps = [_parse_ts(r.get("timestamp")) for r in normal]
    stamps = [x for x in stamps if x is not None]
    gaps = []
    step = timedelta(minutes=_TIMEFRAMES[tf])
    for a, b in zip(stamps, stamps[1:]):
        delta = b - a
        if delta != step:
            gaps.append({"from": a.isoformat(), "to": b.isoformat(), "missing_intervals": max(0, int(delta.total_seconds() // step.total_seconds()) - 1)})
    return {"count": len(normal), "latest_timestamp": normal[-1].get("timestamp") if normal else None, "gaps": gaps, "status": "GAP" if gaps else ("OK" if normal else "UNAVAILABLE")}


def _opening_range(stock, rows, reference_ts, trading_date):
    normal = _normalise_candles(rows)
    high = low = None
    try:
        target_date = datetime.fromisoformat(str(trading_date)).date() if trading_date else None
    except ValueError:
        target_date = None
    selected = []
    for row in normal:
        ts = _parse_ts(row.get("timestamp"))
        if ts is None:
            continue
        local = ts.astimezone(IST)
        if target_date is not None and local.date() != target_date:
            continue
        if (local.hour, local.minute) >= (9, 15) and (local.hour, local.minute) < (9, 30):
            h, l = _num(row.get("high")), _num(row.get("low"))
            if h is not None and l is not None:
                selected.append((ts, h, l))
    if selected:
        high = max(x[1] for x in selected)
        low = min(x[2] for x in selected)
    period_start = f"{trading_date}T09:15:00+05:30" if trading_date else "09:15"
    period_end = f"{trading_date}T09:30:00+05:30" if trading_date else "09:30"
    end_dt = _parse_ts(period_end)
    complete = reference_ts is not None and end_dt is not None and reference_ts >= end_dt
    ltp = _num(_first(stock, "current_price", "last_tick", "ltp", "last_price"))
    if high is None or low is None:
        breakout = "UNAVAILABLE"
    elif ltp is None:
        breakout = "UNKNOWN"
    elif ltp > high:
        breakout = "ABOVE_HIGH"
    elif ltp < low:
        breakout = "BELOW_LOW"
    else:
        breakout = "INSIDE_RANGE"
    return {"period_start": period_start, "period_end": period_end, "status": "COMPLETE" if complete else "FORMING", "high": high, "low": low, "range": high - low if high is not None and low is not None else None, "breakout_status": breakout}


def _candle_map(stock):
    raw = stock.get("candles", {})
    if not isinstance(raw, Mapping):
        return {tf: [] for tf in _TIMEFRAMES}
    return {tf: (raw.get(tf) if isinstance(raw.get(tf), list) else []) for tf in _TIMEFRAMES}


def _normalise_candles(rows):
    out = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        item = copy.deepcopy(dict(row))
        ts = _parse_ts(item.get("timestamp"))
        if ts is not None:
            item["timestamp"] = ts.isoformat()
            item.setdefault("epoch", int(ts.timestamp()))
        out.append(item)
    return out


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _first(mapping: Mapping[str, Any], *keys: str):
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _unique_sorted(values: list[float]) -> list[float]:
    return sorted(set(values))


__all__ = ["build_execution_master", "load_master", "write_execution_master"]
