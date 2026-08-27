from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from math import isfinite
from typing import Any

TIMEFRAMES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
UNAVAILABLE = "UNAVAILABLE"


def _num(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _completed(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    out = []
    for i, row in enumerate(rows):
        ts = _timestamp(row.get("timestamp"))
        if ts is None:
            continue
        # A candle is complete when the next validated candle exists at least
        # one full timeframe later. The final candle is therefore not assumed
        # complete merely because it exists.
        if i + 1 < len(rows):
            nxt = _timestamp(rows[i + 1].get("timestamp"))
            if nxt is not None and (nxt - ts).total_seconds() >= minutes * 60:
                out.append(row)
    return out


def _gaps(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    gaps = []
    previous = None
    for row in rows:
        ts = _timestamp(row.get("timestamp"))
        if ts is None:
            continue
        if previous is not None:
            delta = (ts - previous).total_seconds() / 60
            if delta > minutes:
                gaps.append({"from": previous.isoformat(), "to": ts.isoformat(), "minutes": delta})
        previous = ts
    return gaps


def _ema(rows: list[dict[str, Any]], period: int) -> float | None:
    closes = [_num(r.get("close")) for r in rows]
    closes = [x for x in closes if x is not None]
    if not closes:
        return None
    k = 2 / (period + 1)
    value = closes[0]
    for close in closes[1:]:
        value = close * k + value * (1 - k)
    return value


def _vwap(rows: list[dict[str, Any]]) -> float | None:
    pv = volume = 0.0
    for r in rows:
        h, l, c, v = map(_num, (r.get("high"), r.get("low"), r.get("close"), r.get("volume")))
        if None in (h, l, c, v) or v < 0:
            continue
        pv += ((h + l + c) / 3) * v
        volume += v
    return pv / volume if volume else None


def _levels(rows: list[dict[str, Any]]) -> dict[str, Any]:
    highs = [_num(r.get("high")) for r in rows]
    lows = [_num(r.get("low")) for r in rows]
    highs = [x for x in highs if x is not None]
    lows = [x for x in lows if x is not None]
    if not highs or not lows:
        return {"support": None, "resistance": None, "method": UNAVAILABLE}
    # Deterministic range extrema from the validated supplied candles only.
    return {"support": min(lows), "resistance": max(highs), "method": "candle_extrema"}


def _quality(stock: dict[str, Any], candles: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    available = [tf for tf in TIMEFRAMES if candles.get(tf)]
    return {
        "status": "AVAILABLE" if available else UNAVAILABLE,
        "timeframes_available": available,
        "candle_count": sum(len(candles.get(tf, [])) for tf in TIMEFRAMES),
    }


def _opening_range(stock: dict[str, Any]) -> dict[str, Any]:
    existing = stock.get("opening_range")
    if not isinstance(existing, dict):
        return {"status": UNAVAILABLE, "period": None, "high": None, "low": None}
    result = {
        "status": existing.get("status", UNAVAILABLE),
        "period": existing.get("period"),
        "high": existing.get("high"),
        "low": existing.get("low"),
    }
    return result


def enrich_market_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return an additive enrichment COPY of an existing normalized payload.

    No acquisition, validation, stale-data, endpoint, or publisher behavior is
    invoked or modified. Missing information remains None/UNAVAILABLE.
    """
    result = deepcopy(payload)
    stocks = result.get("stocks")
    if not isinstance(stocks, dict):
        result["execution_enrichment"] = {"status": UNAVAILABLE, "stocks": {}}
        return result

    enriched = {}
    for symbol, original in payload.get("stocks", {}).items():
        stock = original if isinstance(original, dict) else {}
        candles = stock.get("candles") if isinstance(stock.get("candles"), dict) else {}
        out_candles = {}
        indicators = {}
        integrity = {}
        completed = {}
        levels = {}

        for tf, minutes in TIMEFRAMES.items():
            rows = candles.get(tf) if isinstance(candles.get(tf), list) else []
            completed[tf] = _completed(rows, minutes)
            integrity[tf] = {
                "status": "OK" if rows else UNAVAILABLE,
                "candle_count": len(rows),
                "gaps": _gaps(rows, minutes),
            }
            indicators[tf] = {
                "ema9": _ema(completed[tf], 9),
                "ema20": _ema(completed[tf], 20),
                "vwap": _vwap(completed[tf]),
            }
            levels[tf] = _levels(completed[tf])
            out_candles[tf] = {"count": len(completed[tf]), "last_timestamp": completed[tf][-1].get("timestamp") if completed[tf] else None}

        data_age = None
        ts = _timestamp(stock.get("timestamp"))
        payload_ts = _timestamp(payload.get("timestamp"))
        if ts is not None and payload_ts is not None:
            data_age = max(0.0, (payload_ts - ts).total_seconds())

        enriched[symbol] = {
            "completed_candles": out_candles,
            "candle_integrity": integrity,
            "timeframe_indicators": indicators,
            "support_resistance": levels,
            "execution_quality": _quality(stock, candles),
            "data_age_seconds": data_age,
            "opening_range": _opening_range(stock),
        }

    result["execution_enrichment"] = {
        "status": "AVAILABLE",
        "stocks": enriched,
    }
    return result


# Friendly aliases for callers/tests.
enrich_payload = enrich_market_payload
