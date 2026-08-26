from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from typing import Sequence

from .candle_history import Candle, IST


class StructureError(ValueError):
    """Raised when structure input is invalid."""


@dataclass(frozen=True)
class OpeningRange:
    start: time
    end: time
    high: float | None
    low: float | None
    range_size: float | None
    candle_count: int
    complete: bool


@dataclass(frozen=True)
class SwingPoint:
    timestamp: object
    price: float
    kind: str


@dataclass(frozen=True)
class StructureSnapshot:
    timestamp: object | None
    session_high: float | None
    session_low: float | None
    previous_day_high: float | None
    previous_day_low: float | None
    previous_day_close: float | None
    opening_range: OpeningRange
    latest_swing_high: SwingPoint | None
    latest_swing_low: SwingPoint | None
    trend: str
    higher_highs: int
    higher_lows: int
    lower_highs: int
    lower_lows: int


def _validate(candles: Sequence[Candle]) -> tuple[Candle, ...]:
    rows = tuple(candles)
    previous = None
    for candle in rows:
        if candle.timestamp.tzinfo is None:
            raise StructureError("Structure candles require timezone-aware timestamps")
        if previous is not None and candle.timestamp <= previous:
            raise StructureError("Structure candles must be strictly chronological")
        if candle.high < max(candle.open, candle.close) or candle.low > min(candle.open, candle.close):
            raise StructureError("Structure candle violates OHLC bounds")
        if candle.high <= 0 or candle.low <= 0:
            raise StructureError("Structure prices must be positive")
        previous = candle.timestamp
    return rows


def opening_range(candles: Sequence[Candle], *, start: time = time(9, 15), end: time = time(9, 30)) -> OpeningRange:
    rows = _validate(candles)
    selected = tuple(c for c in rows if start <= c.timestamp.astimezone(IST).time() < end)
    high = max((c.high for c in selected), default=None)
    low = min((c.low for c in selected), default=None)
    return OpeningRange(start, end, high, low, None if high is None or low is None else high - low,
                        len(selected), bool(selected) and selected[-1].timestamp.astimezone(IST).time() >= end)


def _swings(candles: Sequence[Candle], strength: int = 2) -> tuple[tuple[SwingPoint, ...], tuple[SwingPoint, ...]]:
    rows = _validate(candles)
    if strength < 1:
        raise StructureError("Swing strength must be positive")
    highs: list[SwingPoint] = []
    lows: list[SwingPoint] = []
    for i in range(strength, len(rows) - strength):
        window = rows[i-strength:i+strength+1]
        current = rows[i]
        if current.high == max(c.high for c in window) and current.high > max(c.high for c in window[:strength]) and current.high >= max(c.high for c in window[strength+1:]):
            highs.append(SwingPoint(current.timestamp, current.high, "HIGH"))
        if current.low == min(c.low for c in window) and current.low < min(c.low for c in window[:strength]) and current.low <= min(c.low for c in window[strength+1:]):
            lows.append(SwingPoint(current.timestamp, current.low, "LOW"))
    return tuple(highs), tuple(lows)


def structure_snapshot(candles: Sequence[Candle], *, previous_day_high: float | None = None,
                       previous_day_low: float | None = None, previous_day_close: float | None = None,
                       opening_range_start: time = time(9, 15), opening_range_end: time = time(9, 30),
                       swing_strength: int = 2) -> StructureSnapshot:
    rows = _validate(candles)
    if not rows:
        return StructureSnapshot(None, None, None, previous_day_high, previous_day_low, previous_day_close,
                                 OpeningRange(opening_range_start, opening_range_end, None, None, None, 0, False),
                                 None, None, "NEUTRAL", 0, 0, 0, 0)
    highs, lows = _swings(rows, swing_strength)
    hh = hl = lh = ll = 0
    if len(highs) >= 2:
        for a, b in zip(highs, highs[1:]):
            hh += b.price > a.price
            lh += b.price < a.price
    if len(lows) >= 2:
        for a, b in zip(lows, lows[1:]):
            hl += b.price > a.price
            ll += b.price < a.price
    if hh and hl and hh + hl >= lh + ll:
        trend = "BULLISH"
    elif lh and ll and lh + ll > hh + hl:
        trend = "BEARISH"
    else:
        trend = "NEUTRAL"
    return StructureSnapshot(rows[-1].timestamp, max(c.high for c in rows), min(c.low for c in rows),
                             previous_day_high, previous_day_low, previous_day_close, opening_range(rows,
                             start=opening_range_start, end=opening_range_end), highs[-1] if highs else None,
                             lows[-1] if lows else None, trend, hh, hl, lh, ll)
