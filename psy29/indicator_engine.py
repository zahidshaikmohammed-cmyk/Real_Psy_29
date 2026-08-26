from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable, Mapping, Sequence

from .candle_history import Candle, StockSessionHistory


class IndicatorError(ValueError):
    """Raised when indicator input is invalid or insufficient."""


@dataclass(frozen=True)
class IndicatorPoint:
    timestamp: object
    close: float
    ema9: float | None
    ema20: float | None
    vwap: float | None
    rsi14: float | None
    atr14: float | None
    macd: float | None
    macd_signal: float | None
    macd_histogram: float | None
    volume_sma20: float | None


@dataclass(frozen=True)
class IndicatorSnapshot:
    symbol: str
    timeframe: str
    timestamp: object | None
    close: float | None
    ema9: float | None
    ema20: float | None
    vwap: float | None
    rsi14: float | None
    atr14: float | None
    macd: float | None
    macd_signal: float | None
    macd_histogram: float | None
    volume_sma20: float | None
    candle_count: int


def _validate_candles(candles: Iterable[Candle]) -> tuple[Candle, ...]:
    rows = tuple(candles)
    previous = None
    for candle in rows:
        if candle.timestamp.tzinfo is None:
            raise IndicatorError("Indicator candles must have timezone-aware timestamps")
        values = (candle.open, candle.high, candle.low, candle.close, candle.volume)
        if not all(isfinite(float(value)) for value in values):
            raise IndicatorError("Indicator candles contain non-finite values")
        if candle.open <= 0 or candle.high <= 0 or candle.low <= 0 or candle.close <= 0:
            raise IndicatorError("Indicator prices must be positive")
        if candle.high < max(candle.open, candle.close) or candle.low > min(candle.open, candle.close):
            raise IndicatorError("Indicator candle violates OHLC bounds")
        if candle.volume < 0:
            raise IndicatorError("Indicator volume cannot be negative")
        if previous is not None and candle.timestamp <= previous:
            raise IndicatorError("Indicator candles must be strictly chronological")
        previous = candle.timestamp
    return rows


def _ema(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    seed = sum(values[:period]) / period
    result[period - 1] = seed
    alpha = 2.0 / (period + 1.0)
    previous = seed
    for index in range(period, len(values)):
        previous = alpha * values[index] + (1.0 - alpha) * previous
        result[index] = previous
    return result


def _sma(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    running = sum(values[:period])
    result[period - 1] = running / period
    for index in range(period, len(values)):
        running += values[index] - values[index - period]
        result[index] = running / period
    return result


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _rsi(closes: list[float], period: int = 14) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return result
    gains = [0.0] * len(closes)
    losses = [0.0] * len(closes)
    for index in range(1, len(closes)):
        delta = closes[index] - closes[index - 1]
        gains[index] = max(delta, 0.0)
        losses[index] = max(-delta, 0.0)
    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period
    result[period] = _rsi_value(avg_gain, avg_loss)
    for index in range(period + 1, len(closes)):
        avg_gain = ((avg_gain * (period - 1)) + gains[index]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[index]) / period
        result[index] = _rsi_value(avg_gain, avg_loss)
    return result


def _atr(candles: tuple[Candle, ...], period: int = 14) -> list[float | None]:
    result: list[float | None] = [None] * len(candles)
    if len(candles) <= period:
        return result
    true_ranges = [0.0] * len(candles)
    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges[index] = candle.high - candle.low
        else:
            previous_close = candles[index - 1].close
            true_ranges[index] = max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
    value = sum(true_ranges[1 : period + 1]) / period
    result[period] = value
    for index in range(period + 1, len(candles)):
        value = ((value * (period - 1)) + true_ranges[index]) / period
        result[index] = value
    return result


def _session_vwap(candles: tuple[Candle, ...]) -> list[float | None]:
    result: list[float | None] = [None] * len(candles)
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    current_date = None
    for index, candle in enumerate(candles):
        if candle.timestamp.date() != current_date:
            current_date = candle.timestamp.date()
            cumulative_pv = 0.0
            cumulative_volume = 0.0
        if candle.volume > 0:
            typical_price = (candle.high + candle.low + candle.close) / 3.0
            cumulative_pv += typical_price * candle.volume
            cumulative_volume += candle.volume
        result[index] = cumulative_pv / cumulative_volume if cumulative_volume > 0 else None
    return result


def _merge_warmup_and_session(
    warmup: Sequence[Candle],
    session: Sequence[Candle],
) -> tuple[tuple[Candle, ...], int]:
    warmup_rows = _validate_candles(warmup)
    session_rows = _validate_candles(session)
    if not session_rows:
        return warmup_rows, len(warmup_rows)
    if warmup_rows and warmup_rows[-1].timestamp >= session_rows[0].timestamp:
        raise IndicatorError("Warmup candles must end before session candles begin")
    return warmup_rows + session_rows, len(warmup_rows)


def calculate_indicators(
    candles: Iterable[Candle],
    *,
    warmup_candles: Iterable[Candle] = (),
) -> tuple[IndicatorPoint, ...]:
    session_rows = _validate_candles(candles)
    warmup_rows = _validate_candles(warmup_candles)
    combined, session_offset = _merge_warmup_and_session(warmup_rows, session_rows)
    if not session_rows:
        return ()

    closes = [c.close for c in combined]
    volumes = [float(c.volume) for c in combined]
    ema9 = _ema(closes, 9)
    ema20 = _ema(closes, 20)
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd = [None if a is None or b is None else a - b for a, b in zip(ema12, ema26)]
    macd_values = [value for value in macd if value is not None]
    macd_signal_values = _ema(macd_values, 9)
    macd_signal: list[float | None] = [None] * len(combined)
    macd_offset = len(macd) - len(macd_values)
    for index, value in enumerate(macd_signal_values):
        macd_signal[index + macd_offset] = value
    macd_histogram = [
        None if value is None or signal is None else value - signal
        for value, signal in zip(macd, macd_signal)
    ]
    rsi14 = _rsi(closes)
    atr14 = _atr(combined)
    vwap = _session_vwap(combined)
    volume_sma20 = _sma(volumes, 20)

    points: list[IndicatorPoint] = []
    for session_index, candle in enumerate(session_rows):
        index = session_index + session_offset
        points.append(
            IndicatorPoint(
                timestamp=candle.timestamp,
                close=candle.close,
                ema9=ema9[index],
                ema20=ema20[index],
                vwap=vwap[index],
                rsi14=rsi14[index],
                atr14=atr14[index],
                macd=macd[index],
                macd_signal=macd_signal[index],
                macd_histogram=macd_histogram[index],
                volume_sma20=volume_sma20[index],
            )
        )
    return tuple(points)


_TIMEFRAME_MAP = {
    "1m": "candles_1m",
    "5m": "candles_5m",
    "15m": "candles_15m",
    "1h": "candles_1h",
}


class LiveIndicatorEngine:
    """Calculate deterministic indicators from the latest reconciled candle history."""

    def __init__(
        self,
        history: StockSessionHistory,
        *,
        warmup_by_timeframe: Mapping[str, Sequence[Candle]] | None = None,
    ):
        self.history = history
        self.warmup_by_timeframe = warmup_by_timeframe or {}

    def calculate(self, timeframe: str) -> tuple[IndicatorPoint, ...]:
        field = _TIMEFRAME_MAP.get(timeframe)
        if field is None:
            raise IndicatorError(f"Unsupported indicator timeframe: {timeframe}")
        return calculate_indicators(
            getattr(self.history, field),
            warmup_candles=self.warmup_by_timeframe.get(timeframe, ()),
        )

    def snapshot(self, timeframe: str) -> IndicatorSnapshot:
        points = self.calculate(timeframe)
        if not points:
            return IndicatorSnapshot(
                symbol=self.history.symbol,
                timeframe=timeframe,
                timestamp=None,
                close=None,
                ema9=None,
                ema20=None,
                vwap=None,
                rsi14=None,
                atr14=None,
                macd=None,
                macd_signal=None,
                macd_histogram=None,
                volume_sma20=None,
                candle_count=0,
            )
        point = points[-1]
        return IndicatorSnapshot(
            symbol=self.history.symbol,
            timeframe=timeframe,
            timestamp=point.timestamp,
            close=point.close,
            ema9=point.ema9,
            ema20=point.ema20,
            vwap=point.vwap,
            rsi14=point.rsi14,
            atr14=point.atr14,
            macd=point.macd,
            macd_signal=point.macd_signal,
            macd_histogram=point.macd_histogram,
            volume_sma20=point.volume_sma20,
            candle_count=len(points),
        )

    def all_snapshots(self) -> Mapping[str, IndicatorSnapshot]:
        return {timeframe: self.snapshot(timeframe) for timeframe in _TIMEFRAME_MAP}
