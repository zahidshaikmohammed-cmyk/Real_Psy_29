from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

from .dhan_rest import DhanRestClient
from .instrument_registry import InstrumentRegistry

IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
SUPPORTED_INTERVALS = ("1", "5", "15", "60")


class CandleHistoryError(RuntimeError):
    """Raised when genuine Dhan candle history cannot be acquired safely."""


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class StockSessionHistory:
    symbol: str
    trading_date: date
    previous_day_high: float | None
    previous_day_low: float | None
    previous_day_close: float | None
    candles_1m: tuple[Candle, ...]
    candles_5m: tuple[Candle, ...]
    candles_15m: tuple[Candle, ...]
    candles_1h: tuple[Candle, ...]


def _session_date(now: datetime | None = None) -> date:
    current = (now or datetime.now(IST)).astimezone(IST)
    return current.date()


def _parse_candle(row: dict) -> Candle:
    timestamp = row.get("timestamp")
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if isinstance(timestamp, (int, float)):
        timestamp = datetime.fromtimestamp(timestamp, tz=IST)
    if not isinstance(timestamp, datetime):
        raise CandleHistoryError("Dhan candle has no valid timestamp")
    timestamp = timestamp.astimezone(IST)
    return Candle(
        timestamp=timestamp,
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]),
    )


def _normalise_candles(rows: list[dict], trading_date: date) -> tuple[Candle, ...]:
    candles = tuple(sorted((_parse_candle(row) for row in rows), key=lambda c: c.timestamp))
    if len({c.timestamp for c in candles}) != len(candles):
        raise CandleHistoryError("Dhan returned duplicate candle timestamps")
    return tuple(c for c in candles if c.timestamp.date() == trading_date)


def _previous_day(rows: list[dict], trading_date: date) -> tuple[float | None, float | None, float | None]:
    candidates = []
    for row in rows:
        candle = _parse_candle(row)
        if candle.timestamp.date() < trading_date:
            candidates.append(candle)
    if not candidates:
        return None, None, None
    latest_date = max(c.timestamp.date() for c in candidates)
    day = [c for c in candidates if c.timestamp.date() == latest_date]
    return max(c.high for c in day), min(c.low for c in day), day[-1].close


def _extract_rows(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "candles", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    raise CandleHistoryError("Unexpected Dhan candle response")


def acquire_stock_session_history(
    client: DhanRestClient,
    registry: InstrumentRegistry,
    symbol: str,
    *,
    now: datetime | None = None,
    candle_fetcher: Callable[[str, str, date, date], object] | None = None,
) -> StockSessionHistory:
    trading_date = _session_date(now)
    instrument = registry.by_symbol.get(symbol)
    if instrument is None:
        raise CandleHistoryError(f"Unknown PSY29 symbol: {symbol}")

    fetch = candle_fetcher or client.intraday_candles
    start = trading_date
    end = trading_date
    series: dict[str, tuple[Candle, ...]] = {}
    for interval in SUPPORTED_INTERVALS:
        payload = fetch(instrument.security_id, interval, start, end)
        series[interval] = _normalise_candles(_extract_rows(payload), trading_date)

    previous_start = trading_date - timedelta(days=7)
    daily_payload = client.daily_history(
        instrument.security_id,
        previous_start,
        trading_date - timedelta(days=1),
    )
    previous_high, previous_low, previous_close = _previous_day(
        _extract_rows(daily_payload), trading_date
    )

    return StockSessionHistory(
        symbol=symbol,
        trading_date=trading_date,
        previous_day_high=previous_high,
        previous_day_low=previous_low,
        previous_day_close=previous_close,
        candles_1m=series["1"],
        candles_5m=series["5"],
        candles_15m=series["15"],
        candles_1h=series["60"],
    )
