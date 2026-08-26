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
SUPPORTED_INTERVALS = (1, 5, 15, 60)
PREVIOUS_DAILY_CANDLE_COUNT = 30


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
    previous_daily_candles: tuple[Candle, ...]


def _session_date(now: datetime | None = None) -> date:
    current = (now or datetime.now(IST)).astimezone(IST)
    return current.date()


def _epoch_to_ist(value: object) -> datetime:
    try:
        return datetime.fromtimestamp(float(value), tz=IST)
    except (TypeError, ValueError, OSError) as exc:
        raise CandleHistoryError("Dhan candle contains an invalid epoch timestamp") from exc


def _parse_columnar(payload: object, trading_date: date, *, filter_date: bool = True) -> tuple[Candle, ...]:
    if not isinstance(payload, dict):
        raise CandleHistoryError("Unexpected Dhan candle response")
    required = ("open", "high", "low", "close", "volume", "timestamp")
    if any(not isinstance(payload.get(key), list) for key in required):
        raise CandleHistoryError("Dhan candle response is missing required arrays")
    columns = [payload[key] for key in required]
    if len({len(column) for column in columns}) != 1:
        raise CandleHistoryError("Dhan candle arrays have inconsistent lengths")

    candles: list[Candle] = []
    for values in zip(*columns):
        timestamp = _epoch_to_ist(values[5])
        if filter_date and timestamp.date() != trading_date:
            continue
        try:
            candle = Candle(
                timestamp=timestamp,
                open=float(values[0]),
                high=float(values[1]),
                low=float(values[2]),
                close=float(values[3]),
                volume=int(values[4]),
            )
        except (TypeError, ValueError) as exc:
            raise CandleHistoryError("Dhan candle contains an invalid OHLCV value") from exc
        if candle.high < max(candle.open, candle.close) or candle.low > min(candle.open, candle.close):
            raise CandleHistoryError("Dhan candle violates OHLC price bounds")
        candles.append(candle)

    candles.sort(key=lambda candle: candle.timestamp)
    timestamps = [candle.timestamp for candle in candles]
    if len(set(timestamps)) != len(timestamps):
        raise CandleHistoryError("Dhan returned duplicate candle timestamps")
    return tuple(candles)


def _previous_daily_candles(payload: object, trading_date: date) -> tuple[Candle, ...]:
    candles = _parse_columnar(payload, trading_date, filter_date=False)
    candles = tuple(c for c in candles if c.timestamp.date() < trading_date)
    dates = [c.timestamp.date() for c in candles]
    if len(set(dates)) != len(dates):
        raise CandleHistoryError("Dhan returned duplicate daily candle dates")
    return candles[-PREVIOUS_DAILY_CANDLE_COUNT:]


def _previous_day(candles: tuple[Candle, ...]) -> tuple[float | None, float | None, float | None]:
    if not candles:
        return None, None, None
    day = candles[-1]
    return day.high, day.low, day.close


def acquire_stock_session_history(
    client: DhanRestClient,
    registry: InstrumentRegistry,
    symbol: str,
    *,
    now: datetime | None = None,
    intraday_fetcher: Callable[[str, int, datetime | str, datetime | str], object] | None = None,
    daily_fetcher: Callable[[str, str, str], object] | None = None,
) -> StockSessionHistory:
    trading_date = _session_date(now)
    instrument = registry.by_symbol.get(symbol)
    if instrument is None:
        raise CandleHistoryError(f"Unknown PSY29 symbol: {symbol}")

    current = (now or datetime.now(IST)).astimezone(IST)
    session_start = datetime.combine(trading_date, MARKET_OPEN, tzinfo=IST)
    session_end = min(current, datetime.combine(trading_date, MARKET_CLOSE, tzinfo=IST))
    if session_end < session_start:
        session_end = session_start

    fetch_intraday = intraday_fetcher or client.intraday
    series: dict[int, tuple[Candle, ...]] = {}
    for interval in SUPPORTED_INTERVALS:
        payload = fetch_intraday(instrument.security_id, interval, session_start, session_end)
        series[interval] = _parse_columnar(payload, trading_date)

    fetch_daily = daily_fetcher or client.historical_daily
    previous_start = trading_date - timedelta(days=60)
    daily_payload = fetch_daily(
        instrument.security_id,
        previous_start.isoformat(),
        trading_date.isoformat(),
    )
    previous_daily = _previous_daily_candles(daily_payload, trading_date)
    previous_high, previous_low, previous_close = _previous_day(previous_daily)

    return StockSessionHistory(
        symbol=symbol,
        trading_date=trading_date,
        previous_day_high=previous_high,
        previous_day_low=previous_low,
        previous_day_close=previous_close,
        candles_1m=series[1],
        candles_5m=series[5],
        candles_15m=series[15],
        candles_1h=series[60],
        previous_daily_candles=previous_daily,
    )
