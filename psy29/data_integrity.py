from __future__ import annotations

import math
from datetime import datetime, time, timedelta

IST_OPEN = time(9, 15)
IST_CLOSE = time(15, 15)


class DataIntegrityError(ValueError):
    """Raised when market data cannot be trusted for PSY29 decisions."""


def _finite_positive(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("non-numeric price") from exc
    if not math.isfinite(number) or number <= 0:
        raise DataIntegrityError("non-finite/non-positive price")
    return number


def validate_ohlcv_row(row: dict, trading_date: str, *, session_only: bool = True) -> dict:
    try:
        ts = datetime.fromisoformat(str(row["timestamp"]))
        epoch = int(row["epoch"])
        values = [_finite_positive(row[key]) for key in ("open", "high", "low", "close")]
        volume = int(row.get("volume", 0))
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("malformed candle") from exc
    if ts.tzinfo is None or ts.date().isoformat() != trading_date:
        raise DataIntegrityError("candle timestamp outside trading date")
    if session_only and not (IST_OPEN <= ts.timetz().replace(tzinfo=None) < IST_CLOSE):
        raise DataIntegrityError("candle timestamp outside NSE session")
    if ts.second != 0 or ts.microsecond != 0:
        raise DataIntegrityError("candle timestamp is not minute-aligned")
    if abs(ts.timestamp() - epoch) > 1:
        raise DataIntegrityError("candle epoch does not match timestamp")
    if volume < 0:
        raise DataIntegrityError("negative candle volume")
    opn, high, low, close = values
    if high < max(opn, close) or low > min(opn, close) or high < low:
        raise DataIntegrityError("invalid OHLC bounds")
    return dict(row)


def validate_intraday_rows(rows: object, trading_date: str) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise DataIntegrityError("empty intraday history")
    valid: list[dict] = []
    previous: datetime | None = None
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise DataIntegrityError("intraday row is not an object")
        clean = validate_ohlcv_row(row, trading_date)
        ts = datetime.fromisoformat(str(clean["timestamp"]))
        epoch = int(clean["epoch"])
        if epoch in seen or (previous is not None and ts <= previous):
            raise DataIntegrityError("duplicate/out-of-order candle")
        seen.add(epoch)
        previous = ts
        valid.append(clean)
    return valid


def validate_quote(quote: object) -> dict:
    if not isinstance(quote, dict):
        raise DataIntegrityError("quote is not an object")
    required = ("current", "open", "high", "low")
    if any(key not in quote or quote[key] is None for key in required):
        raise DataIntegrityError("quote missing required market fields")
    current = _finite_positive(quote["current"])
    opn = _finite_positive(quote["open"])
    high = _finite_positive(quote["high"])
    low = _finite_positive(quote["low"])
    close = quote.get("close")
    if close is not None:
        close = _finite_positive(close)
    volume = int(quote.get("volume") or 0)
    if volume < 0 or high < low or high < opn or low > opn:
        raise DataIntegrityError("invalid quote OHLC/volume")
    if not low <= current <= high:
        raise DataIntegrityError("quote current price outside day range")
    return {"current": current, "open": opn, "high": high, "low": low, "close": close, "volume": volume}


def validate_tick(ltp: object, volume: object, ltt_epoch: object, day_open: object, day_high: object, day_low: object, now: datetime, previous_volume: object = None) -> tuple[float, int, int, float, float, float]:
    price = _finite_positive(ltp)
    opn = _finite_positive(day_open)
    high = _finite_positive(day_high)
    low = _finite_positive(day_low)
    try:
        vol = int(volume)
        ltt = int(ltt_epoch)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick volume/timestamp") from exc
    if vol < 0 or high < low or high < opn or low > opn or not low <= price <= high:
        raise DataIntegrityError("invalid live tick market values")
    ts = datetime.fromtimestamp(ltt, now.tzinfo)
    if ts.date() != now.date() or not (IST_OPEN <= ts.timetz().replace(tzinfo=None) < IST_CLOSE):
        raise DataIntegrityError("live tick timestamp outside current NSE session")
    if ts > now + timedelta(seconds=5):
        raise DataIntegrityError("live tick timestamp is in the future")
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, ltt, opn, high, low
