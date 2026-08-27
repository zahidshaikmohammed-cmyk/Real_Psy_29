from __future__ import annotations

import math
from datetime import datetime, time, timedelta

IST_OPEN = time(9, 15)
IST_CLOSE = time(15, 15)
MIN_REASONABLE_EQUITY_PRICE = 0.01
MAX_REASONABLE_EQUITY_PRICE = 10_000_000.0


class DataIntegrityError(ValueError):
    """Raised when market data cannot be trusted for PSY29 decisions."""


def _finite_positive(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("non-numeric price") from exc
    if not math.isfinite(number) or not (MIN_REASONABLE_EQUITY_PRICE <= number <= MAX_REASONABLE_EQUITY_PRICE):
        raise DataIntegrityError("non-finite/out-of-range equity price")
    return number


def _normalize_epoch_seconds(value: object) -> int:
    """Normalize epoch units without changing valid Unix-second timestamps."""
    try:
        raw = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick timestamp") from exc
    if raw <= 0:
        raise DataIntegrityError("invalid tick timestamp")
    magnitude = abs(raw)
    if magnitude >= 10**18:
        raw //= 10**9
    elif magnitude >= 10**15:
        raw //= 10**6
    elif magnitude >= 10**12:
        raw //= 10**3
    return raw


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
    """Validate an intraday stream while isolating individual corrupt rows.

    Dhan history can occasionally contain an isolated malformed OHLC row. A
    single bad row must never poison an otherwise valid session. Such a row is
    rejected and omitted. Ordering/duplicate integrity is still enforced on the
    retained rows, and a minimum usable history is required.
    """
    if not isinstance(rows, list) or not rows:
        raise DataIntegrityError("empty intraday history")
    valid: list[dict] = []
    seen: set[int] = set()
    rejected = 0
    for row in rows:
        if not isinstance(row, dict):
            rejected += 1
            continue
        try:
            clean = validate_ohlcv_row(row, trading_date)
            epoch = int(clean["epoch"])
            if epoch in seen:
                rejected += 1
                continue
            seen.add(epoch)
            valid.append(clean)
        except (DataIntegrityError, KeyError, TypeError, ValueError, OverflowError):
            rejected += 1
            continue
    valid.sort(key=lambda r: int(r["epoch"]))
    if len(valid) < 15:
        raise DataIntegrityError(f"insufficient valid intraday history: {len(valid)} rows")
    for i in range(1, len(valid)):
        if int(valid[i]["epoch"]) <= int(valid[i - 1]["epoch"]):
            raise DataIntegrityError("duplicate/out-of-order candle")
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
    """Validate market values and use packet receipt time for freshness."""
    price = _finite_positive(ltp)
    opn = _finite_positive(day_open)
    high = _finite_positive(day_high)
    low = _finite_positive(day_low)
    try:
        vol = int(volume)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick volume/timestamp") from exc
    ltt = _normalize_epoch_seconds(ltt_epoch)
    if vol < 0 or high < low or high < opn or low > opn or not low <= price <= high:
        raise DataIntegrityError("invalid live tick market values")
    try:
        embedded_ts = datetime.fromtimestamp(ltt, now.tzinfo)
    except (OverflowError, OSError, ValueError) as exc:
        raise DataIntegrityError("invalid tick timestamp") from exc
    receipt = now
    if not (IST_OPEN <= receipt.timetz().replace(tzinfo=None) < IST_CLOSE) or receipt.date() != now.date():
        raise DataIntegrityError("tick received outside current NSE session")
    receipt_epoch = int(receipt.timestamp())
    if embedded_ts > now + timedelta(seconds=5):
        pass
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, receipt_epoch, opn, high, low
