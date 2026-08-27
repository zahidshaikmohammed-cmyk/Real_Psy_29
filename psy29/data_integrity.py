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
    """Validate an intraday stream while isolating individual corrupt rows."""
    if not isinstance(rows, list) or not rows:
        raise DataIntegrityError("empty intraday history")
    valid: list[dict] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            clean = validate_ohlcv_row(row, trading_date)
            epoch = int(clean["epoch"])
            if epoch in seen:
                continue
            seen.add(epoch)
            valid.append(clean)
        except (DataIntegrityError, KeyError, TypeError, ValueError, OverflowError):
            continue
    valid.sort(key=lambda r: int(r["epoch"]))
    if len(valid) < 15:
        raise DataIntegrityError(f"insufficient valid intraday history: {len(valid)} rows")
    for i in range(1, len(valid)):
        if int(valid[i]["epoch"]) <= int(valid[i - 1]["epoch"]):
            raise DataIntegrityError("duplicate/out-of-order candle")
    return valid


def validate_quote(quote: object) -> dict:
    """Validate a live quote and repair only contradictory aggregate OHLC.

    Dhan's aggregate quote occasionally arrives with a small internal OHLC
    contradiction (for example open slightly above reported day-high). That
    contradiction must not poison the public machine payload when the live LTP
    itself is valid. We therefore normalize the session envelope from the
    finite, plausible quote fields. Grossly implausible values are still
    rejected rather than silently accepted.
    """
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
    try:
        volume = int(quote.get("volume") or 0)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid quote volume") from exc
    if volume < 0:
        raise DataIntegrityError("invalid quote OHLC/volume")

    # Keep the quote anchored to the live price. A grossly distant field is
    # treated as corruption; a small contradiction is repaired by taking the
    # envelope of the plausible values. 50% is intentionally generous for an
    # equity intraday feed while still excluding the scientific-notation garbage
    # that previously appeared in this service.
    anchor = current
    plausible = []
    for value in (opn, high, low):
        if abs(value - anchor) / anchor <= 0.50:
            plausible.append(value)
    if len(plausible) < 2:
        raise DataIntegrityError("quote OHLC values implausibly distant from current price")

    session_high = max([anchor, *plausible])
    session_low = min([anchor, *plausible])
    normalized_open = min(max(opn, session_low), session_high)

    return {
        "current": current,
        "open": normalized_open,
        "high": session_high,
        "low": session_low,
        "close": close,
        "volume": volume,
    }


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
