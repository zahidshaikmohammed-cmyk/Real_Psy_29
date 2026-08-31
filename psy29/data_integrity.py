from __future__ import annotations

import math
import struct
from datetime import datetime, time

IST_OPEN = time(9, 15)
IST_CLOSE = time(15, 30)
MIN_REASONABLE_EQUITY_PRICE = 0.01
MAX_REASONABLE_EQUITY_PRICE = 10_000_000.0
MAX_FUTURE_TICK_SKEW_SECONDS = 60
MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS = 86400
DHAN_QUOTE_PACKET_FORMAT = "<BHBIfHIfIIIffff"
DHAN_QUOTE_PACKET_SIZE = struct.calcsize(DHAN_QUOTE_PACKET_FORMAT)


class DataIntegrityError(ValueError):
    """Raised when market data cannot be trusted for PSY29 decisions."""


def _finite_positive(value):
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("non-numeric price") from exc
    if not math.isfinite(number) or not MIN_REASONABLE_EQUITY_PRICE <= number <= MAX_REASONABLE_EQUITY_PRICE:
        raise DataIntegrityError("non-finite/out-of-range equity price")
    return number


def _normalize_epoch_seconds(value):
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


def parse_dhan_quote_packet(data):
    if not isinstance(data, (bytes, bytearray, memoryview)) or len(data) < DHAN_QUOTE_PACKET_SIZE:
        return None
    try:
        code, msglen, segment, sid, ltp, ltq, ltt, atp, volume, sell, buy, opn, close, high, low = struct.unpack(
            DHAN_QUOTE_PACKET_FORMAT, bytes(data[:DHAN_QUOTE_PACKET_SIZE])
        )
    except struct.error:
        return None
    if code != 4 or segment != 1 or msglen < DHAN_QUOTE_PACKET_SIZE or msglen > len(data):
        return None
    prices = (ltp, atp, opn, close, high, low)
    if not all(math.isfinite(x) for x in prices) or ltp <= 0 or atp < 0 or opn <= 0 or high <= 0 or low <= 0:
        return None
    if volume < 0 or ltq < 0 or sell < 0 or buy < 0:
        return None
    return sid, ltp, volume, _normalize_epoch_seconds(ltt), opn, high, low


def validate_ohlcv_row(row, trading_date, *, session_only=True, allow_zero_volume=False):
    try:
        ts = datetime.fromisoformat(str(row["timestamp"]))
        epoch = int(row["epoch"])
        vals = [_finite_positive(row[k]) for k in ("open", "high", "low", "close")]
        volume = int(row.get("volume", 0))
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("malformed candle") from exc
    if ts.tzinfo is None or ts.date().isoformat() != trading_date:
        raise DataIntegrityError("candle timestamp outside trading date")
    if session_only and not IST_OPEN <= ts.timetz().replace(tzinfo=None) < IST_CLOSE:
        raise DataIntegrityError("candle timestamp outside NSE session")
    if ts > datetime.now(ts.tzinfo):
        raise DataIntegrityError("future candle")
    if ts.second or ts.microsecond:
        raise DataIntegrityError("candle timestamp is not minute-aligned")
    if abs(ts.timestamp() - epoch) > 1:
        raise DataIntegrityError("candle epoch does not match timestamp")
    if volume <= 0 and not allow_zero_volume:
        raise DataIntegrityError("zero/negative candle volume")
    opn, high, low, close = vals
    if high < max(opn, close) or low > min(opn, close) or high < low:
        raise DataIntegrityError("invalid OHLC bounds")
    return dict(row)


def validate_intraday_rows(rows, trading_date):
    if not isinstance(rows, list) or not rows:
        raise DataIntegrityError("empty intraday history")
    valid = []
    previous = None
    for row in rows:
        clean = validate_ohlcv_row(row, trading_date, allow_zero_volume=True)
        epoch = int(clean["epoch"])
        if previous is not None and epoch <= previous:
            raise DataIntegrityError("duplicate/non-chronological candle")
        previous = epoch
        if int(clean.get("volume", 0)) > 0:
            valid.append(clean)
    return valid


def validate_live_quote(quote):
    if not isinstance(quote, dict):
        raise DataIntegrityError("quote is not an object")
    current = _finite_positive(quote.get("current"))
    try:
        volume = int(quote.get("volume"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid quote volume") from exc
    if volume < 0:
        raise DataIntegrityError("invalid quote volume")
    return {"current": current, "volume": volume}


def validate_quote(quote):
    if not isinstance(quote, dict):
        raise DataIntegrityError("quote is not an object")
    for key in ("current", "open", "high", "low"):
        if key not in quote or quote[key] is None:
            raise DataIntegrityError("quote missing required market fields")
    current = _finite_positive(quote["current"])
    opn = _finite_positive(quote["open"])
    high = _finite_positive(quote["high"])
    low = _finite_positive(quote["low"])
    close = _finite_positive(quote["close"]) if quote.get("close") is not None else None
    volume = int(quote.get("volume", 0))
    if volume < 0 or high < max(opn, current) or low > min(opn, current) or high < low:
        raise DataIntegrityError("invalid quote OHLC/volume")
    if close is not None and (high < close or low > close):
        raise DataIntegrityError("invalid quote OHLC bounds")
    return {"current": current, "open": opn, "high": high, "low": low, "close": close, "volume": volume}


def _validated_tick(ltp, volume, ltt_epoch, now, previous_volume=None, max_exchange_age_seconds=300):
    price = _finite_positive(ltp)
    vol = int(volume)
    if vol < 0:
        raise DataIntegrityError("invalid tick volume")
    ltt = _normalize_epoch_seconds(ltt_epoch)
    embedded = datetime.fromtimestamp(ltt, now.tzinfo)
    future = (embedded - now).total_seconds()
    if future > MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS:
        raise DataIntegrityError("future live tick")
    if future > MAX_FUTURE_TICK_SKEW_SECONDS:
        ltt = int(now.timestamp())
        embedded = now
    if embedded.date() != now.date() or not IST_OPEN <= embedded.timetz().replace(tzinfo=None) < IST_CLOSE:
        raise DataIntegrityError("tick timestamp outside current session")
    if (now - embedded).total_seconds() > max_exchange_age_seconds:
        raise DataIntegrityError("stale live tick")
    if not IST_OPEN <= now.timetz().replace(tzinfo=None) < IST_CLOSE:
        raise DataIntegrityError("tick received outside current NSE session")
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, ltt


def validate_live_tick(ltp, volume, ltt_epoch, now, previous_volume=None, *, max_exchange_age_seconds=300):
    return _validated_tick(ltp, volume, ltt_epoch, now, previous_volume, max_exchange_age_seconds)


def validate_tick(ltp, volume, ltt_epoch, day_open, day_high, day_low, now, previous_volume=None):
    price, vol, ltt = _validated_tick(ltp, volume, ltt_epoch, now, previous_volume)
    opn = _finite_positive(day_open)
    high = _finite_positive(day_high)
    low = _finite_positive(day_low)
    if high < low or high < opn or low > opn or not low <= price <= high:
        raise DataIntegrityError("invalid live tick market values")
    return price, vol, ltt, opn, high, low
