from __future__ import annotations

import logging
import math
import struct
import sys
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


def validate_ohlcv_row(row: dict, trading_date: str, *, session_only: bool = True, allow_zero_volume: bool = False) -> dict:
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
    if ts > datetime.now(ts.tzinfo):
        raise DataIntegrityError("future candle")
    if ts.second != 0 or ts.microsecond != 0:
        raise DataIntegrityError("candle timestamp is not minute-aligned")
    if abs(ts.timestamp() - epoch) > 1:
        raise DataIntegrityError("candle epoch does not match timestamp")
    if volume <= 0 and not allow_zero_volume:
        raise DataIntegrityError("zero/negative candle volume")
    opn, high, low, close = values
    if high < max(opn, close) or low > min(opn, close) or high < low:
        raise DataIntegrityError("invalid OHLC bounds")
    return dict(row)


def validate_intraday_rows(rows: object, trading_date: str) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise DataIntegrityError("empty intraday history")
    valid: list[dict] = []
    previous_epoch = None
    rejected_zero_volume = 0
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise DataIntegrityError(f"malformed candle at index {index}")
        clean = validate_ohlcv_row(row, trading_date, allow_zero_volume=True)
        epoch = int(clean["epoch"])
        if previous_epoch is not None and epoch <= previous_epoch:
            if epoch == previous_epoch:
                raise DataIntegrityError("duplicate candle")
            raise DataIntegrityError("non-chronological candle")
        previous_epoch = epoch
        if int(clean.get("volume", 0)) <= 0:
            rejected_zero_volume += 1
            logging.getLogger("psy29.data_integrity").warning(
                "Quarantined intraday candle index=%d timestamp=%s reason=zero/negative candle volume",
                index, clean.get("timestamp")
            )
            continue
        valid.append(clean)
    if len(valid) < 15:
        raise DataIntegrityError(f"insufficient valid intraday history: {len(valid)} rows")
    return valid


def validate_live_quote(quote: object) -> dict:
    if not isinstance(quote, dict):
        raise DataIntegrityError("quote is not an object")
    current = _finite_positive(quote.get("current"))
    try:
        volume = int(quote.get("volume"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid quote volume") from exc
    if volume <= 0:
        raise DataIntegrityError("zero/negative quote volume")
    return {"current": current, "volume": volume}


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
    try:
        volume = int(quote.get("volume"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid quote OHLC/volume") from exc
    if volume <= 0:
        raise DataIntegrityError("zero/negative quote volume")
    if high < max(opn, current) or low > min(opn, current) or high < low:
        raise DataIntegrityError("invalid quote OHLC bounds")
    if close is not None and (high < close or low > close):
        raise DataIntegrityError("invalid quote OHLC bounds")
    return {"current": current, "open": opn, "high": high, "low": low, "close": close, "volume": volume}


def validate_live_tick(ltp: object, volume: object, ltt_epoch: object, now: datetime,
                       previous_volume: object = None, *, max_exchange_age_seconds: int = 300) -> tuple[float, int, int]:
    price = _finite_positive(ltp)
    try:
        vol = int(volume)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick volume") from exc
    if vol <= 0:
        raise DataIntegrityError("zero/negative tick volume")
    ltt = _normalize_epoch_seconds(ltt_epoch)
    try:
        embedded_ts = datetime.fromtimestamp(ltt, now.tzinfo)
    except (OverflowError, OSError, ValueError) as exc:
        raise DataIntegrityError("invalid tick timestamp") from exc
    if embedded_ts > now + timedelta(seconds=5):
        raise DataIntegrityError("future live tick")
    if embedded_ts.date() != now.date():
        raise DataIntegrityError("live tick timestamp outside current trading date")
    if not (IST_OPEN <= embedded_ts.timetz().replace(tzinfo=None) < IST_CLOSE):
        raise DataIntegrityError("live tick timestamp outside NSE session")
    if (now - embedded_ts).total_seconds() > max_exchange_age_seconds:
        raise DataIntegrityError("stale live tick")
    if not (IST_OPEN <= now.timetz().replace(tzinfo=None) < IST_CLOSE):
        raise DataIntegrityError("tick received outside current NSE session")
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, ltt


def validate_tick(ltp: object, volume: object, ltt_epoch: object, day_open: object,
                  day_high: object, day_low: object, now: datetime,
                  previous_volume: object = None) -> tuple[float, int, int, float, float, float]:
    price = _finite_positive(ltp)
    opn = _finite_positive(day_open)
    high = _finite_positive(day_high)
    low = _finite_positive(day_low)
    try:
        vol = int(volume)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick volume/timestamp") from exc
    if vol <= 0 or high < low or high < opn or low > opn or not low <= price <= high:
        raise DataIntegrityError("invalid live tick market values")
    ltt = _normalize_epoch_seconds(ltt_epoch)
    try:
        embedded_ts = datetime.fromtimestamp(ltt, now.tzinfo)
    except (OverflowError, OSError, ValueError) as exc:
        raise DataIntegrityError("invalid tick timestamp") from exc
    if embedded_ts > now + timedelta(seconds=5):
        raise DataIntegrityError("future live tick")
    if embedded_ts.date() != now.date() or not (IST_OPEN <= embedded_ts.timetz().replace(tzinfo=None) < IST_CLOSE):
        raise DataIntegrityError("live tick timestamp outside current NSE session")
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, ltt, opn, high, low


def _install_quote_boundary_diagnostics() -> None:
    main = sys.modules.get("main")
    if main is None or not hasattr(main, "dhan_post") or getattr(main, "_psy29_quote_diag_installed", False):
        return
    logger = logging.getLogger("psy29.quote_diagnostics")
    original_post = main.dhan_post

    def diagnostic_dhan_post(url, *, token, payload, client_id=None, kind="data", timeout=25, label="Dhan API"):
        response = original_post(url, token=token, payload=payload, client_id=client_id, kind=kind, timeout=timeout, label=label)
        if label == "marketfeed:quote":
            try:
                body = response.json()
                raw_quotes = (body.get("data") or {}).get("NSE_EQ") or {}
                ids = [str(v) for v in payload.get("NSE_EQ", [])]
                stocks = list(getattr(main, "STOCKS", []))
                mapping = {ids[i]: stocks[i] for i in range(min(len(ids), len(stocks)))}
                logger.warning("QUOTE_BOUNDARY_REQUEST requested_count=%d returned_count=%d", len(ids), len(raw_quotes))
                for sid in ids:
                    item = raw_quotes.get(sid)
                    if item is None and sid.isdigit():
                        item = raw_quotes.get(int(sid))
                    symbol = mapping.get(sid, "UNKNOWN")
                    if not isinstance(item, dict):
                        logger.warning("QUOTE_BOUNDARY symbol=%s security_id=%s returned=false raw_type=%s", symbol, sid, type(item).__name__)
                        continue
                    ohlc = item.get("ohlc") if isinstance(item.get("ohlc"), dict) else {}
                    safe = {"last_price": item.get("last_price"), "volume": item.get("volume"),
                            "open": ohlc.get("open"), "high": ohlc.get("high"), "low": ohlc.get("low"),
                            "close": ohlc.get("close"), "last_trade_time": item.get("last_trade_time")}
                    logger.warning("QUOTE_BOUNDARY symbol=%s security_id=%s raw=%r", symbol, sid, safe)
            except Exception:
                logger.exception("QUOTE_BOUNDARY diagnostic parse failure")
        return response

    main.dhan_post = diagnostic_dhan_post
    main._psy29_quote_diag_installed = True


def _install_canonical_ws_parser() -> None:
    main = sys.modules.get("main")
    if main is None:
        return

    def parse_quote_packet(data: bytes):
        if len(data) < 50 or data[0] != 4:
            return None
        security_id = struct.unpack_from("<i", data, 4)[0]
        ltp = struct.unpack_from("<f", data, 8)[0]
        ltq = struct.unpack_from("<h", data, 12)[0]
        ltt = struct.unpack_from("<i", data, 14)[0]
        atp = struct.unpack_from("<f", data, 18)[0]
        volume = struct.unpack_from("<i", data, 22)[0]
        total_sell = struct.unpack_from("<i", data, 26)[0]
        total_buy = struct.unpack_from("<i", data, 30)[0]
        day_open = struct.unpack_from("<f", data, 34)[0]
        day_close = struct.unpack_from("<f", data, 38)[0]
        day_high = struct.unpack_from("<f", data, 42)[0]
        day_low = struct.unpack_from("<f", data, 46)[0]
        values = (ltp, atp, day_open, day_close, day_high, day_low)
        if not all(math.isfinite(v) for v in values) or any(v <= 0 for v in (ltp, day_open, day_high, day_low)):
            return None
        if atp < 0 or volume <= 0 or ltq < 0 or total_sell < 0 or total_buy < 0:
            return None
        return security_id, ltp, volume, ltt, day_open, day_high, day_low

    main.parse_quote_packet = parse_quote_packet
    main._psy29_ws_parser_canonical = True


_install_quote_boundary_diagnostics()
_install_canonical_ws_parser()
