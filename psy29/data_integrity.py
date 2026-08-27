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


def validate_live_quote(quote: object) -> dict:
    """Validate the point-in-time quote without trusting broker aggregate OHLC.

    Dhan's quote packet contains both LTP and an aggregate day OHLC snapshot.
    The LTP and volume are independently useful live-feed fields, while the
    session OHLC used by PSY29 is derived from validated 1-minute candles.
    Therefore an internally contradictory broker aggregate OHLC must not be
    promoted into the canonical session state.
    """
    if not isinstance(quote, dict):
        raise DataIntegrityError("quote is not an object")
    current = _finite_positive(quote.get("current"))
    try:
        volume = int(quote.get("volume") or 0)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid quote volume") from exc
    if volume < 0:
        raise DataIntegrityError("invalid quote volume")
    return {"current": current, "volume": volume}


def validate_quote(quote: object) -> dict:
    """Strictly validate a complete quote snapshot.

    This validator remains intentionally strict. It is used when a complete
    OHLC snapshot is presented as a canonical quote. The live collector should
    use validate_live_quote instead and derive session OHLC from validated
    intraday candles.
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
    if high < max(opn, current) or low > min(opn, current) or high < low:
        raise DataIntegrityError("invalid quote OHLC bounds")
    if close is not None and (high < close or low > close):
        raise DataIntegrityError("invalid quote OHLC bounds")

    return {
        "current": current,
        "open": opn,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def validate_live_tick(
    ltp: object,
    volume: object,
    ltt_epoch: object,
    now: datetime,
    previous_volume: object = None,
    *,
    max_exchange_age_seconds: int = 300,
) -> tuple[float, int, int]:
    """Validate a live LTP using receipt time for liveness.

    The broker's day-open/day-high/day-low fields are deliberately excluded
    from this validation. They are not the canonical session object. Exchange
    timestamp is used for chronology/freshness when plausible; packet receipt
    time remains the authoritative liveness signal.
    """
    price = _finite_positive(ltp)
    try:
        vol = int(volume)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("invalid tick volume") from exc
    if vol < 0:
        raise DataIntegrityError("invalid tick volume")
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


def validate_tick(
    ltp: object,
    volume: object,
    ltt_epoch: object,
    day_open: object,
    day_high: object,
    day_low: object,
    now: datetime,
    previous_volume: object = None,
) -> tuple[float, int, int, float, float, float]:
    """Strictly validate a complete broker tick packet.

    Kept for tests and callers that explicitly require broker OHLC integrity.
    The live collector uses validate_live_tick because broker aggregate OHLC is
    not the canonical session OHLC source.
    """
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
    if embedded_ts > now + timedelta(seconds=5):
        raise DataIntegrityError("future live tick")
    receipt = now
    if not (IST_OPEN <= receipt.timetz().replace(tzinfo=None) < IST_CLOSE) or receipt.date() != now.date():
        raise DataIntegrityError("tick received outside current NSE session")
    receipt_epoch = int(receipt.timestamp())
    if previous_volume is not None and vol < int(previous_volume):
        raise DataIntegrityError("live cumulative volume moved backwards")
    return price, vol, receipt_epoch, opn, high, low


# TEMPORARY SAFE QUOTE-BOUNDARY DIAGNOSTICS.
# This instrumentation logs only market-data fields; credentials and auth headers
# are never logged. It is intentionally installed at import time so runner.py
# captures the wrapped Dhan REST adapter as its original fetch path.
def _install_quote_boundary_diagnostics() -> None:
    import logging
    import sys

    main = sys.modules.get("main")
    if main is None or not hasattr(main, "dhan_post"):
        return
    if getattr(main, "_psy29_quote_diag_installed", False):
        return

    original_dhan_post = main.dhan_post
    diag_log = logging.getLogger("psy29.quote_diagnostics")

    def diagnostic_dhan_post(url, *, token, payload, client_id=None,
                             kind="data", timeout=25, label="Dhan API"):
        response = original_dhan_post(
            url,
            token=token,
            payload=payload,
            client_id=client_id,
            kind=kind,
            timeout=timeout,
            label=label,
        )
        if label == "marketfeed:quote":
            try:
                body = response.json()
                data = body.get("data") or {}
                raw_quotes = data.get("NSE_EQ") or {}
                requested_ids = [str(v) for v in payload.get("NSE_EQ", [])]
                stock_names = list(getattr(main, "STOCKS", []))
                request_symbol = {
                    requested_ids[i]: stock_names[i]
                    for i in range(min(len(requested_ids), len(stock_names)))
                }
                returned_ids = {str(k) for k in raw_quotes.keys()}
                diag_log.warning(
                    "QUOTE_BOUNDARY_REQUEST requested_count=%d returned_count=%d "
                    "requested_ids=%s returned_ids=%s",
                    len(requested_ids),
                    len(returned_ids),
                    requested_ids,
                    sorted(returned_ids),
                )
                for sid in requested_ids:
                    item = raw_quotes.get(sid)
                    if item is None:
                        item = raw_quotes.get(int(sid)) if sid.isdigit() else None
                    symbol = request_symbol.get(sid, "UNKNOWN")
                    if not isinstance(item, dict):
                        diag_log.warning(
                            "QUOTE_BOUNDARY symbol=%s security_id=%s returned=false "
                            "raw_type=%s raw_quote=%r",
                            symbol, sid, type(item).__name__, item,
                        )
                        continue
                    ohlc = item.get("ohlc") if isinstance(item.get("ohlc"), dict) else {}
                    safe = {
                        "last_price": item.get("last_price"),
                        "volume": item.get("volume"),
                        "ohlc": {
                            "open": ohlc.get("open"),
                            "high": ohlc.get("high"),
                            "low": ohlc.get("low"),
                            "close": ohlc.get("close"),
                        },
                        "last_trade_time": item.get("last_trade_time"),
                    }
                    diag_log.warning(
                        "QUOTE_BOUNDARY symbol=%s security_id=%s returned=true raw=%r",
                        symbol, sid, safe,
                    )
            except Exception:
                diag_log.exception("QUOTE_BOUNDARY diagnostic parse failure")
        return response

    main.dhan_post = diagnostic_dhan_post
    main._psy29_quote_diag_installed = True


_install_quote_boundary_diagnostics()
