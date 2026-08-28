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
MAX_FUTURE_TICK_SKEW_SECONDS = 60
MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS = 86400
DHAN_QUOTE_PACKET_FORMAT = "<BHBIfHIfIIIffff"
DHAN_QUOTE_PACKET_SIZE = struct.calcsize(DHAN_QUOTE_PACKET_FORMAT)


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


def parse_dhan_quote_packet(data: bytes):
    """Parse one DhanHQ v2 NSE_EQ Quote packet using the official wire layout."""
    if not isinstance(data, (bytes, bytearray, memoryview)):
        return None
    if len(data) < DHAN_QUOTE_PACKET_SIZE:
        return None
    try:
        code, message_length, exchange_segment, security_id, ltp, ltq, ltt, atp, volume, total_sell, total_buy, day_open, day_close, day_high, day_low = struct.unpack(
            DHAN_QUOTE_PACKET_FORMAT, bytes(data[:DHAN_QUOTE_PACKET_SIZE])
        )
    except struct.error:
        return None
    if code != 4 or exchange_segment != 1:
        return None
    if message_length < DHAN_QUOTE_PACKET_SIZE or message_length > len(data):
        return None
    prices = (ltp, atp, day_open, day_close, day_high, day_low)
    if not all(math.isfinite(value) for value in prices):
        return None
    if ltp <= 0 or day_open <= 0 or day_high <= 0 or day_low <= 0 or atp < 0:
        return None
    if volume <= 0 or ltq < 0 or total_sell < 0 or total_buy < 0:
        return None
    return security_id, ltp, volume, ltt, day_open, day_high, day_low


def validate_ohlcv_row(row: dict, trading_date: str, *, session_only: bool = True, allow_zero_volume: bool = False) -> dict:
    try:
        ts = datetime.fromisoformat(str(row["timestamp"])); epoch = int(row["epoch"])
        values = [_finite_positive(row[key]) for key in ("open", "high", "low", "close")]; volume = int(row.get("volume", 0))
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise DataIntegrityError("malformed candle") from exc
    if ts.tzinfo is None or ts.date().isoformat() != trading_date: raise DataIntegrityError("candle timestamp outside trading date")
    if session_only and not (IST_OPEN <= ts.timetz().replace(tzinfo=None) < IST_CLOSE): raise DataIntegrityError("candle timestamp outside NSE session")
    if ts > datetime.now(ts.tzinfo): raise DataIntegrityError("future candle")
    if ts.second != 0 or ts.microsecond != 0: raise DataIntegrityError("candle timestamp is not minute-aligned")
    if abs(ts.timestamp() - epoch) > 1: raise DataIntegrityError("candle epoch does not match timestamp")
    if volume <= 0 and not allow_zero_volume: raise DataIntegrityError("zero/negative candle volume")
    opn, high, low, close = values
    if high < max(opn, close) or low > min(opn, close) or high < low: raise DataIntegrityError("invalid OHLC bounds")
    return dict(row)


def validate_intraday_rows(rows: object, trading_date: str) -> list[dict]:
    if not isinstance(rows, list) or not rows: raise DataIntegrityError("empty intraday history")
    valid=[]; previous_epoch=None
    for index,row in enumerate(rows):
        if not isinstance(row,dict): raise DataIntegrityError(f"malformed candle at index {index}")
        clean=validate_ohlcv_row(row,trading_date,allow_zero_volume=True); epoch=int(clean["epoch"])
        if previous_epoch is not None and epoch<=previous_epoch: raise DataIntegrityError("duplicate candle" if epoch==previous_epoch else "non-chronological candle")
        previous_epoch=epoch
        if int(clean.get("volume",0))<=0:
            logging.getLogger("psy29.data_integrity").warning("Quarantined intraday candle index=%d timestamp=%s reason=zero/negative candle volume",index,clean.get("timestamp")); continue
        valid.append(clean)
    if len(valid)<15: raise DataIntegrityError(f"insufficient valid intraday history: {len(valid)} rows")
    return valid


def validate_live_quote(quote: object) -> dict:
    if not isinstance(quote,dict): raise DataIntegrityError("quote is not an object")
    current=_finite_positive(quote.get("current"))
    try: volume=int(quote.get("volume"))
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid quote volume") from exc
    if volume<=0: raise DataIntegrityError("zero/negative quote volume")
    return {"current":current,"volume":volume}


def validate_quote(quote: object) -> dict:
    if not isinstance(quote,dict): raise DataIntegrityError("quote is not an object")
    required=("current","open","high","low")
    if any(key not in quote or quote[key] is None for key in required): raise DataIntegrityError("quote missing required market fields")
    current=_finite_positive(quote["current"]); opn=_finite_positive(quote["open"]); high=_finite_positive(quote["high"]); low=_finite_positive(quote["low"])
    close=quote.get("close"); close=_finite_positive(close) if close is not None else None
    try: volume=int(quote.get("volume"))
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid quote OHLC/volume") from exc
    if volume<=0: raise DataIntegrityError("zero/negative quote volume")
    if high<max(opn,current) or low>min(opn,current) or high<low: raise DataIntegrityError("invalid quote OHLC bounds")
    if close is not None and (high<close or low>close): raise DataIntegrityError("invalid quote OHLC bounds")
    return {"current":current,"open":opn,"high":high,"low":low,"close":close,"volume":volume}


def _validated_tick(ltp, volume, ltt_epoch, now, previous_volume=None, max_exchange_age_seconds=300):
    price=_finite_positive(ltp)
    try: vol=int(volume)
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid tick volume") from exc
    if vol<=0: raise DataIntegrityError("zero/negative tick volume")
    ltt=_normalize_epoch_seconds(ltt_epoch)
    try: embedded_ts=datetime.fromtimestamp(ltt, now.tzinfo)
    except (OverflowError,OSError,ValueError) as exc: raise DataIntegrityError("invalid tick timestamp") from exc

    future=(embedded_ts-now).total_seconds()
    effective_ltt=ltt
    if future>MAX_FUTURE_TICK_SKEW_SECONDS:
        if future > MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS:
            raise DataIntegrityError("future live tick")
        logging.getLogger("psy29.data_integrity").warning(
            "Accepted Dhan exchange-clock skew: %.1fs; using receipt time for candle placement",
            future,
        )
        effective_ltt=int(now.timestamp())
        embedded_ts=now
    elif future>5:
        logging.getLogger("psy29.data_integrity").warning("Accepted exchange-clock skew on live tick: %.1fs",future)

    if embedded_ts.date()!=now.date():
        logging.getLogger("psy29.data_integrity").warning(
            "Dhan LTT date mismatch; using live receipt time for candle placement: ltt=%s now=%s",
            embedded_ts.isoformat(), now.isoformat(),
        )
        effective_ltt=int(now.timestamp())
        embedded_ts=now
    if not (IST_OPEN<=embedded_ts.timetz().replace(tzinfo=None)<IST_CLOSE):
        logging.getLogger("psy29.data_integrity").warning(
            "Dhan LTT outside NSE session; using live receipt time: ltt=%s now=%s",
            embedded_ts.isoformat(), now.isoformat(),
        )
        effective_ltt=int(now.timestamp())
        embedded_ts=now
    if (now-embedded_ts).total_seconds()>max_exchange_age_seconds: raise DataIntegrityError("stale live tick")
    if not (IST_OPEN<=now.timetz().replace(tzinfo=None)<IST_CLOSE): raise DataIntegrityError("tick received outside current NSE session")
    if previous_volume is not None and vol<int(previous_volume): raise DataIntegrityError("live cumulative volume moved backwards")
    return price,vol,effective_ltt


def validate_live_tick(ltp, volume, ltt_epoch, now, previous_volume=None, *, max_exchange_age_seconds=300):
    return _validated_tick(ltp,volume,ltt_epoch,now,previous_volume,max_exchange_age_seconds)


def validate_tick(ltp, volume, ltt_epoch, day_open, day_high, day_low, now, previous_volume=None):
    price,vol,ltt=_validated_tick(ltp,volume,ltt_epoch,now,previous_volume)
    opn=_finite_positive(day_open); high=_finite_positive(day_high); low=_finite_positive(day_low)
    if high<low or high<opn or low>opn or not low<=price<=high: raise DataIntegrityError("invalid live tick market values")
    return price,vol,ltt,opn,high,low


def _install_quote_boundary_diagnostics():
    main=sys.modules.get("main")
    if main is None or not hasattr(main,"dhan_post") or getattr(main,"_psy29_quote_diag_installed",False): return
    logger=logging.getLogger("psy29.quote_diagnostics"); original_post=main.dhan_post
    def diagnostic_dhan_post(url,*,token,payload,client_id=None,kind="data",timeout=25,label="Dhan API"):
        response=original_post(url,token=token,payload=payload,client_id=client_id,kind=kind,timeout=timeout,label=label)
        if label=="marketfeed:quote":
            try:
                body=response.json(); raw_quotes=(body.get("data") or {}).get("NSE_EQ") or {}; ids=[str(v) for v in payload.get("NSE_EQ",[])]; stocks=list(getattr(main,"STOCKS",[])); mapping={ids[i]:stocks[i] for i in range(min(len(ids),len(stocks)))}
                logger.warning("QUOTE_BOUNDARY_REQUEST requested_count=%d returned_count=%d",len(ids),len(raw_quotes))
                for sid in ids:
                    item=raw_quotes.get(sid) or (raw_quotes.get(int(sid)) if sid.isdigit() else None); symbol=mapping.get(sid,"UNKNOWN")
                    if not isinstance(item,dict): logger.warning("QUOTE_BOUNDARY symbol=%s security_id=%s returned=false",symbol,sid); continue
                    ohlc=item.get("ohlc") if isinstance(item.get("ohlc"),dict) else {}; logger.warning("QUOTE_BOUNDARY symbol=%s security_id=%s raw=%r",symbol,sid,{"last_price":item.get("last_price"),"volume":item.get("volume"),"open":ohlc.get("open"),"high":ohlc.get("high"),"low":ohlc.get("low"),"close":ohlc.get("close"),"last_trade_time":item.get("last_trade_time")})
            except Exception: logger.exception("QUOTE_BOUNDARY diagnostic parse failure")
        return response
    main.dhan_post=diagnostic_dhan_post; main._psy29_quote_diag_installed=True


def _install_canonical_ws_parser():
    main=sys.modules.get("main")
    if main is None: return
    main.parse_quote_packet = parse_dhan_quote_packet
    main._psy29_ws_parser_canonical=True
    if hasattr(main,"websocket_loop") and not getattr(main,"_psy29_ws_loop_guard_installed",False):
        original_loop=main.websocket_loop
        async def guarded_websocket_loop(token):
            previous_parser=getattr(main,"parse_quote_packet",None)
            main.parse_quote_packet=parse_dhan_quote_packet
            try:
                return await original_loop(token)
            finally:
                if previous_parser is not None:
                    main.parse_quote_packet=previous_parser
        main.websocket_loop=guarded_websocket_loop
        main._psy29_ws_loop_guard_installed=True


_install_quote_boundary_diagnostics(); _install_canonical_ws_parser()
