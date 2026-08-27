import json
import math
import struct
import threading
import time
from datetime import datetime

import main
from fastapi.responses import Response, JSONResponse
from psy29.intraday_store import IntradayStore
from psy29.data_integrity import DataIntegrityError, validate_intraday_rows, validate_quote, validate_tick, validate_ohlcv_row

main.app.router.on_startup.clear()
store = IntradayStore()
CHECKPOINT_SECONDS = 60.0
AUTH_COOLDOWN_SECONDS = 125.0


def _parse_quote_packet(data: bytes):
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
    prices = (ltp, atp, day_open, day_close, day_high, day_low)
    if not all(math.isfinite(v) for v in prices) or ltp <= 0 or atp < 0 or day_open <= 0 or day_high <= 0 or day_low <= 0:
        return None
    if volume < 0 or ltq < 0 or total_sell < 0 or total_buy < 0:
        return None
    if day_high < day_low or day_high < day_open or day_low > day_open or not day_low <= ltp <= day_high:
        return None
    return security_id, ltp, volume, ltt, day_open, day_high, day_low

main.parse_quote_packet = _parse_quote_packet


def _validated_intraday_fetch(token, security_id, from_dt, to_dt):
    rows = main._original_fetch_intraday_1m(token, security_id, from_dt, to_dt)
    return validate_intraday_rows(rows, main.now_ist().date().isoformat())


def _validated_previous_day(token, security_id, today):
    prev = main._original_fetch_previous_day(token, security_id, today)
    if not isinstance(prev, dict) or any(prev.get(k) is None for k in ("high", "low", "close")):
        raise DataIntegrityError("missing previous-day OHLC")
    values = {k: float(prev[k]) for k in ("high", "low", "close")}
    if not all(math.isfinite(v) and v > 0 for v in values.values()) or values["high"] < values["low"]:
        raise DataIntegrityError("invalid previous-day OHLC")
    return values

main._original_fetch_intraday_1m = main.fetch_intraday_1m
main._original_fetch_previous_day = main.fetch_previous_day
main.fetch_intraday_1m = _validated_intraday_fetch
main.fetch_previous_day = _validated_previous_day


def _validated_market_quote(token, client_id, security_map):
    raw = main._original_fetch_market_quote(token, client_id, security_map)
    clean, rejected = {}, []
    for symbol in main.STOCKS:
        try:
            clean[symbol] = validate_quote(raw.get(symbol))
        except DataIntegrityError:
            rejected.append(symbol)
    if rejected:
        raise DataIntegrityError("Dhan quote integrity failure: " + ",".join(rejected))
    return clean

main._original_fetch_market_quote = main.fetch_market_quote
main.fetch_market_quote = _validated_market_quote


def _snapshot():
    with main.lock:
        return main.state.get("trading_date"), {k: main.clean_stock(v) for k, v in main.state.get("stocks", {}).items()}


def _checkpoint(force=False):
    trading_date, stocks = _snapshot()
    if not trading_date or not stocks:
        return 0
    return store.save_market(trading_date, stocks)


def _checkpoint_worker():
    while True:
        try:
            if main.in_session(main.now_ist()):
                _checkpoint()
                time.sleep(CHECKPOINT_SECONDS)
            else:
                time.sleep(10)
        except Exception as exc:
            main.log.warning("Intraday checkpoint worker error: %s", exc)
            time.sleep(CHECKPOINT_SECONDS)


def _seed_live_state(token, security_map):
    now = main.now_ist()
    quote_map = main.fetch_market_quote(token, main.os.environ["DHAN_CLIENT_ID"], security_map)
    with main.lock:
        main.state["trading_date"] = now.date().isoformat()
        main.state["market_session_status"] = main.session_status(now)
        main.state["security_map"] = security_map
        main.state["stocks"] = {}
        for symbol in main.STOCKS:
            q = quote_map[symbol]
            main.state["stocks"][symbol] = {
                "symbol": symbol, "security_id": security_map[symbol], "current_price": q["current"],
                "ohlc": {"open": q["open"], "high": q["high"], "low": q["low"], "close": q["close"]},
                "session_high": q["high"], "session_low": q["low"],
                "previous_day": {"high": None, "low": None, "close": None}, "volume": q["volume"],
                "candles": {"1m": [], "5m": [], "15m": [], "1h": []}, "vwap": None, "ema9": None, "ema20": None,
                "opening_range": {"period": "09:15-09:30", "status": "FORMING", "high": None, "low": None},
                "structure": {"trend": "INSUFFICIENT_DATA", "swing_high": None, "swing_low": None},
                "timestamp": now.isoformat(), "trading_date": now.date().isoformat(),
                "market_session_status": main.session_status(now), "data_source_status": "LIVE", "last_tick": None,
                "_one_min": [], "_volume_anchor": None,
            }


def _valid_saved_candles(candles, trading_date):
    if not isinstance(candles, dict):
        return False
    try:
        for tf in ("1m", "5m", "15m", "1h"):
            rows = candles.get(tf, [])
            if not isinstance(rows, list):
                return False
            previous = None
            for row in rows:
                validate_ohlcv_row(row, trading_date, session_only=(tf != "1h"))
                ts = datetime.fromisoformat(str(row["timestamp"]))
                if previous is not None and ts <= previous:
                    return False
                previous = ts
    except (DataIntegrityError, KeyError, TypeError, ValueError, OverflowError):
        return False
    return True


def _restore_checkpoint_if_needed(trading_date):
    saved = store.load_session(trading_date)
    if not saved:
        return
    with main.lock:
        for symbol, payload in saved.items():
            current = main.state["stocks"].get(symbol)
            if not current or not isinstance(payload, dict):
                continue
            candles = payload.get("candles")
            if _valid_saved_candles(candles, trading_date):
                current["candles"] = candles
                current["_one_min"] = list(candles.get("1m", []))
                for key in ("vwap", "ema9", "ema20", "opening_range", "structure", "session_high", "session_low"):
                    if key in payload:
                        current[key] = payload[key]
            current["_volume_anchor"] = None


def _validated_rebuild_stock(symbol, one_min, quote, prev):
    validated = validate_intraday_rows(one_min, main.now_ist().date().isoformat())
    clean_quote = validate_quote(quote)
    main._original_rebuild_stock(symbol, validated, clean_quote, prev)

main._original_rebuild_stock = main.rebuild_stock
main.rebuild_stock = _validated_rebuild_stock


def _guarded_update_tick(symbol, price, volume, ltt_epoch, day_open, day_high, day_low):
    now = main.now_ist()
    with main.lock:
        stock = main.state["stocks"].get(symbol)
        previous_volume = stock.get("volume") if stock else None
    try:
        clean = validate_tick(price, volume, ltt_epoch, day_open, day_high, day_low, now, previous_volume)
    except DataIntegrityError as exc:
        main.log.warning("Rejected corrupt live tick for %s: %s", symbol, exc)
        return
    main._original_update_tick(symbol, *clean)

main._original_update_tick = main.update_tick
main.update_tick = _guarded_update_tick


def _backfill_worker(token, security_map):
    today = main.now_ist()
    for symbol in main.STOCKS:
        if not main.in_session(main.now_ist()):
            return
        try:
            from_dt = today.replace(hour=9, minute=15, second=0, microsecond=0)
            one_min = main.fetch_intraday_1m(token, security_map[symbol], from_dt, main.now_ist())
            prev = main.fetch_previous_day(token, security_map[symbol], today)
            with main.lock:
                s = main.state["stocks"].get(symbol)
                if not s:
                    continue
                quote = {"current": s.get("current_price"), "open": s.get("ohlc", {}).get("open"), "high": s.get("ohlc", {}).get("high"), "low": s.get("ohlc", {}).get("low"), "close": s.get("ohlc", {}).get("close"), "volume": s.get("volume")}
                last_tick, status = s.get("last_tick"), s.get("data_source_status")
            main.rebuild_stock(symbol, one_min, quote, prev)
            with main.lock:
                if symbol in main.state["stocks"]:
                    main.state["stocks"][symbol]["last_tick"] = last_tick
                    if status == "LIVE":
                        main.state["stocks"][symbol]["data_source_status"] = "LIVE"
            time.sleep(0.22)
        except Exception:
            main.log.exception("Background backfill failed for %s", symbol)
    _checkpoint(force=True)


def _supervisor():
    while True:
        try:
            now = main.now_ist()
            if not main.in_session(now):
                with main.lock:
                    main.state["market_session_status"] = main.session_status(now)
                time.sleep(5)
                continue
            token, expiry = main.generate_access_token()
            with main.lock:
                main.state["access_token_expiry"] = expiry
                main.state["market_session_status"] = "OPEN"
                main.state["source_status"] = "CONNECTING"
            security_map = main.load_security_map()
            _seed_live_state(token, security_map)
            _restore_checkpoint_if_needed(now.date().isoformat())
            threading.Thread(target=_backfill_worker, args=(token, security_map), daemon=True, name="psy29-backfill").start()
            main.asyncio.run(main.websocket_loop(token))
            _checkpoint(force=True)
            with main.lock:
                main.state["source_status"] = "POST_CLOSE" if not main.in_session(main.now_ist()) else "RECONNECTING"
            if main.in_session(main.now_ist()):
                time.sleep(3)
        except Exception as exc:
            message = str(exc)
            if "Token can be generated once every 2 minutes" in message:
                with main.lock:
                    main.state["source_status"] = "AUTH_COOLDOWN"
                time.sleep(AUTH_COOLDOWN_SECONDS)
                continue
            main.log.exception("Collector supervisor failure: %s", exc)
            with main.lock:
                main.state["source_status"] = "ERROR"
            time.sleep(5)


def _machine_payload():
    with main.lock:
        raw = {"service": "PSY29 Live Data", "timestamp": main.now_ist().isoformat(), "trading_date": main.state["trading_date"], "market_session_status": main.state["market_session_status"], "data_source_status": main.state["source_status"], "stocks_expected": 29, "stocks": {k: main.clean_stock(v) for k, v in main.state["stocks"].items()}}
    unsafe = []
    for symbol, stock in raw["stocks"].items():
        try:
            if stock.get("trading_date") != raw["trading_date"]:
                raise DataIntegrityError("stock trading date mismatch")
            candles = stock.get("candles") or {}
            for tf in ("1m", "5m", "15m", "1h"):
                rows = candles.get(tf, [])
                previous = None
                for row in rows:
                    validate_ohlcv_row(row, raw["trading_date"], session_only=(tf != "1h"))
                    ts = datetime.fromisoformat(str(row["timestamp"]))
                    if previous is not None and ts <= previous:
                        raise DataIntegrityError(f"{tf} candle order invalid")
                    previous = ts
            one_min = candles.get("1m", [])
            if not one_min:
                raise DataIntegrityError("1m candle history missing")
            for key, expected in (("vwap", main.calc_vwap(one_min)), ("ema9", main.calc_ema(one_min, 9)), ("ema20", main.calc_ema(one_min, 20))):
                supplied = stock.get(key)
                if supplied is None or expected is None or not math.isfinite(float(supplied)) or abs(float(supplied) - expected) > max(0.01, abs(expected) * 0.0001):
                    raise DataIntegrityError(f"{key} does not match validated candles")
            validate_quote({"current": stock.get("current_price"), "open": stock.get("ohlc", {}).get("open"), "high": stock.get("ohlc", {}).get("high"), "low": stock.get("ohlc", {}).get("low"), "close": stock.get("ohlc", {}).get("close"), "volume": stock.get("volume")})
            if stock.get("last_tick") and datetime.fromisoformat(stock["last_tick"]).date().isoformat() != raw["trading_date"]:
                raise DataIntegrityError("last_tick date mismatch")
        except (DataIntegrityError, TypeError, ValueError, OverflowError) as exc:
            unsafe.append(f"{symbol}: {exc}")
    if unsafe:
        raw["data_source_status"] = "DATA_UNSAFE"
        raw["diagnostic"] = {"status": "ERROR", "error_code": "DATA_INTEGRITY_FAILURE", "error_message": "; ".join(unsafe), "stage": "PUBLIC_GATE", "affected_stocks": [x.split(":",1)[0] for x in unsafe], "recovery_action": "REJECT_PAYLOAD_AND_REFRESH_FROM_DHAN", "data_safe": False}
    else:
        raw["diagnostic"] = {"status": "OK", "error_code": None, "error_message": None, "stage": None, "affected_stocks": [], "recovery_action": None, "data_safe": True}
    return main.normalize_market(raw)


def _json_response():
    body = _machine_payload()
    return JSONResponse(content=body, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0", "Surrogate-Control": "no-store", "Access-Control-Allow-Origin": "*", "X-Content-Type-Options": "nosniff"})


@main.app.get("/api/v1/live.json")
def live_json():
    return _json_response()


@main.app.get("/api/v1/market.json")
def market_json():
    return _json_response()


@main.app.get("/data.json")
def data_json():
    return _json_response()


@main.app.get("/data.txt")
def data_txt():
    body = json.dumps(_machine_payload(), separators=(",", ":"), ensure_ascii=False)
    return Response(content=body, media_type="text/plain", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0", "Access-Control-Allow-Origin": "*"})


@main.app.get("/live.txt")
def live_txt():
    return data_txt()


def startup():
    with main.lock:
        main.state["collector_started"] = True
    threading.Thread(target=_checkpoint_worker, daemon=True, name="psy29-db-checkpoint").start()
    threading.Thread(target=_supervisor, daemon=True, name="psy29-supervisor").start()

main.app.add_event_handler("startup", startup)
app = main.app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(main.os.getenv("PORT", "10000")))
