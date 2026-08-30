from __future__ import annotations

import asyncio
import json
import math
import struct
import threading
import time
from datetime import datetime, timedelta, time as dtime

import main
from fastapi.responses import JSONResponse, Response
from psy29.data_integrity import DataIntegrityError, parse_dhan_quote_packet, validate_live_quote, validate_ohlcv_row
from psy29.intraday_store import IntradayStore
from psy29.session_reset import reset_for_trading_date

# PSY29 canonical session rules: NSE cash market 09:15-15:30 IST.
# main.py still contains the old 15:15 boundary, so the production runner
# explicitly overrides that shared constant before any session loop starts.
main.MARKET_OPEN = (9, 15)
main.MARKET_CLOSE = (15, 30)

# This runner deliberately does NOT use WebSocket ticks to manufacture 1m candles.
# Dhan REST intraday 1m OHLCV is the sole canonical candle source.
CANDLE_SOURCE = "DHAN_V2_REST_INTRADAY_1M_COMPLETED"
CANDLE_START = dtime(9, 15)
CANDLE_CLOSE = dtime(15, 30)
FIRST_COMPLETED_MINUTE = 9 * 60 + 16
CANDLE_FETCH_DELAY_SECONDS = 2.0
CANDLE_RETRY_SECONDS = 2.0
CANDLE_RETRY_WINDOW_SECONDS = 18.0
CHECKPOINT_SECONDS = 60.0
AUTH_COOLDOWN_SECONDS = 125.0

main.app.router.on_startup.clear()
store = IntradayStore()


def _snapshot():
    with main.lock:
        return main.state.get("trading_date"), {k: main.clean_stock(v) for k, v in main.state.get("stocks", {}).items()}


def _checkpoint(force=False):
    trading_date, stocks = _snapshot()
    if not trading_date or not stocks:
        return 0
    try:
        return store.save_market(trading_date, stocks)
    except Exception as exc:
        main.log.warning("Intraday checkpoint failed: %s", exc)
        return 0


def _checkpoint_worker():
    while True:
        try:
            if main.in_session(main.now_ist()):
                _checkpoint()
                time.sleep(CHECKPOINT_SECONDS)
            else:
                time.sleep(10)
        except Exception as exc:
            main.log.warning("Checkpoint worker error: %s", exc)
            time.sleep(CHECKPOINT_SECONDS)


def _parse_live_packet(data: bytes):
    parsed = parse_dhan_quote_packet(data)
    if parsed is None:
        return None
    return parsed


def _seed_state(token: str, security_map: dict[str, str]):
    now = main.now_ist()
    client_id = main.os.environ["DHAN_CLIENT_ID"]
    raw_quotes = main._original_fetch_market_quote(token, client_id, security_map)

    with main.lock:
        main.state["trading_date"] = now.date().isoformat()
        main.state["market_session_status"] = main.session_status(now)
        main.state["security_map"] = security_map
        main.state["stocks"] = {}

    failures = []
    for symbol in main.STOCKS:
        try:
            q = raw_quotes.get(symbol)
            live = validate_live_quote(q)
            prev = main._original_fetch_previous_day(token, security_map[symbol], now)
            if not isinstance(prev, dict) or any(prev.get(k) is None for k in ("high", "low", "close")):
                raise DataIntegrityError("missing previous-day OHLC")
            prev = {k: float(prev[k]) for k in ("high", "low", "close")}
            if not all(math.isfinite(v) and v > 0 for v in prev.values()):
                raise DataIntegrityError("invalid previous-day OHLC")
            if prev["high"] < prev["low"] or prev["high"] < prev["close"] or prev["low"] > prev["close"]:
                raise DataIntegrityError("invalid previous-day OHLC bounds")
        except Exception as exc:
            failures.append(symbol)
            main.log.warning("Seed validation failed for %s: %s", symbol, exc)
            q = raw_quotes.get(symbol) or {}
            live = {"current": float(q.get("current")) if q.get("current") is not None else None,
                    "volume": int(q.get("volume") or 0)}
            prev = {"high": None, "low": None, "close": None}

        with main.lock:
            main.state["stocks"][symbol] = {
                "symbol": symbol,
                "security_id": security_map[symbol],
                "current_price": live.get("current"),
                "ohlc": {"open": None, "high": None, "low": None, "close": None},
                "session_high": None,
                "session_low": None,
                "previous_day": prev,
                "volume": live.get("volume"),
                "candles": {"1m": [], "5m": [], "15m": [], "1h": []},
                "vwap": None,
                "ema9": None,
                "ema20": None,
                "opening_range": {"period": "09:15-09:30", "status": "NOT_FORMED", "high": None, "low": None},
                "structure": {"trend": "INSUFFICIENT_DATA", "swing_high": None, "swing_low": None},
                "timestamp": now.isoformat(),
                "trading_date": now.date().isoformat(),
                "market_session_status": main.session_status(now),
                "data_source_status": "LIVE",
                "last_tick": None,
                "last_completed_candle": None,
                "completed_candle_count": 0,
                "candle_source": CANDLE_SOURCE,
                "_one_min": [],
                "_canonical_last_epoch": None,
            }

    main.log.info("Seeded %d/%d live stocks", len(main.STOCKS) - len(failures), len(main.STOCKS))
    return failures


def _completed_rows(rows: list[dict], trading_date: str, cutoff_epoch: int) -> list[dict]:
    """Validate Dhan 1m rows and keep only completed real candles.

    No minimum-row requirement is imposed here: at 09:16 there is exactly one
    possible completed regular-session candle. Missing minutes remain missing;
    the engine never fills them with synthetic OHLCV.
    """
    if not isinstance(rows, list):
        raise DataIntegrityError("intraday response is not a list")
    valid = []
    previous = None
    for row in sorted(rows, key=lambda r: int(r.get("epoch", 0)) if isinstance(r, dict) else 0):
        if not isinstance(row, dict):
            raise DataIntegrityError("malformed intraday candle")
        epoch = int(row["epoch"])
        if epoch > cutoff_epoch:
            continue
        validate_ohlcv_row(row, trading_date, session_only=True)
        if previous is not None and epoch <= previous:
            raise DataIntegrityError("duplicate/non-chronological Dhan candle")
        previous = epoch
        valid.append(dict(row))
    return valid


def _live_metadata(symbol: str):
    with main.lock:
        s = main.state["stocks"].get(symbol, {})
        return s.get("current_price"), s.get("volume"), s.get("last_tick"), s.get("previous_day")


def _install_canonical_candles(symbol: str, rows: list[dict], quote_current, quote_volume, prev):
    if not rows:
        return False
    session_ohlc = {
        "open": rows[0]["open"],
        "high": max(r["high"] for r in rows),
        "low": min(r["low"] for r in rows),
        "close": rows[-1]["close"],
    }
    quote = {
        "current": quote_current,
        "open": session_ohlc["open"],
        "high": session_ohlc["high"],
        "low": session_ohlc["low"],
        "close": session_ohlc["close"],
        "volume": quote_volume,
    }
    if quote_current is None:
        quote["current"] = rows[-1]["close"]
    try:
        validate_live_quote({"current": quote["current"], "volume": int(quote.get("volume") or 0)})
    except DataIntegrityError:
        quote["volume"] = max(1, int(quote.get("volume") or 0))

    # main.rebuild_stock derives 5m/15m/1h only from these real 1m rows.
    main.rebuild_stock(symbol, rows, quote, prev or {"high": None, "low": None, "close": None})
    with main.lock:
        stock = main.state["stocks"].get(symbol)
        if not stock:
            return False
        last_tick = stock.get("last_tick")
        stock["ohlc"] = session_ohlc
        stock["session_high"] = session_ohlc["high"]
        stock["session_low"] = session_ohlc["low"]
        stock["current_price"] = quote["current"]
        stock["volume"] = quote["volume"]
        stock["last_tick"] = last_tick
        stock["data_source_status"] = "LIVE"
        stock["candle_source"] = CANDLE_SOURCE
        stock["completed_candle_count"] = len(rows)
        stock["last_completed_candle"] = rows[-1]["timestamp"]
        stock["_canonical_last_epoch"] = int(rows[-1]["epoch"])
        stock["_one_min"] = list(rows)
        stock["candles"]["1m"] = list(rows)
        main.state["last_update"] = main.now_ist().isoformat()
    return True


def _refresh_symbol_completed(token: str, symbol: str, security_id: str, target_epoch: int):
    now = main.now_ist()
    from_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)
    to_dt = datetime.fromtimestamp(target_epoch + 60, main.IST)
    rows = main._original_fetch_intraday_1m(token, security_id, from_dt, to_dt)
    completed = _completed_rows(rows, now.date().isoformat(), target_epoch)
    current, volume, last_tick, prev = _live_metadata(symbol)
    if not completed:
        main.log.warning("No completed Dhan 1m candle for %s target=%s; NO SYNTHETIC CANDLE CREATED", symbol, datetime.fromtimestamp(target_epoch, main.IST).strftime("%H:%M"))
        return False
    return _install_canonical_candles(symbol, completed, current, volume, prev)


def _canonical_minute_worker(token: str, security_map: dict[str, str]):
    # Never create a candle at 09:15. The first possible completed candle is
    # the 09:15-09:15:59 candle, published/accepted at the 09:16 boundary.
    last_target = None
    while main.in_session(main.now_ist()):
        now = main.now_ist()
        minute_index = now.hour * 60 + now.minute
        if minute_index < FIRST_COMPLETED_MINUTE:
            time.sleep(0.5)
            continue

        target_epoch = int(now.replace(second=0, microsecond=0).timestamp()) - 60
        if last_target == target_epoch:
            time.sleep(0.25)
            continue

        # Give the broker a short publication window after the minute closes.
        target_boundary = datetime.fromtimestamp(target_epoch + 60, main.IST)
        delay = (target_boundary - now).total_seconds() + CANDLE_FETCH_DELAY_SECONDS
        if delay > 0:
            time.sleep(delay)
            continue

        deadline = time.monotonic() + CANDLE_RETRY_WINDOW_SECONDS
        pending = set(main.STOCKS)
        while pending and time.monotonic() < deadline and main.in_session(main.now_ist()):
            for symbol in list(pending):
                try:
                    if _refresh_symbol_completed(token, symbol, security_map[symbol], target_epoch):
                        pending.discard(symbol)
                except Exception as exc:
                    main.log.warning("Canonical 1m refresh failed for %s target=%s: %s", symbol, target_epoch, exc)
            if pending:
                time.sleep(CANDLE_RETRY_SECONDS)

        if pending:
            main.log.error("Canonical minute %s incomplete: missing=%s; synthetic fill intentionally disabled", datetime.fromtimestamp(target_epoch, main.IST).strftime("%H:%M"), ",".join(sorted(pending)))
        else:
            main.log.info("Canonical 1m minute committed: %s | %d/%d stocks", datetime.fromtimestamp(target_epoch, main.IST).strftime("%H:%M"), len(main.STOCKS), len(main.STOCKS))
        last_target = target_epoch
        _checkpoint(force=True)


def _websocket_live_loop(token: str):
    """WebSocket is LTP/volume only. It is forbidden from mutating candle arrays."""
    client_id = main.os.environ["DHAN_CLIENT_ID"]
    reverse = {int(v): k for k, v in main.state["security_map"].items()}
    url = f"{main.WS_URL}?version=2&token={token}&clientId={client_id}&authType=2"
    bad_ltt = 0
    delay = 3.0
    while main.in_session(main.now_ist()):
        try:
            async def session():
                nonlocal bad_ltt, delay
                async with main.websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=None) as ws:
                    instruments = [{"ExchangeSegment": "NSE_EQ", "SecurityId": sid} for sid in main.state["security_map"].values()]
                    for start in range(0, len(instruments), 100):
                        batch = instruments[start:start + 100]
                        await ws.send(json.dumps({"RequestCode": 17, "InstrumentCount": len(batch), "InstrumentList": batch}))
                    with main.lock:
                        main.state["source_status"] = "LIVE"
                    delay = 3.0
                    while main.in_session(main.now_ist()):
                        message = await asyncio.wait_for(ws.recv(), timeout=35)
                        if isinstance(message, str) or not message:
                            continue
                        if message[0] == 50:
                            raise RuntimeError("Dhan websocket disconnect packet")
                        parsed = _parse_live_packet(message)
                        if not parsed:
                            continue
                        sid, ltp, volume, ltt, *_ = parsed
                        symbol = reverse.get(int(sid))
                        if not symbol:
                            continue
                        try:
                            live = validate_live_quote({"current": ltp, "volume": volume})
                        except DataIntegrityError as exc:
                            main.log.warning("Rejected corrupt live quote for %s: %s", symbol, exc)
                            continue
                        now = main.now_ist()
                        ltt_iso = None
                        try:
                            ltt_dt = datetime.fromtimestamp(int(ltt), main.IST)
                            if ltt_dt.date() == now.date() and main.MARKET_OPEN <= (ltt_dt.hour, ltt_dt.minute) < main.MARKET_CLOSE and abs((now - ltt_dt).total_seconds()) <= 300:
                                ltt_iso = ltt_dt.isoformat()
                            else:
                                bad_ltt += 1
                        except Exception:
                            bad_ltt += 1
                        with main.lock:
                            stock = main.state["stocks"].get(symbol)
                            if not stock:
                                continue
                            stock["current_price"] = live["current"]
                            stock["volume"] = live["volume"]
                            stock["timestamp"] = now.isoformat()
                            stock["market_session_status"] = main.session_status(now)
                            stock["data_source_status"] = "LIVE"
                            if ltt_iso:
                                stock["last_tick"] = ltt_iso
                            # Deliberately do not touch _one_min/candles here.
                            main.state["last_update"] = now.isoformat()
                        if bad_ltt and bad_ltt % 100 == 1:
                            main.log.warning("Dhan LTT rejected for candle timing %d times; live price retained, no synthetic candle fallback", bad_ltt)
            asyncio.run(session())
        except Exception as exc:
            with main.lock:
                main.state["source_status"] = "RECONNECTING"
            if not main.in_session(main.now_ist()):
                break
            main.log.warning("Dhan websocket disconnected: %s; retry in %.1fs", exc, delay)
            time.sleep(delay)
            delay = min(30.0, delay * 2.0)


def _supervisor():
    while True:
        try:
            now = main.now_ist()
            with main.lock:
                reset_for_trading_date(main.state, now.date())
            if not main.in_session(now):
                with main.lock:
                    main.state["market_session_status"] = main.session_status(now)
                time.sleep(5)
                continue

            token, expiry = main.generate_access_token()
            security_map = main.load_security_map()
            with main.lock:
                main.state["access_token_expiry"] = expiry
                main.state["market_session_status"] = "OPEN"
                main.state["source_status"] = "CONNECTING"

            _seed_state(token, security_map)
            threading.Thread(target=_canonical_minute_worker, args=(token, security_map), daemon=True, name="psy29-canonical-1m").start()
            _websocket_live_loop(token)
            _checkpoint(force=True)
            with main.lock:
                main.state["source_status"] = "POST_CLOSE" if not main.in_session(main.now_ist()) else "RECONNECTING"
            if main.in_session(main.now_ist()):
                time.sleep(3)
        except Exception as exc:
            if "Token can be generated once every 2 minutes" in str(exc):
                with main.lock:
                    main.state["source_status"] = "AUTH_COOLDOWN"
                time.sleep(AUTH_COOLDOWN_SECONDS)
                continue
            main.log.exception("Collector supervisor failure: %s", exc)
            with main.lock:
                main.state["source_status"] = "ERROR"
            time.sleep(5)


def _machine_payload():
    now = main.now_ist()
    trading_date = now.date().isoformat()
    with main.lock:
        reset_for_trading_date(main.state, now.date())
        raw = {
            "service": "PSY29 Live Data",
            "timestamp": now.isoformat(),
            "trading_date": main.state["trading_date"],
            "market_session_status": main.state["market_session_status"],
            "data_source_status": main.state["source_status"],
            "stocks_expected": len(main.STOCKS),
            "stocks": {k: main.clean_stock(v) for k, v in main.state["stocks"].items()},
            "candle_policy": {
                "source": CANDLE_SOURCE,
                "first_completed_minute": "09:16",
                "regular_session": "09:15-15:30 IST",
                "synthetic_candles": False,
            },
        }

    unsafe = []
    completed_cutoff = int(now.replace(second=0, microsecond=0).timestamp()) - 60 if main.in_session(now) else None
    for symbol, stock in raw["stocks"].items():
        try:
            if stock.get("trading_date") != trading_date:
                raise DataIntegrityError("stock trading date mismatch")
            one_min = list((stock.get("candles") or {}).get("1m", []))
            previous = None
            for row in one_min:
                validate_ohlcv_row(row, trading_date, session_only=True)
                epoch = int(row["epoch"])
                if completed_cutoff is not None and epoch > completed_cutoff:
                    raise DataIntegrityError("uncompleted 1m candle exposed")
                if previous is not None and epoch <= previous:
                    raise DataIntegrityError("1m candle order invalid")
                previous = epoch
            if not one_min:
                if main.in_session(now) and (now.hour * 60 + now.minute) >= FIRST_COMPLETED_MINUTE:
                    raise DataIntegrityError("1m candle history missing after first completed minute")
                continue

            expected = {"open": one_min[0]["open"], "high": max(r["high"] for r in one_min), "low": min(r["low"] for r in one_min), "close": one_min[-1]["close"]}
            supplied = stock.get("ohlc") or {}
            for key in expected:
                if supplied.get(key) is None or abs(float(supplied[key]) - float(expected[key])) > max(0.01, abs(float(expected[key])) * 0.0001):
                    raise DataIntegrityError(f"session OHLC {key} does not match canonical 1m candles")
            if stock.get("session_high") != expected["high"] or stock.get("session_low") != expected["low"]:
                raise DataIntegrityError("session range does not match canonical 1m candles")

            live = validate_live_quote({"current": stock.get("current_price"), "volume": stock.get("volume")})
            for key, expected_value in (("vwap", main.calc_vwap(one_min)), ("ema9", main.calc_ema(one_min, 9)), ("ema20", main.calc_ema(one_min, 20))):
                supplied_value = stock.get(key)
                if expected_value is None or supplied_value is None or not math.isfinite(float(supplied_value)) or abs(float(supplied_value) - float(expected_value)) > max(0.01, abs(float(expected_value)) * 0.0001):
                    raise DataIntegrityError(f"{key} does not match canonical candles")
            if stock.get("candle_source") != CANDLE_SOURCE:
                raise DataIntegrityError("candle source is not canonical Dhan REST")
        except (DataIntegrityError, TypeError, ValueError, OverflowError) as exc:
            unsafe.append(f"{symbol}: {exc}")

    if unsafe:
        raw["data_source_status"] = "DATA_UNSAFE"
        raw["diagnostic"] = {"status": "ERROR", "error_code": "DATA_INTEGRITY_FAILURE", "error_message": "; ".join(unsafe), "stage": "PUBLIC_GATE", "affected_stocks": [x.split(":", 1)[0] for x in unsafe], "recovery_action": "REFETCH_COMPLETED_DHAN_1M_CANDLES", "data_safe": False}
    else:
        raw["diagnostic"] = {"status": "OK", "error_code": None, "error_message": None, "stage": None, "affected_stocks": [], "recovery_action": None, "data_safe": True}
    return main.normalize_market(raw)


def _headers(refresh_id: str):
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0, s-maxage=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "Surrogate-Control": "no-store",
        "Vary": "*",
        "Access-Control-Allow-Origin": "*",
        "X-Content-Type-Options": "nosniff",
        "X-PSY29-Refresh-ID": refresh_id,
    }


def _json_response():
    refresh_id = f"{time.time_ns()}"
    return JSONResponse(content=_machine_payload(), headers=_headers(refresh_id))


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
    refresh_id = f"{time.time_ns()}"
    body = json.dumps(_machine_payload(), separators=(",", ":"), ensure_ascii=False)
    return Response(content=body, media_type="text/plain", headers=_headers(refresh_id))


@main.app.get("/live.txt")
def live_txt():
    return data_txt()


@main.app.get("/health")
def health():
    with main.lock:
        return {"ok": True, "status": main.state["source_status"], "timestamp": main.now_ist().isoformat(), "candle_source": CANDLE_SOURCE, "synthetic_candles": False, "market_close": "15:30"}


def startup():
    with main.lock:
        main.state["collector_started"] = True
    threading.Thread(target=_checkpoint_worker, daemon=True, name="psy29-checkpoint").start()
    threading.Thread(target=_supervisor, daemon=True, name="psy29-supervisor").start()


main.app.add_event_handler("startup", startup)
app = main.app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(main.os.getenv("PORT", "10000")))
