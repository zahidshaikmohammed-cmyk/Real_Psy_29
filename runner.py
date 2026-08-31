from __future__ import annotations

import asyncio
import json
import math
import os
import struct
import threading
import time
from datetime import datetime, time as dtime

import main
from fastapi import Response
from fastapi.responses import JSONResponse
from psy29.data_integrity import DataIntegrityError
from psy29.intraday_store import IntradayStore
from psy29.session_reset import reset_for_trading_date

# One canonical session definition everywhere in the production runner.
main.MARKET_OPEN = (9, 15)
main.MARKET_CLOSE = (15, 30)

FIRST_COMPLETED_MINUTE = 9 * 60 + 16
CANDLE_SOURCE = "DHAN_WEBSOCKET_REAL_TICKS_1M"
AUTH_COOLDOWN_SECONDS = 125
CHECKPOINT_SECONDS = 60
MAX_TICK_AGE_SECONDS = 300

# Dhan V2 Quote packet: response header + LTP + LTQ + LTT + ATP + cumulative
# volume + market quantities + day OHLC. All fields are real feed data.
QUOTE_FORMAT = "<BHBIfHIfIIIffff"
QUOTE_SIZE = struct.calcsize(QUOTE_FORMAT)

main.app.router.on_startup.clear()
store = IntradayStore()


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


def _normalize_epoch(value: int) -> int:
    value = int(value)
    magnitude = abs(value)
    if magnitude >= 10**18:
        value //= 10**9
    elif magnitude >= 10**15:
        value //= 10**6
    elif magnitude >= 10**12:
        value //= 10**3
    return value


def _parse_quote_packets(message: bytes):
    """Yield every Dhan packet in a websocket frame; never assume one frame=one packet."""
    if not isinstance(message, (bytes, bytearray, memoryview)):
        return
    buf = bytes(message)
    offset = 0
    while offset + 8 <= len(buf):
        msg_len = int.from_bytes(buf[offset + 1:offset + 3], "little", signed=False)
        if msg_len <= 0:
            break
        if offset + msg_len > len(buf):
            break
        packet = buf[offset:offset + msg_len]
        if len(packet) >= QUOTE_SIZE and packet[0] == 4:
            try:
                values = struct.unpack(QUOTE_FORMAT, packet[:QUOTE_SIZE])
                code, msglen, segment, sid, ltp, ltq, ltt, atp, volume, sell, buy, day_open, day_close, day_high, day_low = values
                if code == 4 and segment == 1 and msglen >= QUOTE_SIZE:
                    yield {
                        "security_id": int(sid),
                        "ltp": float(ltp),
                        "ltq": int(ltq),
                        "ltt": _normalize_epoch(ltt),
                        "volume": int(volume),
                        "open": float(day_open),
                        "high": float(day_high),
                        "low": float(day_low),
                    }
            except (struct.error, ValueError, OverflowError):
                pass
        offset += msg_len


def _finite_price(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0 or value > 10_000_000:
        raise DataIntegrityError("non-finite/out-of-range equity price")
    return value


def _valid_tick(tick: dict, now):
    price = _finite_price(tick["ltp"])
    volume = int(tick["volume"])
    ltq = max(0, int(tick["ltq"]))
    if volume < 0:
        raise DataIntegrityError("invalid cumulative volume")
    epoch = int(tick["ltt"])
    dt = datetime.fromtimestamp(epoch, main.IST)
    if dt.date() != now.date():
        raise DataIntegrityError("tick timestamp outside trading date")
    if not main.MARKET_OPEN <= (dt.hour, dt.minute) < main.MARKET_CLOSE:
        raise DataIntegrityError("tick timestamp outside NSE session")
    age = (now - dt).total_seconds()
    if age < -60:
        raise DataIntegrityError("future live tick")
    if age > MAX_TICK_AGE_SECONDS:
        raise DataIntegrityError("stale live tick")
    return price, volume, ltq, dt


def _seed_state(token: str, security_map: dict[str, str]):
    now = main.now_ist()
    client_id = os.environ["DHAN_CLIENT_ID"]
    quotes = main.fetch_market_quote(token, client_id, security_map)

    with main.lock:
        main.state["trading_date"] = now.date().isoformat()
        main.state["market_session_status"] = "OPEN"
        main.state["security_map"] = security_map
        main.state["stocks"] = {}
        main.state["source_status"] = "CONNECTING"

    for symbol in main.STOCKS:
        q = quotes.get(symbol) or {}
        try:
            prev = main.fetch_previous_day(token, security_map[symbol], now)
            if not isinstance(prev, dict):
                prev = {"high": None, "low": None, "close": None}
        except Exception as exc:
            main.log.warning("Previous-day reference unavailable for %s: %s", symbol, exc)
            prev = {"high": None, "low": None, "close": None}

        with main.lock:
            main.state["stocks"][symbol] = {
                "symbol": symbol,
                "security_id": security_map[symbol],
                "current_price": q.get("current"),
                "ohlc": {"open": q.get("open"), "high": q.get("high"), "low": q.get("low"), "close": q.get("current")},
                "session_high": q.get("high"),
                "session_low": q.get("low"),
                "previous_day": prev,
                "volume": q.get("volume"),
                "candles": {"1m": [], "5m": [], "15m": [], "1h": []},
                "vwap": None,
                "ema9": None,
                "ema20": None,
                "opening_range": {"period": "09:15-09:30", "status": "NOT_FORMED", "high": None, "low": None},
                "structure": {"trend": "INSUFFICIENT_DATA", "swing_high": None, "swing_low": None},
                "timestamp": now.isoformat(),
                "trading_date": now.date().isoformat(),
                "market_session_status": "OPEN",
                "data_source_status": "LIVE",
                "last_tick": None,
                "completed_candle_count": 0,
                "last_completed_candle": None,
                "candle_source": CANDLE_SOURCE,
                "_one_min": [],
                "_builder": None,
                "_last_cumulative_volume": None,
            }

    main.log.info("Seeded live state: %d/%d stocks", len(main.state["stocks"]), len(main.STOCKS))


def _commit_bar(symbol: str, bar: dict):
    if not bar:
        return
    # Only completed bars enter the canonical candle history.
    rows = []
    with main.lock:
        stock = main.state["stocks"].get(symbol)
        if not stock:
            return
        rows = list(stock.get("_one_min", []))
    if rows and int(rows[-1]["epoch"]) == int(bar["epoch"]):
        return
    rows.append(dict(bar))
    rows.sort(key=lambda x: int(x["epoch"]))

    with main.lock:
        stock = main.state["stocks"].get(symbol)
        if not stock:
            return
        quote = {
            "current": stock.get("current_price"),
            "open": rows[0]["open"],
            "high": max(r["high"] for r in rows),
            "low": min(r["low"] for r in rows),
            "close": rows[-1]["close"],
            "volume": stock.get("volume"),
        }
        prev = stock.get("previous_day") or {"high": None, "low": None, "close": None}

    main.rebuild_stock(symbol, rows, quote, prev)
    with main.lock:
        stock = main.state["stocks"].get(symbol)
        if not stock:
            return
        stock["candle_source"] = CANDLE_SOURCE
        stock["completed_candle_count"] = len(rows)
        stock["last_completed_candle"] = rows[-1]["timestamp"]
        stock["_one_min"] = rows
        stock["_builder"] = None
        stock["data_source_status"] = "LIVE"
        stock["last_tick"] = bar["timestamp"]
        main.state["last_update"] = main.now_ist().isoformat()


def _ingest_tick(symbol: str, tick: dict):
    now = main.now_ist()
    price, cumulative_volume, ltq, dt = _valid_tick(tick, now)
    minute_dt = dt.replace(second=0, microsecond=0)
    epoch = int(minute_dt.timestamp())

    with main.lock:
        stock = main.state["stocks"].get(symbol)
        if not stock:
            return
        builder = stock.get("_builder")
        if builder and epoch < int(builder["epoch"]):
            raise DataIntegrityError("out-of-order live tick")

        if builder and epoch > int(builder["epoch"]):
            finished = dict(builder)
            # The previous minute is now definitely closed because the exchange
            # timestamp has entered a later minute. Commit only that real bar.
            _commit_bar(symbol, finished)
            builder = None

        if builder is None:
            builder = {
                "timestamp": minute_dt.isoformat(),
                "epoch": epoch,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
            }
            stock["_builder"] = builder

        builder["high"] = max(float(builder["high"]), price)
        builder["low"] = min(float(builder["low"]), price)
        builder["close"] = price
        # LTQ is the quantity of the real trade represented by this quote event.
        builder["volume"] = int(builder.get("volume", 0)) + ltq

        stock["current_price"] = price
        stock["volume"] = cumulative_volume
        stock["timestamp"] = now.isoformat()
        stock["market_session_status"] = main.session_status(now)
        stock["data_source_status"] = "LIVE"
        stock["last_tick"] = dt.isoformat()
        stock["ohlc"]["close"] = price
        stock["ohlc"]["high"] = max(stock["ohlc"].get("high") or price, price)
        stock["ohlc"]["low"] = min(stock["ohlc"].get("low") or price, price)
        stock["session_high"] = stock["ohlc"]["high"]
        stock["session_low"] = stock["ohlc"]["low"]
        main.state["last_update"] = now.isoformat()


def _finalize_closed_builders():
    now = main.now_ist()
    current_epoch = int(now.replace(second=0, microsecond=0).timestamp())
    for symbol in main.STOCKS:
        with main.lock:
            stock = main.state["stocks"].get(symbol)
            builder = dict(stock.get("_builder")) if stock and stock.get("_builder") else None
        if builder and int(builder["epoch"]) < current_epoch:
            _commit_bar(symbol, builder)


def _websocket_loop(token: str):
    client_id = os.environ["DHAN_CLIENT_ID"]
    reverse = {int(v): k for k, v in main.state["security_map"].items()}
    url = f"{main.WS_URL}?version=2&token={token}&clientId={client_id}&authType=2"
    delay = 3.0

    while main.in_session(main.now_ist()):
        try:
            async def session():
                nonlocal delay
                async with main.websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=None) as ws:
                    instruments = [{"ExchangeSegment": "NSE_EQ", "SecurityId": sid} for sid in main.state["security_map"].values()]
                    for start in range(0, len(instruments), 100):
                        batch = instruments[start:start + 100]
                        await ws.send(json.dumps({"RequestCode": 17, "InstrumentCount": len(batch), "InstrumentList": batch}))
                    with main.lock:
                        main.state["source_status"] = "LIVE"
                    main.log.info("Dhan real-time websocket connected: %d instruments", len(instruments))
                    delay = 3.0

                    while main.in_session(main.now_ist()):
                        message = await asyncio.wait_for(ws.recv(), timeout=35)
                        for tick in _parse_quote_packets(message):
                            symbol = reverse.get(int(tick["security_id"]))
                            if not symbol:
                                continue
                            try:
                                _ingest_tick(symbol, tick)
                            except DataIntegrityError as exc:
                                main.log.warning("Rejected live tick for %s: %s", symbol, exc)
                        _finalize_closed_builders()

            asyncio.run(session())
        except Exception as exc:
            with main.lock:
                main.state["source_status"] = "RECONNECTING"
            if not main.in_session(main.now_ist()):
                break
            main.log.warning("Dhan websocket disconnected: %s; retry in %.1fs", exc, delay)
            time.sleep(delay)
            delay = min(30.0, delay * 2.0)


def _checkpoint_worker():
    while True:
        try:
            if main.in_session(main.now_ist()):
                with main.lock:
                    date_value = main.state.get("trading_date")
                    stocks = {k: main.clean_stock(v) for k, v in main.state.get("stocks", {}).items()}
                if date_value and stocks:
                    try:
                        store.save_market(date_value, stocks)
                    except Exception as exc:
                        main.log.warning("Intraday checkpoint failed: %s", exc)
                time.sleep(CHECKPOINT_SECONDS)
            else:
                time.sleep(10)
        except Exception as exc:
            main.log.warning("Checkpoint worker error: %s", exc)
            time.sleep(CHECKPOINT_SECONDS)


def _supervisor():
    while True:
        try:
            now = main.now_ist()
            changed = False
            with main.lock:
                changed = reset_for_trading_date(main.state, now.date())
                main.state["market_session_status"] = main.session_status(now)
                if not main.in_session(now):
                    main.state["source_status"] = "WAITING_FOR_SESSION"
                    main.state["stocks"] = {}

            if not main.in_session(now):
                time.sleep(2)
                continue

            # Only one live session bootstrap per process/trading date.
            with main.lock:
                ready = main.state.get("source_status") in {"CONNECTING", "LIVE", "RECONNECTING"} and len(main.state.get("stocks", {})) == len(main.STOCKS)
            if not ready:
                with main.lock:
                    main.state["source_status"] = "CONNECTING"
                try:
                    token, expiry = main.generate_access_token()
                    security_map = main.load_security_map()
                    with main.lock:
                        main.state["access_token_expiry"] = expiry
                    _seed_state(token, security_map)
                    _websocket_loop(token)
                except Exception as exc:
                    if "Token can be generated once every 2 minutes" in str(exc):
                        with main.lock:
                            main.state["source_status"] = "AUTH_COOLDOWN"
                        time.sleep(AUTH_COOLDOWN_SECONDS)
                    else:
                        main.log.exception("Live collector failure: %s", exc)
                        with main.lock:
                            main.state["source_status"] = "ERROR"
                        time.sleep(5)
            else:
                time.sleep(1)
        except Exception as exc:
            main.log.exception("Supervisor failure: %s", exc)
            with main.lock:
                main.state["source_status"] = "ERROR"
            time.sleep(5)


def _machine_payload():
    now = main.now_ist()
    with main.lock:
        reset_for_trading_date(main.state, now.date())
        in_session = main.in_session(now)
        source = main.state.get("source_status") or "WAITING_FOR_SESSION"
        if not in_session:
            source = "WAITING_FOR_SESSION" if (now.hour, now.minute) < main.MARKET_OPEN else "POST_CLOSE"
        stocks = {k: main.clean_stock(v) for k, v in main.state.get("stocks", {}).items()}
        raw = {
            "service": "PSY29 Live Data",
            "timestamp": now.isoformat(),
            "trading_date": now.date().isoformat(),
            "market_session_status": main.session_status(now),
            "data_source_status": source,
            "stocks_expected": len(main.STOCKS),
            "stocks": stocks,
            "candle_policy": {
                "source": CANDLE_SOURCE,
                "first_completed_minute": "09:16",
                "regular_session": "09:15-15:30 IST",
                "synthetic_candles": False,
                "rule": "Only completed bars built from real Dhan websocket trade events are published.",
            },
        }

    if not in_session:
        raw["diagnostic"] = {
            "status": "OK",
            "error_code": None,
            "error_message": None,
            "stage": "SESSION",
            "affected_stocks": [],
            "recovery_action": "WAIT_FOR_SESSION" if raw["market_session_status"] == "PRE_OPEN" else None,
            "data_safe": True,
        }
        return main.normalize_market(raw)

    missing_live = [s for s in main.STOCKS if s not in stocks or stocks[s].get("current_price") is None]
    if missing_live:
        raw["diagnostic"] = {
            "status": "RECOVERING",
            "error_code": "LIVE_STOCKS_PENDING",
            "error_message": "Waiting for live Dhan feed state for: " + ",".join(missing_live),
            "stage": "WEBSOCKET",
            "affected_stocks": missing_live,
            "recovery_action": "WAIT_FOR_LIVE_FEED",
            "data_safe": False,
        }
    elif raw["data_source_status"] in {"LIVE", "CONNECTING", "RECONNECTING"}:
        raw["diagnostic"] = {"status": "OK" if raw["data_source_status"] == "LIVE" else "RECOVERING", "data_safe": raw["data_source_status"] == "LIVE", "last_good_tick": raw["timestamp"]}
    else:
        raw["diagnostic"] = {
            "status": "ERROR",
            "error_code": "COLLECTOR_FAILURE",
            "error_message": f"Live data source status is {raw['data_source_status']}",
            "stage": "FEED",
            "affected_stocks": [],
            "recovery_action": "AUTOMATIC_RECONNECT",
            "data_safe": False,
        }
    return main.normalize_market(raw)


def _json_response():
    return JSONResponse(content=_machine_payload(), headers=_headers(str(time.time_ns())))


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
    return Response(content=body, media_type="text/plain", headers=_headers(str(time.time_ns())))


@main.app.get("/live.txt")
def live_txt():
    return data_txt()


def _safe_root():
    now = main.now_ist()
    with main.lock:
        stocks_loaded = len(main.state.get("stocks", {}))
        source = main.state.get("source_status") or "WAITING_FOR_SESSION"
    if (now.hour, now.minute) < main.MARKET_OPEN:
        source = "WAITING_FOR_SESSION"
    return {
        "service": "PSY29 Live Data",
        "status": source,
        "trading_date": now.date().isoformat(),
        "market_session_status": main.session_status(now),
        "stocks_expected": len(main.STOCKS),
        "stocks_loaded": stocks_loaded,
        "last_update": main.state.get("last_update"),
        "postgres": False,
        "storage_mode": "in-memory intraday session only",
    }


# Replace the old root route rather than leaving a stale previous-day ERROR visible.
main.app.add_api_route("/", _safe_root, methods=["GET"], include_in_schema=False)
main.app.router.routes.insert(0, main.app.router.routes.pop())


@main.app.get("/health")
def health():
    now = main.now_ist()
    with main.lock:
        source = main.state.get("source_status") or "WAITING_FOR_SESSION"
    if (now.hour, now.minute) < main.MARKET_OPEN:
        source = "WAITING_FOR_SESSION"
    return {
        "ok": True,
        "status": source,
        "timestamp": now.isoformat(),
        "candle_source": CANDLE_SOURCE,
        "synthetic_candles": False,
        "market_close": "15:30",
    }


# Replace the old /health route too.
main.app.router.routes.insert(0, main.app.router.routes.pop())


def startup():
    with main.lock:
        main.state["collector_started"] = True
        main.state["source_status"] = "WAITING_FOR_SESSION"
        main.state["market_session_status"] = main.session_status(main.now_ist())
    threading.Thread(target=_checkpoint_worker, daemon=True, name="psy29-checkpoint").start()
    threading.Thread(target=_supervisor, daemon=True, name="psy29-supervisor").start()


main.app.add_event_handler("startup", startup)
app = main.app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(main.os.getenv("PORT", "10000")))
