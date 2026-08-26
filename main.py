import asyncio
import csv
import io
import json
import logging
import os
import struct
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pyotp
import requests
import websockets
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from psy29.normalized_api import normalize_market

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 15)

STOCKS = [
    "NESTLEIND", "VEDL", "ICICIPRULI", "KALYANKJIL", "KOTAKBANK",
    "BANDHANBNK", "BANKBARODA", "TITAN", "INFY", "DLF", "TCS",
    "MAXHEALTH", "KFINTECH", "PRESTIGE", "BHEL", "RBLBANK", "HCLTECH",
    "ICICIGI", "HDFCLIFE", "MARICO", "LUPIN", "COFORGE", "TECHM",
    "SWIGGY", "PERSISTENT", "OBEROIRLTY", "SUPREMEIND", "LAURUSLABS",
    "AMBUJACEM",
]

DHAN_AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"
DHAN_BASE = "https://api.dhan.co/v2"
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
WS_URL = "wss://api-feed.dhan.co"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("psy29")

app = FastAPI(title="PSY29 Live Data", version="1.0.0")
lock = threading.RLock()
state: dict[str, Any] = {
    "trading_date": None,
    "market_session_status": "UNKNOWN",
    "source_status": "STARTING",
    "last_update": None,
    "stocks": {},
    "security_map": {},
    "access_token_expiry": None,
    "collector_started": False,
}


def now_ist() -> datetime:
    return datetime.now(IST)


def session_status(dt: datetime) -> str:
    hm = (dt.hour, dt.minute)
    if hm < MARKET_OPEN:
        return "PRE_OPEN"
    if hm >= MARKET_CLOSE:
        return "POST_CLOSE"
    return "OPEN"


def is_weekday(dt: datetime) -> bool:
    return dt.weekday() < 5


def in_session(dt: datetime) -> bool:
    return is_weekday(dt) and MARKET_OPEN <= (dt.hour, dt.minute) < MARKET_CLOSE


def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def safe_int(v):
    try:
        return int(v)
    except Exception:
        return None


def dhan_headers(token: str, client_id: str | None = None):
    h = {"Accept": "application/json", "Content-Type": "application/json", "access-token": token}
    if client_id:
        h["client-id"] = client_id
    return h


def generate_access_token() -> tuple[str, str | None]:
    client_id = os.environ["DHAN_CLIENT_ID"]
    pin = os.environ["DHAN_PIN"]
    secret = os.environ["DHAN_TOTP_SECRET"]
    totp = pyotp.TOTP(secret).now()
    r = requests.post(
        DHAN_AUTH_URL,
        params={"dhanClientId": client_id, "pin": pin, "totp": totp},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    token = data.get("accessToken")
    if not token:
        raise RuntimeError(f"Dhan auth returned no accessToken: {data}")
    return token, data.get("expiryTime")


def load_security_map() -> dict[str, str]:
    r = requests.get(SCRIP_MASTER_URL, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    result = {}
    for row in reader:
        symbol = (row.get("SEM_TRADING_SYMBOL") or row.get("SM_SYMBOL_NAME") or row.get("SYMBOL_NAME") or "").strip()
        segment = (row.get("SEM_SEGMENT") or row.get("SEGMENT") or "").strip()
        exch = (row.get("SEM_EXM_EXCH_ID") or row.get("EXCH_ID") or "").strip()
        sid = (row.get("SEM_SMST_SECURITY_ID") or row.get("SECURITY_ID") or "").strip()
        instrument = (row.get("SEM_INSTRUMENT_NAME") or row.get("INSTRUMENT") or "").strip()
        if symbol and sid and exch == "NSE" and segment == "E" and instrument in {"EQUITY", ""}:
            result[symbol] = sid
    missing = [s for s in STOCKS if s not in result]
    if missing:
        raise RuntimeError(f"Missing NSE equity security IDs: {missing}")
    return {s: result[s] for s in STOCKS}


def parse_series_response(data: dict) -> list[dict]:
    keys = ["open", "high", "low", "close", "volume", "timestamp"]
    if not all(k in data for k in keys):
        return []
    rows = []
    n = min(len(data[k]) for k in keys)
    for i in range(n):
        ts = int(data["timestamp"][i])
        rows.append({
            "timestamp": datetime.fromtimestamp(ts, IST).isoformat(),
            "epoch": ts,
            "open": safe_float(data["open"][i]),
            "high": safe_float(data["high"][i]),
            "low": safe_float(data["low"][i]),
            "close": safe_float(data["close"][i]),
            "volume": safe_int(data["volume"][i]),
        })
    return rows


def fetch_intraday_1m(token: str, security_id: str, from_dt: datetime, to_dt: datetime) -> list[dict]:
    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": "1",
        "oi": False,
        "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }
    r = requests.post(f"{DHAN_BASE}/charts/intraday", headers=dhan_headers(token), json=payload, timeout=25)
    r.raise_for_status()
    return parse_series_response(r.json())


def fetch_previous_day(token: str, security_id: str, today: datetime) -> dict:
    start = (today - timedelta(days=10)).date().isoformat()
    end = today.date().isoformat()
    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "expiryCode": 0,
        "oi": False,
        "fromDate": start,
        "toDate": end,
    }
    r = requests.post(f"{DHAN_BASE}/charts/historical", headers=dhan_headers(token), json=payload, timeout=25)
    r.raise_for_status()
    rows = parse_series_response(r.json())
    if not rows:
        return {"high": None, "low": None, "close": None}
    last = rows[-1]
    return {"high": last["high"], "low": last["low"], "close": last["close"]}


def fetch_market_quote(token: str, client_id: str, security_map: dict[str, str]) -> dict[str, dict]:
    payload = {"NSE_EQ": [int(v) for v in security_map.values()]}
    r = requests.post(f"{DHAN_BASE}/marketfeed/quote", headers=dhan_headers(token, client_id), json=payload, timeout=20)
    r.raise_for_status()
    raw = r.json().get("data", {}).get("NSE_EQ", {})
    reverse = {str(v): k for k, v in security_map.items()}
    result = {}
    for sid, item in raw.items():
        symbol = reverse.get(str(sid))
        if not symbol:
            continue
        ohlc = item.get("ohlc", {}) or {}
        result[symbol] = {
            "current": safe_float(item.get("last_price")),
            "open": safe_float(ohlc.get("open")),
            "high": safe_float(ohlc.get("high")),
            "low": safe_float(ohlc.get("low")),
            "close": safe_float(ohlc.get("close")),
            "volume": safe_int(item.get("volume")),
        }
    return result


def aggregate(candles: list[dict], minutes: int) -> list[dict]:
    buckets: dict[int, dict] = {}
    for c in candles:
        dt = datetime.fromisoformat(c["timestamp"])
        minute = (dt.minute // minutes) * minutes
        bdt = dt.replace(minute=minute, second=0, microsecond=0)
        key = int(bdt.timestamp())
        if key not in buckets:
            buckets[key] = {
                "timestamp": bdt.isoformat(), "epoch": key,
                "open": c["open"], "high": c["high"], "low": c["low"],
                "close": c["close"], "volume": c["volume"] or 0,
            }
        else:
            b = buckets[key]
            b["high"] = max(b["high"], c["high"])
            b["low"] = min(b["low"], c["low"])
            b["close"] = c["close"]
            b["volume"] += c["volume"] or 0
    return [buckets[k] for k in sorted(buckets)]


def calc_vwap(candles: list[dict]) -> float | None:
    pv = 0.0
    vol = 0
    for c in candles:
        if c["high"] is None or c["low"] is None or c["close"] is None:
            continue
        typical = (c["high"] + c["low"] + c["close"]) / 3.0
        v = c["volume"] or 0
        pv += typical * v
        vol += v
    return pv / vol if vol else None


def calc_ema(candles: list[dict], period: int) -> float | None:
    closes = [c["close"] for c in candles if c["close"] is not None]
    if not closes:
        return None
    k = 2 / (period + 1)
    ema = closes[0]
    for close in closes[1:]:
        ema = close * k + ema * (1 - k)
    return ema


def opening_range(candles: list[dict]) -> dict:
    first = [c for c in candles if "09:15:00" <= datetime.fromisoformat(c["timestamp"]).strftime("%H:%M:%S") < "09:30:00"]
    if not first:
        return {"period": "09:15-09:30", "status": "NOT_FORMED", "high": None, "low": None}
    return {
        "period": "09:15-09:30",
        "status": "FORMED" if len(first) >= 15 else "FORMING",
        "high": max(c["high"] for c in first),
        "low": min(c["low"] for c in first),
    }


def structure(candles: list[dict]) -> dict:
    if len(candles) < 5:
        return {"trend": "INSUFFICIENT_DATA", "swing_high": None, "swing_low": None}
    highs = []
    lows = []
    for i in range(2, len(candles) - 2):
        h = candles[i]["high"]
        l = candles[i]["low"]
        if h > candles[i-1]["high"] and h > candles[i-2]["high"] and h >= candles[i+1]["high"] and h >= candles[i+2]["high"]:
            highs.append((candles[i]["timestamp"], h))
        if l < candles[i-1]["low"] and l <= candles[i-2]["low"] and l <= candles[i+1]["low"] and l <= candles[i+2]["low"]:
            lows.append((candles[i]["timestamp"], l))
    closes = [c["close"] for c in candles[-5:] if c["close"] is not None]
    trend = "FLAT"
    if len(closes) >= 3:
        if closes[-1] > closes[0]:
            trend = "UP"
        elif closes[-1] < closes[0]:
            trend = "DOWN"
    return {
        "trend": trend,
        "swing_high": highs[-1] if highs else None,
        "swing_low": lows[-1] if lows else None,
    }


def rebuild_stock(symbol: str, one_min: list[dict], quote: dict | None, prev: dict):
    one_min = sorted({c["epoch"]: c for c in one_min}.values(), key=lambda x: x["epoch"])
    five = aggregate(one_min, 5)
    fifteen = aggregate(one_min, 15)
    sixty = aggregate(one_min, 60)
    with lock:
        state["stocks"][symbol] = {
            "symbol": symbol,
            "security_id": state["security_map"][symbol],
            "current_price": quote.get("current") if quote else (one_min[-1]["close"] if one_min else None),
            "ohlc": {
                "open": quote.get("open") if quote else (one_min[0]["open"] if one_min else None),
                "high": quote.get("high") if quote else (max(c["high"] for c in one_min) if one_min else None),
                "low": quote.get("low") if quote else (min(c["low"] for c in one_min) if one_min else None),
                "close": quote.get("close") if quote else (one_min[-1]["close"] if one_min else None),
            },
            "session_high": quote.get("high") if quote else (max(c["high"] for c in one_min) if one_min else None),
            "session_low": quote.get("low") if quote else (min(c["low"] for c in one_min) if one_min else None),
            "previous_day": prev,
            "volume": quote.get("volume") if quote else (sum(c["volume"] or 0 for c in one_min) if one_min else None),
            "candles": {"1m": one_min, "5m": five, "15m": fifteen, "1h": sixty},
            "vwap": calc_vwap(one_min),
            "ema9": calc_ema(one_min, 9),
            "ema20": calc_ema(one_min, 20),
            "opening_range": opening_range(one_min),
            "structure": structure(five),
            "timestamp": now_ist().isoformat(),
            "trading_date": now_ist().date().isoformat(),
            "market_session_status": session_status(now_ist()),
            "data_source_status": "LIVE" if quote else "HISTORICAL_BACKFILL",
            "last_tick": None,
            "_one_min": one_min,
            "_volume_anchor": None,
        }


def update_tick(symbol: str, price: float, volume: int, ltt_epoch: int, day_open: float, day_high: float, day_low: float):
    dt = datetime.fromtimestamp(ltt_epoch, IST)
    with lock:
        s = state["stocks"].get(symbol)
        if not s:
            return
        s["current_price"] = price
        s["ohlc"]["open"] = day_open
        s["ohlc"]["high"] = day_high
        s["ohlc"]["low"] = day_low
        s["session_high"] = day_high
        s["session_low"] = day_low
        s["volume"] = volume
        s["timestamp"] = now_ist().isoformat()
        s["market_session_status"] = session_status(now_ist())
        s["data_source_status"] = "LIVE"
        s["last_tick"] = dt.isoformat()
        minute_dt = dt.replace(second=0, microsecond=0)
        key = int(minute_dt.timestamp())
        candles = s["_one_min"]
        if candles and candles[-1]["epoch"] == key:
            c = candles[-1]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            if s["_volume_anchor"] is None:
                s["_volume_anchor"] = volume
            c["volume"] = max(0, volume - s["_volume_anchor"])
        else:
            prev_close = candles[-1]["close"] if candles else price
            s["_volume_anchor"] = volume
            candles.append({"timestamp": minute_dt.isoformat(), "epoch": key, "open": prev_close, "high": price, "low": price, "close": price, "volume": 0})
        s["candles"]["1m"] = candles
        s["candles"]["5m"] = aggregate(candles, 5)
        s["candles"]["15m"] = aggregate(candles, 15)
        s["candles"]["1h"] = aggregate(candles, 60)
        s["vwap"] = calc_vwap(candles)
        s["ema9"] = calc_ema(candles, 9)
        s["ema20"] = calc_ema(candles, 20)
        s["opening_range"] = opening_range(candles)
        s["structure"] = structure(s["candles"]["5m"])
        state["last_update"] = now_ist().isoformat()


def clean_stock(s: dict) -> dict:
    return {k: v for k, v in s.items() if not k.startswith("_")}


def initialize_session(token: str):
    today = now_ist()
    with lock:
        state["trading_date"] = today.date().isoformat()
        state["market_session_status"] = session_status(today)
        state["source_status"] = "BACKFILLING"
    security_map = load_security_map()
    with lock:
        state["security_map"] = security_map
    client_id = os.environ["DHAN_CLIENT_ID"]
    quote_map = fetch_market_quote(token, client_id, security_map) if in_session(today) else {}
    for symbol in STOCKS:
        try:
            from_dt = today.replace(hour=9, minute=15, second=0, microsecond=0)
            one_min = fetch_intraday_1m(token, security_map[symbol], from_dt, today) if in_session(today) else []
            prev = fetch_previous_day(token, security_map[symbol], today)
            rebuild_stock(symbol, one_min, quote_map.get(symbol), prev)
            time.sleep(0.22)
        except Exception as exc:
            log.exception("Initialization failed for %s: %s", symbol, exc)
            with lock:
                state["stocks"][symbol] = {
                    "symbol": symbol,
                    "security_id": security_map[symbol],
                    "data_source_status": "ERROR",
                    "error": str(exc),
                    "timestamp": now_ist().isoformat(),
                    "trading_date": today.date().isoformat(),
                    "market_session_status": session_status(today),
                }
    with lock:
        state["source_status"] = "READY"
        state["last_update"] = now_ist().isoformat()


def parse_quote_packet(data: bytes):
    if len(data) < 51 or data[0] != 4:
        return None
    security_id = struct.unpack_from("<I", data, 4)[0]
    ltp = struct.unpack_from("<f", data, 9)[0]
    ltt = struct.unpack_from("<I", data, 15)[0]
    volume = struct.unpack_from("<I", data, 23)[0]
    day_open = struct.unpack_from("<f", data, 35)[0]
    day_high = struct.unpack_from("<f", data, 43)[0]
    day_low = struct.unpack_from("<f", data, 47)[0]
    return security_id, ltp, volume, ltt, day_open, day_high, day_low


async def websocket_loop(token: str):
    client_id = os.environ["DHAN_CLIENT_ID"]
    reverse = {int(v): k for k, v in state["security_map"].items()}
    url = f"{WS_URL}?version=2&token={token}&clientId={client_id}&authType=2"
    while in_session(now_ist()):
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=None) as ws:
                instruments = [{"ExchangeSegment": "NSE_EQ", "SecurityId": sid} for sid in state["security_map"].values()]
                for start in range(0, len(instruments), 100):
                    batch = instruments[start:start+100]
                    await ws.send(json.dumps({"RequestCode": 17, "InstrumentCount": len(batch), "InstrumentList": batch}))
                with lock:
                    state["source_status"] = "LIVE"
                while in_session(now_ist()):
                    message = await asyncio.wait_for(ws.recv(), timeout=35)
                    if isinstance(message, str):
                        continue
                    parsed = parse_quote_packet(message)
                    if parsed:
                        sid, ltp, volume, ltt, day_open, day_high, day_low = parsed
                        symbol = reverse.get(sid)
                        if symbol:
                            update_tick(symbol, ltp, volume, ltt, day_open, day_high, day_low)
        except Exception as exc:
            log.warning("Dhan websocket disconnected: %s", exc)
            with lock:
                state["source_status"] = "RECONNECTING"
            if not in_session(now_ist()):
                break
            await asyncio.sleep(3)


def collector_thread():
    if state["collector_started"]:
        return
    state["collector_started"] = True
    while True:
        try:
            now = now_ist()
            if in_session(now):
                token, expiry = generate_access_token()
                with lock:
                    state["access_token_expiry"] = expiry
                initialize_session(token)
                asyncio.run(websocket_loop(token))
                with lock:
                    state["source_status"] = "POST_CLOSE" if not in_session(now_ist()) else "DISCONNECTED"
                break
            time.sleep(30)
        except Exception as exc:
            log.exception("Collector failure: %s", exc)
            with lock:
                state["source_status"] = "ERROR"
            time.sleep(30)


@app.on_event("startup")
def startup():
    threading.Thread(target=collector_thread, daemon=True, name="psy29-collector").start()


@app.get("/")
def root():
    with lock:
        return {
            "service": "PSY29 Live Data",
            "status": state["source_status"],
            "trading_date": state["trading_date"],
            "market_session_status": state["market_session_status"],
            "stocks_expected": len(STOCKS),
            "stocks_loaded": len(state["stocks"]),
            "last_update": state["last_update"],
            "postgres": False,
            "storage_mode": "in-memory intraday session only",
        }


@app.get("/health")
def health():
    with lock:
        return {"ok": True, "status": state["source_status"], "timestamp": now_ist().isoformat()}


@app.get("/data")
def data():
    with lock:
        raw = {
            "service": "PSY29 Live Data",
            "timestamp": now_ist().isoformat(),
            "trading_date": state["trading_date"],
            "market_session_status": state["market_session_status"],
            "data_source_status": state["source_status"],
            "stocks_expected": 29,
            "stocks": {k: clean_stock(v) for k, v in state["stocks"].items()},
        }
    payload = normalize_market(raw)
    return JSONResponse(payload)


@app.get("/data/{symbol}")
def stock_data(symbol: str):
    symbol = symbol.upper()
    with lock:
        if symbol not in state["stocks"]:
            return JSONResponse({"error": "unknown_or_not_loaded", "symbol": symbol}, status_code=404)
        return JSONResponse(clean_stock(state["stocks"][symbol]))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")))