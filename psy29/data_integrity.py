from __future__ import annotations
import asyncio, json, logging, math, os, struct
from datetime import datetime, time

IST_OPEN=time(9,15); IST_CLOSE=time(15,15)
MIN_REASONABLE_EQUITY_PRICE=0.01; MAX_REASONABLE_EQUITY_PRICE=10_000_000.0
MAX_FUTURE_TICK_SKEW_SECONDS=60; MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS=86400
DHAN_QUOTE_PACKET_FORMAT="<BHBIfHIfIIIffff"; DHAN_QUOTE_PACKET_SIZE=struct.calcsize(DHAN_QUOTE_PACKET_FORMAT)

class DataIntegrityError(ValueError):
    """Raised when market data cannot be trusted for PSY29 decisions."""

def _finite_positive(value):
    try: number=float(value)
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("non-numeric price") from exc
    if not math.isfinite(number) or not MIN_REASONABLE_EQUITY_PRICE<=number<=MAX_REASONABLE_EQUITY_PRICE: raise DataIntegrityError("non-finite/out-of-range equity price")
    return number

def _normalize_epoch_seconds(value):
    try: raw=int(value)
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid tick timestamp") from exc
    if raw<=0: raise DataIntegrityError("invalid tick timestamp")
    m=abs(raw)
    if m>=10**18: raw//=10**9
    elif m>=10**15: raw//=10**6
    elif m>=10**12: raw//=10**3
    return raw

def parse_dhan_quote_packet(data):
    if not isinstance(data,(bytes,bytearray,memoryview)) or len(data)<DHAN_QUOTE_PACKET_SIZE: return None
    try:
        code,msglen,segment,sid,ltp,ltq,ltt,atp,volume,sell,buy,opn,close,high,low=struct.unpack(DHAN_QUOTE_PACKET_FORMAT,bytes(data[:DHAN_QUOTE_PACKET_SIZE]))
    except struct.error: return None
    if code!=4 or segment!=1 or msglen<DHAN_QUOTE_PACKET_SIZE or msglen>len(data): return None
    prices=(ltp,atp,opn,close,high,low)
    if not all(math.isfinite(x) for x in prices) or ltp<=0 or atp<0 or opn<=0 or high<=0 or low<=0: return None
    if volume<=0 or ltq<0 or sell<0 or buy<0: return None
    return sid,ltp,volume,ltt,opn,high,low

def validate_ohlcv_row(row,trading_date,*,session_only=True,allow_zero_volume=False):
    try:
        ts=datetime.fromisoformat(str(row["timestamp"])); epoch=int(row["epoch"])
        vals=[_finite_positive(row[k]) for k in ("open","high","low","close")]; volume=int(row.get("volume",0))
    except (KeyError,TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("malformed candle") from exc
    if ts.tzinfo is None or ts.date().isoformat()!=trading_date: raise DataIntegrityError("candle timestamp outside trading date")
    if session_only and not IST_OPEN<=ts.timetz().replace(tzinfo=None)<IST_CLOSE: raise DataIntegrityError("candle timestamp outside NSE session")
    if ts>datetime.now(ts.tzinfo): raise DataIntegrityError("future candle")
    if ts.second or ts.microsecond: raise DataIntegrityError("candle timestamp is not minute-aligned")
    if abs(ts.timestamp()-epoch)>1: raise DataIntegrityError("candle epoch does not match timestamp")
    if volume<=0 and not allow_zero_volume: raise DataIntegrityError("zero/negative candle volume")
    opn,high,low,close=vals
    if high<max(opn,close) or low>min(opn,close) or high<low: raise DataIntegrityError("invalid OHLC bounds")
    return dict(row)

def validate_intraday_rows(rows,trading_date):
    if not isinstance(rows,list) or not rows: raise DataIntegrityError("empty intraday history")
    valid=[]; prev=None
    for i,row in enumerate(rows):
        if not isinstance(row,dict): raise DataIntegrityError(f"malformed candle at index {i}")
        clean=validate_ohlcv_row(row,trading_date,allow_zero_volume=True); epoch=int(clean["epoch"])
        if prev is not None and epoch<=prev: raise DataIntegrityError("duplicate candle" if epoch==prev else "non-chronological candle")
        prev=epoch
        if int(clean.get("volume",0))<=0:
            logging.getLogger("psy29.data_integrity").warning("Quarantined intraday candle index=%d timestamp=%s reason=zero/negative candle volume",i,clean.get("timestamp")); continue
        valid.append(clean)
    if len(valid)<15: raise DataIntegrityError(f"insufficient valid intraday history: {len(valid)} rows")
    return valid

def validate_live_quote(quote):
    if not isinstance(quote,dict): raise DataIntegrityError("quote is not an object")
    current=_finite_positive(quote.get("current"))
    try: volume=int(quote.get("volume"))
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid quote volume") from exc
    if volume<=0: raise DataIntegrityError("zero/negative quote volume")
    return {"current":current,"volume":volume}

def validate_quote(quote):
    if not isinstance(quote,dict): raise DataIntegrityError("quote is not an object")
    if any(k not in quote or quote[k] is None for k in ("current","open","high","low")): raise DataIntegrityError("quote missing required market fields")
    current=_finite_positive(quote["current"]); opn=_finite_positive(quote["open"]); high=_finite_positive(quote["high"]); low=_finite_positive(quote["low"])
    close=quote.get("close"); close=_finite_positive(close) if close is not None else None
    try: volume=int(quote.get("volume"))
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid quote OHLC/volume") from exc
    if volume<=0: raise DataIntegrityError("zero/negative quote volume")
    if high<max(opn,current) or low>min(opn,current) or high<low: raise DataIntegrityError("invalid quote OHLC bounds")
    if close is not None and (high<close or low>close): raise DataIntegrityError("invalid quote OHLC bounds")
    return {"current":current,"open":opn,"high":high,"low":low,"close":close,"volume":volume}

def _validated_tick(ltp,volume,ltt_epoch,now,previous_volume=None,max_exchange_age_seconds=300):
    price=_finite_positive(ltp)
    try: vol=int(volume)
    except (TypeError,ValueError,OverflowError) as exc: raise DataIntegrityError("invalid tick volume") from exc
    if vol<=0: raise DataIntegrityError("zero/negative tick volume")
    ltt=_normalize_epoch_seconds(ltt_epoch)
    try: embedded=datetime.fromtimestamp(ltt,now.tzinfo)
    except (OverflowError,OSError,ValueError) as exc: raise DataIntegrityError("invalid tick timestamp") from exc
    future=(embedded-now).total_seconds(); effective=ltt
    if future>MAX_FUTURE_TICK_SKEW_SECONDS:
        if future>MAX_ACCEPTABLE_CLOCK_SKEW_SECONDS: raise DataIntegrityError("future live tick")
        logging.getLogger("psy29.data_integrity").warning("Accepted Dhan exchange-clock skew: %.1fs; using receipt time for candle placement",future); effective=int(now.timestamp()); embedded=now
    elif future>5: logging.getLogger("psy29.data_integrity").warning("Accepted exchange-clock skew on live tick: %.1fs",future)
    if embedded.date()!=now.date() or not IST_OPEN<=embedded.timetz().replace(tzinfo=None)<IST_CLOSE:
        logging.getLogger("psy29.data_integrity").warning("Dhan LTT outside current session; using live receipt time")
        effective=int(now.timestamp()); embedded=now
    if (now-embedded).total_seconds()>max_exchange_age_seconds: raise DataIntegrityError("stale live tick")
    if not IST_OPEN<=now.timetz().replace(tzinfo=None)<IST_CLOSE: raise DataIntegrityError("tick received outside current NSE session")
    if previous_volume is not None and vol<int(previous_volume): raise DataIntegrityError("live cumulative volume moved backwards")
    return price,vol,effective

def validate_live_tick(ltp,volume,ltt_epoch,now,previous_volume=None,*,max_exchange_age_seconds=300): return _validated_tick(ltp,volume,ltt_epoch,now,previous_volume,max_exchange_age_seconds)
def validate_tick(ltp,volume,ltt_epoch,day_open,day_high,day_low,now,previous_volume=None):
    price,vol,ltt=_validated_tick(ltp,volume,ltt_epoch,now,previous_volume); opn=_finite_positive(day_open); high=_finite_positive(day_high); low=_finite_positive(day_low)
    if high<low or high<opn or low>opn or not low<=price<=high: raise DataIntegrityError("invalid live tick market values")
    return price,vol,ltt,opn,high,low

# runner.py imports this module after main.py is fully loaded. Replace the
# original reconnect loop here, before runner starts its supervisor.
def _install_resilient_websocket():
    main=sys.modules.get("main")
    if main is None or not hasattr(main,"websocket_loop") or getattr(main,"_psy29_resilient_ws",False): return
    async def resilient(token):
        client_id=os.environ["DHAN_CLIENT_ID"]; reverse={int(v):k for k,v in main.state["security_map"].items()}
        url=f"{main.WS_URL}?version=2&token={token}&clientId={client_id}&authType=2"; delay=5.0
        while main.in_session(main.now_ist()):
            try:
                async with main.websockets.connect(url,ping_interval=20,ping_timeout=20,close_timeout=5,max_size=None) as ws:
                    instruments=[{"ExchangeSegment":"NSE_EQ","SecurityId":sid} for sid in main.state["security_map"].values()]
                    for i in range(0,len(instruments),100):
                        b=instruments[i:i+100]; await ws.send(json.dumps({"RequestCode":17,"InstrumentCount":len(b),"InstrumentList":b}))
                    with main.lock: main.state["source_status"]="LIVE"
                    main.log.info("Canonical Dhan websocket connected: %d instruments",len(instruments)); delay=5.0
                    while main.in_session(main.now_ist()):
                        msg=await asyncio.wait_for(ws.recv(),timeout=35)
                        if isinstance(msg,str) or not msg: continue
                        if msg[0]==50:
                            reason=struct.unpack_from("<h",msg,8)[0] if len(msg)>=10 else None; raise RuntimeError(f"Dhan feed disconnect packet reason={reason}")
                        parsed=parse_dhan_quote_packet(msg)
                        if not parsed: continue
                        sid,ltp,volume,ltt,*_=parsed; symbol=reverse.get(int(sid))
                        if not symbol: continue
                        try:
                            with main.lock:
                                stock=main.state["stocks"].get(symbol); previous=stock.get("volume") if stock else None; last=stock.get("_one_min",[])[-1]["epoch"] if stock and stock.get("_one_min") else None
                            price,vol,tick=validate_live_tick(ltp,volume,ltt,main.now_ist(),previous)
                            if last is not None and tick<int(last): raise DataIntegrityError("out-of-order live tick")
                            updater=getattr(main,"_original_update_tick",main.update_tick); updater(symbol,price,vol,tick,price,price,price)
                            with main.lock:
                                stock=main.state["stocks"].get(symbol); candles=stock.get("_one_min",[]) if stock else []
                                if not candles: raise DataIntegrityError("live tick produced no 1m candle")
                                o={"open":candles[0]["open"],"high":max(c["high"] for c in candles),"low":min(c["low"] for c in candles),"close":candles[-1]["close"]}
                                stock["ohlc"]=o; stock["session_high"]=o["high"]; stock["session_low"]=o["low"]; stock["current_price"]=price; stock["volume"]=vol; stock["last_tick"]=main.datetime.fromtimestamp(tick,main.IST).isoformat(); stock["data_source_status"]="LIVE"; main.state["last_update"]=main.now_ist().isoformat()
                        except DataIntegrityError as exc: main.log.warning("Rejected corrupt live tick for %s: %s",symbol,exc)
            except Exception as exc:
                with main.lock: main.state["source_status"]="RECONNECTING"
                if not main.in_session(main.now_ist()): break
                main.log.warning("Dhan websocket disconnected: %s; retry in %.1fs",exc,delay); await asyncio.sleep(delay); delay=min(60.0,delay*2.0)
    main.websocket_loop=resilient; main.parse_quote_packet=parse_dhan_quote_packet; main._psy29_resilient_ws=True

_install_resilient_websocket()
