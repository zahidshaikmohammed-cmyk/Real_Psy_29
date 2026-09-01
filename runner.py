from __future__ import annotations
import asyncio, json, math, os, struct, threading, time
from datetime import datetime
import main
from fastapi import Response
from fastapi.responses import JSONResponse
from psy29.data_integrity import DataIntegrityError
from psy29.intraday_store import IntradayStore
from psy29.session_reset import reset_for_trading_date

main.MARKET_OPEN=(9,15); main.MARKET_CLOSE=(15,15)

# Dhan chart endpoints are authenticated with both the access token and client id.
def _fetch_intraday(token, security_id, from_dt, to_dt):
    payload={"securityId":str(security_id),"exchangeSegment":"NSE_EQ","instrument":"EQUITY","interval":"1","oi":False,"fromDate":from_dt.strftime("%Y-%m-%d %H:%M:%S"),"toDate":to_dt.strftime("%Y-%m-%d %H:%M:%S")}
    r=main.dhan_post(f"{main.DHAN_BASE}/charts/intraday",token=token,client_id=os.environ["DHAN_CLIENT_ID"],payload=payload,kind="data",timeout=25,label=f"intraday:{security_id}")
    return main.parse_series_response(r.json())

def _fetch_previous(token, security_id, today):
    start=(today - __import__('datetime').timedelta(days=10)).date().isoformat(); end=today.date().isoformat()
    payload={"securityId":str(security_id),"exchangeSegment":"NSE_EQ","instrument":"EQUITY","expiryCode":0,"oi":False,"fromDate":start,"toDate":end}
    r=main.dhan_post(f"{main.DHAN_BASE}/charts/historical",token=token,client_id=os.environ["DHAN_CLIENT_ID"],payload=payload,kind="data",timeout=25,label=f"historical:{security_id}")
    rows=main.parse_series_response(r.json())
    if not rows:return {"high":None,"low":None,"close":None}
    x=rows[-1]; return {"high":x["high"],"low":x["low"],"close":x["close"]}

main.fetch_intraday_1m=_fetch_intraday
main.fetch_previous_day=_fetch_previous
CANDLE_SOURCE='DHAN_WEBSOCKET_REAL_TICKS_1M'; HISTORICAL_CANDLE_SOURCE='DHAN_INTRADAY_1M'
CHECKPOINT_SECONDS=60; CANDLE_RECONCILE_SECONDS=20; QUOTE_REFRESH_SECONDS=2
MAX_TICK_AGE_SECONDS=300; FUTURE_TICK_TOLERANCE_SECONDS=120
QUOTE_FORMAT='<BHBIfHIfIIIffff'; QUOTE_SIZE=struct.calcsize(QUOTE_FORMAT)
main.app.router.on_startup.clear(); store=IntradayStore()

def headers(rid): return {'Cache-Control':'no-store, no-cache, must-revalidate, max-age=0, s-maxage=0','Pragma':'no-cache','Expires':'0','Surrogate-Control':'no-store','Vary':'*','Access-Control-Allow-Origin':'*','X-Content-Type-Options':'nosniff','X-PSY29-Refresh-ID':rid}

def norm_epoch(v):
    v=int(v); m=abs(v)
    if m>=10**18:v//=10**9
    elif m>=10**15:v//=10**6
    elif m>=10**12:v//=10**3
    return v

def parse_packets(message):
    if not isinstance(message,(bytes,bytearray,memoryview)): return
    buf=bytes(message); off=0
    while off+QUOTE_SIZE<=len(buf):
        try: code=int(buf[off]); declared=int.from_bytes(buf[off+1:off+3],'little')
        except Exception: break
        if declared!=QUOTE_SIZE or off+declared>len(buf): break
        packet=buf[off:off+QUOTE_SIZE]
        try: code,msglen,segment,sid,ltp,ltq,ltt,atp,volume,sell,buy,op,cl,hi,lo=struct.unpack(QUOTE_FORMAT,packet)
        except struct.error: break
        if code==4 and segment==1:
            yield {'security_id':int(sid),'ltp':float(ltp),'ltq':int(ltq),'ltt':norm_epoch(ltt),'volume':int(volume),'open':float(op),'high':float(hi),'low':float(lo)}
        off+=declared

def valid_tick(tick,now):
    p=float(tick['ltp']); v=int(tick['volume']); q=max(0,int(tick['ltq']))
    if not math.isfinite(p) or p<=0 or p>10_000_000: raise DataIntegrityError('non-finite/out-of-range equity price')
    if v<0: raise DataIntegrityError('invalid cumulative volume')
    dt=datetime.fromtimestamp(int(tick['ltt']),main.IST)
    if dt.date()!=now.date(): raise DataIntegrityError('tick timestamp outside trading date')
    if not main.MARKET_OPEN <= (dt.hour,dt.minute) < main.MARKET_CLOSE: raise DataIntegrityError('tick timestamp outside NSE session')
    age=(now-dt).total_seconds()
    if age < -FUTURE_TICK_TOLERANCE_SECONDS: raise DataIntegrityError('future live tick')
    if age > MAX_TICK_AGE_SECONDS: raise DataIntegrityError('stale live tick')
    return p,v,q,dt

def seed(token,security_map):
    now=main.now_ist(); client_id=os.environ['DHAN_CLIENT_ID']
    try: quotes=main.fetch_market_quote(token,client_id,security_map)
    except Exception as exc: main.log.warning('Initial market quote snapshot unavailable: %s',exc); quotes={}
    with main.lock: main.state.update(trading_date=now.date().isoformat(),market_session_status='OPEN',security_map=security_map,stocks={},source_status='CONNECTING')
    for symbol in main.STOCKS:
        q=quotes.get(symbol) or {}
        try: prev=main.fetch_previous_day(token,security_map[symbol],now)
        except Exception as exc: main.log.warning('Previous-day reference unavailable for %s: %s',symbol,exc); prev={'high':None,'low':None,'close':None}
        with main.lock:
            main.state['stocks'][symbol]={'symbol':symbol,'security_id':security_map[symbol],'current_price':q.get('current'),'ohlc':{'open':q.get('open'),'high':q.get('high'),'low':q.get('low'),'close':q.get('current')},'session_high':q.get('high'),'session_low':q.get('low'),'previous_day':prev,'volume':q.get('volume'),'candles':{'1m':[],'5m':[],'15m':[],'1h':[]},'vwap':None,'ema9':None,'ema20':None,'opening_range':{'period':'09:15-09:30','status':'NOT_FORMED','high':None,'low':None},'structure':{'trend':'INSUFFICIENT_DATA','swing_high':None,'swing_low':None},'timestamp':now.isoformat(),'trading_date':now.date().isoformat(),'market_session_status':'OPEN','data_source_status':'LIVE','last_tick':None,'completed_candle_count':0,'last_completed_candle':None,'candle_source':CANDLE_SOURCE,'_one_min':[],'_builder':None}
    main.log.info('Seeded live state: %d/%d stocks',len(main.state['stocks']),len(main.STOCKS))

def commit_bar(symbol,bar):
    if not bar:return
    with main.lock:
        s=main.state['stocks'].get(symbol)
        if not s:return
        rows=list(s.get('_one_min',[]))
    if rows and int(rows[-1]['epoch'])==int(bar['epoch']):return
    rows.append(dict(bar)); rows.sort(key=lambda x:int(x['epoch']))
    with main.lock:
        s=main.state['stocks'].get(symbol)
        if not s:return
        quote={'current':s.get('current_price'),'open':s.get('ohlc',{}).get('open') or rows[0]['open'],'high':s.get('ohlc',{}).get('high') or max(r['high'] for r in rows),'low':s.get('ohlc',{}).get('low') or min(r['low'] for r in rows),'close':s.get('current_price') or rows[-1]['close'],'volume':s.get('volume')}
        prev=s.get('previous_day') or {'high':None,'low':None,'close':None}; last=s.get('last_tick'); builder=s.get('_builder')
    main.rebuild_stock(symbol,rows,quote,prev)
    with main.lock:
        s=main.state['stocks'].get(symbol)
        if not s:return
        s['candle_source']=CANDLE_SOURCE; s['completed_candle_count']=len(rows); s['last_completed_candle']=rows[-1]['timestamp']; s['_one_min']=rows; s['_builder']=builder; s['data_source_status']='LIVE'; s['last_tick']=last or bar['timestamp']; main.state['last_update']=main.now_ist().isoformat()

def ingest(symbol,tick):
    now=main.now_ist(); price,cum,ltq,dt=valid_tick(tick,now); minute=dt.replace(second=0,microsecond=0); epoch=int(minute.timestamp())
    with main.lock:
        s=main.state['stocks'].get(symbol)
        if not s:return
        b=s.get('_builder')
        if b and epoch<int(b['epoch']): raise DataIntegrityError('out-of-order live tick')
        finished=dict(b) if b and epoch>int(b['epoch']) else None
    if finished and int(finished['epoch']) <= int(now.replace(second=0,microsecond=0).timestamp()): commit_bar(symbol,finished)
    with main.lock:
        s=main.state['stocks'].get(symbol)
        if not s:return
        b=s.get('_builder')
        if b is None or int(b['epoch'])!=epoch: b={'timestamp':minute.isoformat(),'epoch':epoch,'open':price,'high':price,'low':price,'close':price,'volume':0}; s['_builder']=b
        b['high']=max(float(b['high']),price); b['low']=min(float(b['low']),price); b['close']=price; b['volume']=int(b.get('volume',0))+ltq
        s['current_price']=price; s['volume']=cum; s['timestamp']=now.isoformat(); s['market_session_status']=main.session_status(now); s['data_source_status']='LIVE'; s['last_tick']=dt.isoformat(); s['ohlc']['close']=price; s['ohlc']['high']=max(s['ohlc'].get('high') or price,price); s['ohlc']['low']=min(s['ohlc'].get('low') or price,price); s['session_high']=s['ohlc']['high']; s['session_low']=s['ohlc']['low']; main.state['last_update']=now.isoformat()

def finalize():
    cur=int(main.now_ist().replace(second=0,microsecond=0).timestamp())
    for symbol in main.STOCKS:
        with main.lock: s=dict(main.state['stocks'].get(symbol,{})); b=dict(s.get('_builder')) if s.get('_builder') else None
        if b and int(b['epoch'])<cur: commit_bar(symbol,b)

def reconcile(token):
    now=main.now_ist(); cur=int(now.replace(second=0,microsecond=0).timestamp()); start=now.replace(hour=9,minute=15,second=0,microsecond=0)
    with main.lock: sm=dict(main.state.get('security_map') or {})
    for symbol in main.STOCKS:
        if not main.in_session(main.now_ist()):break
        sid=sm.get(symbol)
        if not sid:continue
        try:
            rows=main.fetch_intraday_1m(token,sid,start,now); completed=[r for r in rows if int(r.get('epoch',0))<cur]
            if not completed:continue
            with main.lock:
                s=main.state['stocks'].get(symbol); existing=list(s.get('_one_min',[])) if s else []; builder=dict(s.get('_builder')) if s and s.get('_builder') else None; last=s.get('last_tick') if s else None; current=s.get('current_price') if s else None; volume=s.get('volume') if s else None; prev=s.get('previous_day') if s else {'high':None,'low':None,'close':None}
            merged={int(r['epoch']):r for r in existing}; merged.update({int(r['epoch']):r for r in completed}); merged_rows=[merged[k] for k in sorted(merged)]
            quote={'current':current or merged_rows[-1]['close'],'open':merged_rows[0]['open'],'high':max(r['high'] for r in merged_rows),'low':min(r['low'] for r in merged_rows),'close':current or merged_rows[-1]['close'],'volume':volume}
            main.rebuild_stock(symbol,merged_rows,quote,prev or {'high':None,'low':None,'close':None})
            with main.lock:
                s=main.state['stocks'][symbol]; s['_one_min']=merged_rows; s['_builder']=builder; s['candle_source']=HISTORICAL_CANDLE_SOURCE; s['completed_candle_count']=len(merged_rows); s['last_completed_candle']=merged_rows[-1]['timestamp']; s['last_tick']=last; s['data_source_status']='LIVE'; s['timestamp']=now.isoformat()
        except Exception as exc: main.log.warning('Candle reconciliation failed for %s: %s',symbol,exc)

def candle_worker(token):
    while main.in_session(main.now_ist()):
        try: finalize(); reconcile(token)
        except Exception as exc: main.log.warning('Candle reconciliation worker failed: %s',exc)
        time.sleep(CANDLE_RECONCILE_SECONDS)

def quote_worker(token):
    while main.in_session(main.now_ist()):
        try:
            with main.lock: sm=dict(main.state.get('security_map') or {})
            if len(sm)==len(main.STOCKS):
                q=main.fetch_market_quote(token,os.environ['DHAN_CLIENT_ID'],sm); now=main.now_ist()
                with main.lock:
                    for symbol,x in q.items():
                        s=main.state['stocks'].get(symbol)
                        if s and x.get('current') is not None:s['current_price']=x['current']; s['ohlc']['close']=x['current']; s['timestamp']=now.isoformat(); s['data_source_status']='LIVE'
                    main.state['last_update']=now.isoformat()
        except Exception as exc: main.log.warning('Optional quote snapshot unavailable: %s',exc)
        time.sleep(QUOTE_REFRESH_SECONDS)

def websocket_loop(token):
    reverse={int(v):k for k,v in main.state['security_map'].items()}; url=f'{main.WS_URL}?version=2&token={token}&clientId={os.environ["DHAN_CLIENT_ID"]}&authType=2'; delay=3
    while main.in_session(main.now_ist()):
        try:
            async def session():
                async with main.websockets.connect(url,ping_interval=20,ping_timeout=20,close_timeout=5,max_size=None) as ws:
                    instruments=[{'ExchangeSegment':'NSE_EQ','SecurityId':sid} for sid in main.state['security_map'].values()]
                    for start in range(0,len(instruments),100):
                        batch=instruments[start:start+100]; await ws.send(json.dumps({'RequestCode':17,'InstrumentCount':len(batch),'InstrumentList':batch}))
                    with main.lock: main.state['source_status']='LIVE'
                    main.log.info('Dhan real-time websocket connected: %d instruments',len(instruments))
                    while main.in_session(main.now_ist()):
                        message=await asyncio.wait_for(ws.recv(),timeout=35)
                        if isinstance(message,str):continue
                        for tick in parse_packets(message):
                            symbol=reverse.get(int(tick['security_id']))
                            if symbol:
                                try:ingest(symbol,tick)
                                except DataIntegrityError as exc:main.log.warning('Rejected live tick for %s: %s',symbol,exc)
                        finalize()
            asyncio.run(session()); delay=3
        except Exception as exc:
            with main.lock:main.state['source_status']='RECONNECTING'
            if not main.in_session(main.now_ist()):break
            main.log.warning('Dhan websocket disconnected: %s; retry in %.1fs',exc,delay); time.sleep(delay); delay=min(30,delay*2)

def checkpoint():
    while True:
        try:
            if main.in_session(main.now_ist()):
                with main.lock:date=main.state.get('trading_date'); stocks={k:main.clean_stock(v) for k,v in main.state.get('stocks',{}).items()}
                if date and stocks:
                    try:store.save_market(date,stocks)
                    except Exception as exc:main.log.warning('Intraday checkpoint failed: %s',exc)
                time.sleep(CHECKPOINT_SECONDS)
            else:time.sleep(10)
        except Exception as exc:main.log.warning('Checkpoint worker error: %s',exc); time.sleep(CHECKPOINT_SECONDS)

def supervisor():
    main.log.info('PSY29 live supervisor started')
    while True:
        now=main.now_ist()
        with main.lock:
            reset_for_trading_date(main.state,now.date()); main.state['market_session_status']=main.session_status(now)
            if not main.in_session(now):main.state['source_status']='WAITING_FOR_SESSION'; main.state['stocks']={}
            ready=main.state.get('source_status') in {'CONNECTING','LIVE','RECONNECTING'} and len(main.state.get('stocks',{}))==len(main.STOCKS)
        if not main.in_session(now):time.sleep(2);continue
        if ready:time.sleep(1);continue
        with main.lock:main.state['source_status']='CONNECTING'
        try:
            main.log.info('Opening live Dhan session for %s',now.date().isoformat()); token,expiry=main.generate_access_token(); sm=main.load_security_map()
            with main.lock:main.state['access_token_expiry']=expiry
            seed(token,sm)
            threading.Thread(target=candle_worker,args=(token,),daemon=True,name='psy29-candle-reconcile').start()
            threading.Thread(target=quote_worker,args=(token,),daemon=True,name='psy29-quote-refresh').start()
            websocket_loop(token)
        except Exception as exc: main.log.exception('Live collector failure: %s',exc); time.sleep(5)

def payload():
    now=main.now_ist()
    with main.lock:
        reset_for_trading_date(main.state,now.date()); stocks={k:main.clean_stock(v) for k,v in main.state.get('stocks',{}).items()}; source=main.state.get('source_status') or 'WAITING_FOR_SESSION'
    raw={'service':'PSY29 Live Data','timestamp':now.isoformat(),'trading_date':now.date().isoformat(),'market_session_status':main.session_status(now),'data_source_status':source,'stocks_expected':len(main.STOCKS),'stocks':stocks,'candle_policy':{'source':CANDLE_SOURCE,'fallback_source':HISTORICAL_CANDLE_SOURCE,'first_completed_minute':'09:16','regular_session':'09:15-15:15 IST','synthetic_candles':False,'rule':'Only completed bars from Dhan websocket ticks or Dhan intraday candles are published; no bar is fabricated from server time.'}}
    missing=[s for s in main.STOCKS if s not in stocks or stocks[s].get('current_price') is None]; no=[s for s in main.STOCKS if s in stocks and stocks[s].get('completed_candle_count',0)==0]
    raw['diagnostic']={'status':'OK' if not missing and not no else 'RECOVERING','error_code':None if not missing and not no else 'LIVE_DATA_PENDING','error_message':None if not missing and not no else 'Waiting for automatic Dhan live state/candle reconciliation.','stage':'FEED','affected_stocks':missing or no,'recovery_action':None if not missing and not no else 'AUTOMATIC_RECONNECT_AND_RECONCILIATION','data_safe':not missing and not no}
    return main.normalize_market(raw)

def response():return JSONResponse(content=payload(),headers=headers(str(time.time_ns())))
for route in ['/api/v1/live.json','/api/v1/market.json','/data.json']:main.app.add_api_route(route,response,methods=['GET'])
@main.app.get('/data.txt')
def data_txt():return Response(content=json.dumps(payload(),separators=(',',':'),ensure_ascii=False),media_type='text/plain',headers=headers(str(time.time_ns())))
@main.app.get('/live.txt')
def live_txt():return data_txt()
@main.app.get('/')
def root():
    with main.lock:return {'service':'PSY29 Live Data','status':main.state.get('source_status'),'trading_date':main.state.get('trading_date'),'market_session_status':main.state.get('market_session_status'),'stocks_expected':len(main.STOCKS),'stocks_loaded':len(main.state.get('stocks',{})),'last_update':main.state.get('last_update'),'postgres':False,'storage_mode':'in-memory intraday session only'}
@main.app.get('/health')
def health():return {'ok':True,'status':main.state.get('source_status'),'timestamp':main.now_ist().isoformat(),'candle_source':CANDLE_SOURCE,'fallback_candle_source':HISTORICAL_CANDLE_SOURCE,'synthetic_candles':False,'market_close':'15:15'}

def startup():
    if getattr(main,'_psy29_runtime_started',False):return
    main._psy29_runtime_started=True
    with main.lock:main.state['collector_started']=True; main.state['source_status']='WAITING_FOR_SESSION'; main.state['market_session_status']=main.session_status(main.now_ist())
    threading.Thread(target=checkpoint,daemon=True,name='psy29-checkpoint').start(); threading.Thread(target=supervisor,daemon=True,name='psy29-supervisor').start()
main.app.router.on_startup.clear(); main.app.add_event_handler('startup',startup); startup(); app=main.app
if __name__=='__main__':
 import uvicorn; uvicorn.run(app,host='0.0.0.0',port=int(os.getenv('PORT','10000')))
