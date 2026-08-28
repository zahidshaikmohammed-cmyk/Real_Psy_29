from __future__ import annotations
import asyncio, json, os, struct
try:
    import main, websockets
    from psy29.data_integrity import DataIntegrityError, parse_dhan_quote_packet, validate_live_tick

    async def _psy29_ws(token: str):
        cid = os.environ["DHAN_CLIENT_ID"]
        reverse = {int(v): k for k, v in main.state["security_map"].items()}
        url = f"{main.WS_URL}?version=2&token={token}&clientId={cid}&authType=2"
        delay = 5.0
        while main.in_session(main.now_ist()):
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=None) as ws:
                    instruments = [{"ExchangeSegment":"NSE_EQ","SecurityId":sid} for sid in main.state["security_map"].values()]
                    for i in range(0, len(instruments), 100):
                        b = instruments[i:i+100]
                        await ws.send(json.dumps({"RequestCode":17,"InstrumentCount":len(b),"InstrumentList":b}))
                    with main.lock: main.state["source_status"] = "LIVE"
                    main.log.info("Canonical Dhan websocket connected: %d instruments", len(instruments))
                    delay = 5.0
                    while main.in_session(main.now_ist()):
                        msg = await asyncio.wait_for(ws.recv(), timeout=35)
                        if isinstance(msg, str) or not msg: continue
                        if msg[0] == 50:
                            r = struct.unpack_from("<h", msg, 8)[0] if len(msg) >= 10 else None
                            raise RuntimeError(f"Dhan feed disconnect packet reason={r}")
                        p = parse_dhan_quote_packet(msg)
                        if not p: continue
                        sid, ltp, volume, ltt, *_ = p
                        symbol = reverse.get(int(sid))
                        if not symbol: continue
                        try:
                            with main.lock:
                                s = main.state["stocks"].get(symbol)
                                pv = s.get("volume") if s else None
                                last = s.get("_one_min", [])[-1]["epoch"] if s and s.get("_one_min") else None
                            price, vol, tick = validate_live_tick(ltp, volume, ltt, main.now_ist(), pv)
                            if last is not None and tick < int(last): raise DataIntegrityError("out-of-order live tick")
                            updater = getattr(main, "_original_update_tick", main.update_tick)
                            updater(symbol, price, vol, tick, price, price, price)
                            with main.lock:
                                s = main.state["stocks"].get(symbol)
                                candles = s.get("_one_min", []) if s else []
                                if not candles: raise DataIntegrityError("live tick produced no 1m candle")
                                o = {"open":candles[0]["open"],"high":max(c["high"] for c in candles),"low":min(c["low"] for c in candles),"close":candles[-1]["close"]}
                                s["ohlc"] = o; s["session_high"] = o["high"]; s["session_low"] = o["low"]; s["current_price"] = price; s["volume"] = vol; s["last_tick"] = main.datetime.fromtimestamp(tick, main.IST).isoformat(); s["data_source_status"] = "LIVE"; main.state["last_update"] = main.now_ist().isoformat()
                        except DataIntegrityError as exc:
                            main.log.warning("Rejected corrupt live tick for %s: %s", symbol, exc)
            except Exception as exc:
                with main.lock: main.state["source_status"] = "RECONNECTING"
                if not main.in_session(main.now_ist()): break
                main.log.warning("Dhan websocket disconnected: %s; retry in %.1fs", exc, delay)
                await asyncio.sleep(delay); delay = min(60.0, delay * 2.0)

    main.websocket_loop = _psy29_ws
    main.parse_quote_packet = parse_dhan_quote_packet
    main.log.info("PSY29 canonical Dhan websocket hardening loaded")
except Exception as exc:
    print(f"PSY29 startup hardening failed: {type(exc).__name__}: {exc}", flush=True)
