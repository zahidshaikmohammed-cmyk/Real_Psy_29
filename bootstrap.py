from __future__ import annotations

import asyncio
import json
import struct
import time

import main
from psy29.data_integrity import DataIntegrityError, parse_dhan_quote_packet, validate_live_tick

# runner.py intentionally remains the public-data layer. We load it first so all
# existing endpoints/validation/checkpointing are preserved, then replace only
# the transport boundary with a canonical Dhan v2 parser and backoff reconnect.
import runner  # noqa: E402,F401

main.parse_quote_packet = parse_dhan_quote_packet


async def _stable_websocket_loop(token: str):
    client_id = main.os.environ["DHAN_CLIENT_ID"]
    reverse = {int(v): k for k, v in main.state["security_map"].items()}
    url = f"{main.WS_URL}?version=2&token={token}&clientId={client_id}&authType=2"
    backoff = 5.0

    while main.in_session(main.now_ist()):
        try:
            async with main.websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=None,
            ) as ws:
                instruments = [
                    {"ExchangeSegment": "NSE_EQ", "SecurityId": sid}
                    for sid in main.state["security_map"].values()
                ]
                for start in range(0, len(instruments), 100):
                    batch = instruments[start : start + 100]
                    await ws.send(json.dumps({
                        "RequestCode": 17,
                        "InstrumentCount": len(batch),
                        "InstrumentList": batch,
                    }))

                with main.lock:
                    main.state["source_status"] = "LIVE"
                main.log.info("Dhan websocket connected: %d NSE_EQ instruments", len(instruments))
                backoff = 5.0

                while main.in_session(main.now_ist()):
                    message = await asyncio.wait_for(ws.recv(), timeout=35)
                    if isinstance(message, str):
                        continue
                    if not message:
                        continue

                    # Dhan v2 Feed Disconnect packet: response code 50,
                    # reason code at bytes 9-10 (zero-based offset 8).
                    if message[0] == 50:
                        reason = struct.unpack_from("<h", message, 8)[0] if len(message) >= 10 else None
                        raise RuntimeError(f"Dhan feed disconnect packet reason={reason}")

                    parsed = parse_dhan_quote_packet(message)
                    if not parsed:
                        continue

                    sid, ltp, volume, ltt, day_open, day_high, day_low = parsed
                    symbol = reverse.get(int(sid))
                    if not symbol:
                        continue

                    try:
                        with main.lock:
                            stock = main.state["stocks"].get(symbol)
                            previous_volume = stock.get("volume") if stock else None
                            last_epoch = (
                                stock.get("_one_min", [])[-1]["epoch"]
                                if stock and stock.get("_one_min")
                                else None
                            )

                        clean_price, clean_volume, clean_ltt = validate_live_tick(
                            ltp,
                            volume,
                            ltt,
                            main.now_ist(),
                            previous_volume,
                        )
                        if last_epoch is not None and clean_ltt < int(last_epoch):
                            raise DataIntegrityError("out-of-order live tick")

                        # Update through the original state mutator, not the
                        # runner's public-gate wrapper. The public gate runs
                        # immediately afterward and canonicalizes OHLC from
                        # validated 1m candles.
                        main._original_update_tick(
                            symbol,
                            clean_price,
                            clean_volume,
                            clean_ltt,
                            clean_price,
                            clean_price,
                            clean_price,
                        )

                        with main.lock:
                            stock = main.state["stocks"].get(symbol)
                            if not stock:
                                continue
                            candles = stock.get("_one_min", [])
                            if not candles:
                                raise DataIntegrityError("live tick produced no 1m candle")
                            session_ohlc = runner._canonical_session_ohlc(candles)
                            stock["ohlc"] = session_ohlc
                            stock["session_high"] = session_ohlc["high"]
                            stock["session_low"] = session_ohlc["low"]
                            stock["current_price"] = clean_price
                            stock["volume"] = clean_volume
                            stock["last_tick"] = main.datetime.fromtimestamp(
                                clean_ltt, main.IST
                            ).isoformat()
                            stock["data_source_status"] = "LIVE"
                            main.state["last_update"] = main.now_ist().isoformat()

                    except DataIntegrityError as exc:
                        # A corrupt tick must be quarantined, never allowed to
                        # tear down the single live WebSocket for all 29 stocks.
                        main.log.warning(
                            "Rejected corrupt live tick for %s: %s",
                            symbol,
                            exc,
                        )

        except Exception as exc:
            with main.lock:
                main.state["source_status"] = "RECONNECTING"
            if not main.in_session(main.now_ist()):
                break
            main.log.warning(
                "Dhan websocket disconnected: %s; reconnecting in %.1fs",
                exc,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(60.0, backoff * 2.0)


main.websocket_loop = _stable_websocket_loop
main.parse_quote_packet = parse_dhan_quote_packet


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        runner.app,
        host="0.0.0.0",
        port=int(main.os.getenv("PORT", "10000")),
    )
