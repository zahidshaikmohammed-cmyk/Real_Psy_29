import threading
import time

import main
from psy29.intraday_store import IntradayStore


# Replace the legacy startup hook with a supervisor that brings the live feed up
# before the slower historical backfill. This keeps /data live immediately at
# session open instead of blocking on 29 sequential REST history requests.
main.app.router.on_startup.clear()

store = IntradayStore()
CHECKPOINT_SECONDS = 60.0


def _snapshot() -> tuple[str | None, dict]:
    with main.lock:
        trading_date = main.state.get("trading_date")
        stocks = {
            symbol: main.clean_stock(payload)
            for symbol, payload in main.state.get("stocks", {}).items()
        }
    return trading_date, stocks


def _checkpoint(force: bool = False) -> int:
    trading_date, stocks = _snapshot()
    if not trading_date or not stocks:
        return 0
    saved = store.save_market(trading_date, stocks)
    if saved:
        main.log.info("Intraday Postgres checkpoint: %s/%s stocks", saved, len(stocks))
    return saved


def _checkpoint_worker() -> None:
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


def _seed_live_state(token: str, security_map: dict[str, str]) -> None:
    now = main.now_ist()
    client_id = main.os.environ["DHAN_CLIENT_ID"]
    quote_map = main.fetch_market_quote(token, client_id, security_map)
    with main.lock:
        main.state["trading_date"] = now.date().isoformat()
        main.state["market_session_status"] = main.session_status(now)
        main.state["security_map"] = security_map
        main.state["stocks"] = {}
        for symbol in main.STOCKS:
            q = quote_map.get(symbol, {})
            main.state["stocks"][symbol] = {
                "symbol": symbol,
                "security_id": security_map[symbol],
                "current_price": q.get("current"),
                "ohlc": {
                    "open": q.get("open"),
                    "high": q.get("high"),
                    "low": q.get("low"),
                    "close": q.get("close"),
                },
                "session_high": q.get("high"),
                "session_low": q.get("low"),
                "previous_day": {"high": None, "low": None, "close": None},
                "volume": q.get("volume"),
                "candles": {"1m": [], "5m": [], "15m": [], "1h": []},
                "vwap": None,
                "ema9": None,
                "ema20": None,
                "opening_range": {"period": "09:15-09:30", "status": "FORMING", "high": None, "low": None},
                "structure": {"trend": "INSUFFICIENT_DATA", "swing_high": None, "swing_low": None},
                "timestamp": now.isoformat(),
                "trading_date": now.date().isoformat(),
                "market_session_status": main.session_status(now),
                "data_source_status": "LIVE",
                "last_tick": None,
                "_one_min": [],
                "_volume_anchor": None,
            }


def _restore_checkpoint_if_needed(trading_date: str) -> None:
    saved = store.load_session(trading_date)
    if not saved:
        return
    with main.lock:
        for symbol, payload in saved.items():
            if symbol not in main.STOCKS:
                continue
            current = main.state["stocks"].get(symbol)
            # Only use durable data as a fallback. Fresh live state remains authoritative.
            if not current or current.get("current_price") is None:
                main.state["stocks"][symbol] = payload
    main.log.info("Intraday Postgres recovery loaded %s/%s saved stocks", len(saved), len(main.STOCKS))


def _backfill_worker(token: str, security_map: dict[str, str]) -> None:
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
                live_price = s.get("current_price")
                live_ohlc = dict(s.get("ohlc", {}))
                live_volume = s.get("volume")
                live_last_tick = s.get("last_tick")
                live_status = s.get("data_source_status")
            main.rebuild_stock(symbol, one_min, {
                "current": live_price,
                "open": live_ohlc.get("open"),
                "high": live_ohlc.get("high"),
                "low": live_ohlc.get("low"),
                "close": live_ohlc.get("close"),
                "volume": live_volume,
            }, prev)
            with main.lock:
                s = main.state["stocks"].get(symbol)
                if s:
                    s["last_tick"] = live_last_tick
                    s["data_source_status"] = "LIVE" if live_status == "LIVE" else s.get("data_source_status")
            time.sleep(0.22)
        except Exception as exc:
            main.log.exception("Background backfill failed for %s: %s", symbol, exc)

    _checkpoint(force=True)


def _supervisor() -> None:
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
            threading.Thread(
                target=_backfill_worker,
                args=(token, security_map),
                daemon=True,
                name="psy29-backfill",
            ).start()

            asyncio_runner = lambda: main.asyncio.run(main.websocket_loop(token))
            asyncio_runner()

            _checkpoint(force=True)
            with main.lock:
                main.state["source_status"] = "POST_CLOSE" if not main.in_session(main.now_ist()) else "RECONNECTING"
            if not main.in_session(main.now_ist()):
                continue
            time.sleep(3)
        except Exception as exc:
            main.log.exception("Collector supervisor failure: %s", exc)
            with main.lock:
                main.state["source_status"] = "ERROR"
            time.sleep(5)


def startup() -> None:
    with main.lock:
        main.state["collector_started"] = True
    threading.Thread(target=_checkpoint_worker, daemon=True, name="psy29-db-checkpoint").start()
    threading.Thread(target=_supervisor, daemon=True, name="psy29-supervisor").start()


main.app.add_event_handler("startup", startup)
app = main.app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(main.os.getenv("PORT", "10000")))
