from __future__ import annotations

import asyncio
import threading
import time

import uvicorn

from psy29.intraday_store import IntradayStore

import main


store = IntradayStore()
_original_update_tick = main.update_tick
_original_initialize_session = main.initialize_session
_original_websocket_loop = main.websocket_loop
_checkpoint_lock = threading.Lock()
_last_checkpoint = 0.0
CHECKPOINT_SECONDS = 60.0


def _snapshot() -> tuple[str | None, dict]:
    with main.lock:
        trading_date = main.state.get("trading_date")
        stocks = {
            symbol: main.clean_stock(payload)
            for symbol, payload in main.state.get("stocks", {}).items()
        }
    return trading_date, stocks


def checkpoint(force: bool = False) -> int:
    global _last_checkpoint
    now = time.monotonic()
    with _checkpoint_lock:
        if not force and now - _last_checkpoint < CHECKPOINT_SECONDS:
            return 0
        trading_date, stocks = _snapshot()
        if not trading_date or not stocks:
            return 0
        saved = store.save_market(trading_date, stocks)
        if saved:
            _last_checkpoint = now
        return saved


def update_tick(*args, **kwargs):
    result = _original_update_tick(*args, **kwargs)
    checkpoint()
    return result


def initialize_session(token: str):
    _original_initialize_session(token)
    # Persist the fresh REST backfill immediately. On a later Render restart,
    # this gives us a durable same-day checkpoint even if REST is temporarily
    # unavailable during recovery.
    checkpoint(force=True)


async def websocket_loop(token: str):
    try:
        return await _original_websocket_loop(token)
    finally:
        # Final checkpoint on disconnect/close, including the normal 15:15 exit.
        checkpoint(force=True)


main.update_tick = update_tick
main.initialize_session = initialize_session
main.websocket_loop = websocket_loop


if __name__ == "__main__":
    uvicorn.run(main.app, host="0.0.0.0", port=int(main.os.getenv("PORT", "10000")))
