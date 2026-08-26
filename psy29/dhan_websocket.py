from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass
from typing import Awaitable, Callable

import websockets

from .instrument_registry import InstrumentRegistry

WS_URL = "wss://api-feed.dhan.co"
FEED_VERSION = "2"
AUTH_TYPE = "2"
SUBSCRIBE_QUOTE = 17
UNSUBSCRIBE_QUOTE = 18
DISCONNECT = 50

HEADER_SIZE = 8
QUOTE_PACKET_SIZE = 52


class DhanWebSocketError(RuntimeError):
    """Raised when the Dhan live market feed cannot be used safely."""


@dataclass(frozen=True)
class QuoteTick:
    symbol: str
    security_id: str
    ltp: float
    last_trade_quantity: int
    last_trade_epoch: int
    average_trade_price: float
    volume: int
    total_sell_quantity: int
    total_buy_quantity: int
    day_open: float
    day_close: float
    day_high: float
    day_low: float


@dataclass(frozen=True)
class FeedDisconnect:
    reason_code: int


def websocket_url(token: str, client_id: str) -> str:
    if not token or not client_id:
        raise DhanWebSocketError("Dhan WebSocket requires access token and client ID")
    return (
        f"{WS_URL}?version={FEED_VERSION}&token={token}"
        f"&clientId={client_id}&authType={AUTH_TYPE}"
    )


def subscription_messages(registry: InstrumentRegistry) -> list[dict]:
    instruments = [
        {"ExchangeSegment": item.exchange_segment, "SecurityId": item.security_id}
        for item in registry.instruments
    ]
    return [{
        "RequestCode": SUBSCRIBE_QUOTE,
        "InstrumentCount": len(instruments),
        "InstrumentList": instruments,
    }]


def parse_quote_packet(payload: bytes, registry: InstrumentRegistry) -> QuoteTick | None:
    if len(payload) < QUOTE_PACKET_SIZE or payload[0] != 4:
        return None

    security_id = str(struct.unpack_from("<i", payload, 4)[0])
    instrument = registry.by_security_id.get(security_id)
    if instrument is None:
        return None

    return QuoteTick(
        symbol=instrument.symbol,
        security_id=security_id,
        ltp=struct.unpack_from("<f", payload, 8)[0],
        last_trade_quantity=struct.unpack_from("<i", payload, 12)[0],
        last_trade_epoch=struct.unpack_from("<i", payload, 16)[0],
        average_trade_price=struct.unpack_from("<f", payload, 20)[0],
        volume=struct.unpack_from("<i", payload, 24)[0],
        total_sell_quantity=struct.unpack_from("<i", payload, 28)[0],
        total_buy_quantity=struct.unpack_from("<i", payload, 32)[0],
        day_open=struct.unpack_from("<f", payload, 36)[0],
        day_close=struct.unpack_from("<f", payload, 40)[0],
        day_high=struct.unpack_from("<f", payload, 44)[0],
        day_low=struct.unpack_from("<f", payload, 48)[0],
    )


def parse_disconnect_packet(payload: bytes) -> FeedDisconnect | None:
    if len(payload) < HEADER_SIZE + 2 or payload[0] != 50:
        return None
    return FeedDisconnect(reason_code=struct.unpack_from("<h", payload, 8)[0])


async def receive_quotes(
    websocket,
    registry: InstrumentRegistry,
    on_quote: Callable[[QuoteTick], Awaitable[None]],
    on_disconnect: Callable[[FeedDisconnect], Awaitable[None]] | None = None,
) -> None:
    async for message in websocket:
        if not isinstance(message, bytes):
            continue
        disconnect = parse_disconnect_packet(message)
        if disconnect:
            if on_disconnect:
                await on_disconnect(disconnect)
            return
        quote = parse_quote_packet(message, registry)
        if quote:
            await on_quote(quote)


async def run_once(
    token: str,
    client_id: str,
    registry: InstrumentRegistry,
    on_quote: Callable[[QuoteTick], Awaitable[None]],
    on_disconnect: Callable[[FeedDisconnect], Awaitable[None]] | None = None,
    *,
    open_timeout: float = 15.0,
) -> None:
    try:
        async with websockets.connect(
            websocket_url(token, client_id),
            open_timeout=open_timeout,
            ping_interval=20,
            ping_timeout=40,
            close_timeout=10,
            max_size=2**20,
        ) as websocket:
            for message in subscription_messages(registry):
                await websocket.send(json.dumps(message, separators=(",", ":")))
            await receive_quotes(websocket, registry, on_quote, on_disconnect)
    except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
        raise DhanWebSocketError("Dhan live market feed connection failed") from exc
