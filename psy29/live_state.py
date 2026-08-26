from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Mapping

from .dhan_websocket import QuoteTick
from .instrument_registry import InstrumentRegistry


@dataclass(frozen=True)
class LiveTick:
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
    day_high: float
    day_low: float
    received_epoch: float


@dataclass
class LiveState:
    symbol: str
    security_id: str
    current_price: float | None = None
    session_open: float | None = None
    session_high: float | None = None
    session_low: float | None = None
    volume: int | None = None
    average_trade_price: float | None = None
    last_trade_quantity: int | None = None
    total_buy_quantity: int | None = None
    total_sell_quantity: int | None = None
    last_trade_epoch: int | None = None
    last_received_epoch: float | None = None
    update_count: int = 0

    def apply(self, tick: QuoteTick, received_epoch: float) -> None:
        if tick.symbol != self.symbol or tick.security_id != self.security_id:
            raise ValueError("Quote does not belong to this instrument")
        if self.last_trade_epoch is not None and tick.last_trade_epoch < self.last_trade_epoch:
            return
        self.current_price = tick.ltp
        self.session_open = tick.day_open
        self.session_high = tick.day_high
        self.session_low = tick.day_low
        self.volume = tick.volume
        self.average_trade_price = tick.average_trade_price
        self.last_trade_quantity = tick.last_trade_quantity
        self.total_buy_quantity = tick.total_buy_quantity
        self.total_sell_quantity = tick.total_sell_quantity
        self.last_trade_epoch = tick.last_trade_epoch
        self.last_received_epoch = received_epoch
        self.update_count += 1

    def snapshot(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "security_id": self.security_id,
            "current_price": self.current_price,
            "session_open": self.session_open,
            "session_high": self.session_high,
            "session_low": self.session_low,
            "volume": self.volume,
            "average_trade_price": self.average_trade_price,
            "last_trade_quantity": self.last_trade_quantity,
            "total_buy_quantity": self.total_buy_quantity,
            "total_sell_quantity": self.total_sell_quantity,
            "last_trade_epoch": self.last_trade_epoch,
            "last_received_epoch": self.last_received_epoch,
            "update_count": self.update_count,
        }


class LiveTickStateEngine:
    def __init__(self, registry: InstrumentRegistry) -> None:
        self._lock = RLock()
        self._states = {
            item.symbol: LiveState(symbol=item.symbol, security_id=item.security_id)
            for item in registry.instruments
        }
        self._symbol_by_security_id = {
            item.security_id: item.symbol for item in registry.instruments
        }

    def apply_quote(self, tick: QuoteTick, received_epoch: float) -> bool:
        with self._lock:
            symbol = self._symbol_by_security_id.get(tick.security_id)
            if symbol != tick.symbol:
                return False
            state = self._states[symbol]
            before = state.update_count
            state.apply(tick, received_epoch)
            return state.update_count != before

    def get(self, symbol: str) -> dict[str, object]:
        with self._lock:
            return self._states[symbol].snapshot()

    def snapshot(self) -> Mapping[str, dict[str, object]]:
        with self._lock:
            return {symbol: state.snapshot() for symbol, state in self._states.items()}

    def complete(self) -> bool:
        with self._lock:
            return all(state.current_price is not None for state in self._states.values())

    def reset(self) -> None:
        with self._lock:
            for state in self._states.values():
                state.current_price = None
                state.session_open = None
                state.session_high = None
                state.session_low = None
                state.volume = None
                state.average_trade_price = None
                state.last_trade_quantity = None
                state.total_buy_quantity = None
                state.total_sell_quantity = None
                state.last_trade_epoch = None
                state.last_received_epoch = None
                state.update_count = 0
