from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import monotonic
from typing import Iterable


class FeedHealth(str, Enum):
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    RECOVERING = "RECOVERING"
    AUTH_EXPIRED = "AUTH_EXPIRED"


class RecoveryAction(str, Enum):
    NONE = "NONE"
    RECONNECT = "RECONNECT"
    REAUTHORIZE = "REAUTHORIZE"
    REST_RECONCILE = "REST_RECONCILE"
    RESET_SESSION = "RESET_SESSION"


@dataclass(frozen=True)
class FreshnessPolicy:
    live_after_seconds: float = 5.0
    degraded_after_seconds: float = 15.0
    stale_after_seconds: float = 30.0
    reconnect_after_seconds: float = 35.0
    max_reconnect_attempts: int = 8
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not (0 < self.live_after_seconds < self.degraded_after_seconds < self.stale_after_seconds):
            raise ValueError("freshness thresholds must be strictly increasing")
        if self.reconnect_after_seconds < self.stale_after_seconds:
            raise ValueError("reconnect threshold cannot precede stale threshold")
        if self.max_reconnect_attempts < 1:
            raise ValueError("max_reconnect_attempts must be positive")
        if self.initial_backoff_seconds <= 0 or self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("invalid reconnect backoff")


@dataclass(frozen=True)
class InstrumentFreshness:
    symbol: str
    last_trade_epoch: int | None
    last_received_monotonic: float | None
    health: FeedHealth


@dataclass(frozen=True)
class RecoveryDecision:
    health: FeedHealth
    action: RecoveryAction
    stale_symbols: tuple[str, ...]
    missing_symbols: tuple[str, ...]
    reconnect_attempt: int
    retry_delay_seconds: float
    reason: str


@dataclass
class FreshnessRecoveryEngine:
    symbols: tuple[str, ...]
    policy: FreshnessPolicy = FreshnessPolicy()
    reconnect_attempt: int = 0
    _connected: bool = False
    _authorized: bool = False
    _last_connection_monotonic: float | None = None
    _last_market_tick_monotonic: float | None = None
    _last_seen: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("symbols must be a non-empty unique sequence")
        self._last_seen = {}

    def on_connected(self, now: float | None = None) -> None:
        t = monotonic() if now is None else now
        self._connected = True
        self._authorized = False
        self._last_connection_monotonic = t
        self._last_market_tick_monotonic = None

    def on_authorized(self) -> None:
        if not self._connected:
            raise RuntimeError("cannot authorize a disconnected feed")
        self._authorized = True
        self.reconnect_attempt = 0

    def on_tick(self, symbol: str, now: float | None = None) -> None:
        if symbol not in self._last_seen:  # type: ignore[operator]
            raise KeyError(f"unknown symbol: {symbol}")
        t = monotonic() if now is None else now
        self._last_seen[symbol] = t  # type: ignore[index]
        self._last_market_tick_monotonic = t

    def on_disconnect(self, reason_code: int | None = None) -> RecoveryDecision:
        self._connected = False
        self._authorized = False
        if reason_code in (807, 808, 809):
            health = FeedHealth.AUTH_EXPIRED
            action = RecoveryAction.REAUTHORIZE
            reason = f"Dhan authentication/disconnection code {reason_code}"
        else:
            health = FeedHealth.DISCONNECTED
            action = RecoveryAction.RECONNECT
            reason = "market feed disconnected"
        return self._decision(health, action, (), (), reason)

    def reset_for_new_session(self) -> None:
        self._connected = False
        self._authorized = False
        self.reconnect_attempt = 0
        self._last_connection_monotonic = None
        self._last_market_tick_monotonic = None
        assert self._last_seen is not None
        self._last_seen.clear()

    def observe(self, now: float | None = None) -> RecoveryDecision:
        t = monotonic() if now is None else now
        last_seen = self._last_seen or {}
        stale = tuple(sorted(s for s in self.symbols if s not in last_seen or t - last_seen[s] >= self.policy.stale_after_seconds))
        missing = tuple(sorted(s for s in self.symbols if s not in last_seen))

        if not self._connected:
            return self._decision(FeedHealth.DISCONNECTED, RecoveryAction.RECONNECT, stale, missing, "feed is disconnected")
        if not self._authorized:
            return self._decision(FeedHealth.RECOVERING, RecoveryAction.REAUTHORIZE, stale, missing, "feed is connected but not authorized")
        if missing or stale:
            if self._last_market_tick_monotonic is None or t - self._last_market_tick_monotonic >= self.policy.reconnect_after_seconds:
                return self._decision(FeedHealth.STALE, RecoveryAction.RECONNECT, stale, missing, "one or more instruments have no sufficiently fresh tick")
            return self._decision(FeedHealth.DEGRADED, RecoveryAction.REST_RECONCILE, stale, missing, "instrument feed freshness is degraded")

        age = t - max(last_seen.values()) if last_seen else float("inf")
        if age <= self.policy.live_after_seconds:
            return self._decision(FeedHealth.LIVE, RecoveryAction.NONE, (), (), "all instruments have fresh ticks")
        if age <= self.policy.degraded_after_seconds:
            return self._decision(FeedHealth.DEGRADED, RecoveryAction.REST_RECONCILE, (), (), "latest feed activity is slowing")
        if age < self.policy.reconnect_after_seconds:
            return self._decision(FeedHealth.STALE, RecoveryAction.REST_RECONCILE, (), (), "feed activity is stale")
        return self._decision(FeedHealth.STALE, RecoveryAction.RECONNECT, (), (), "feed has exceeded reconnect threshold")

    def next_backoff(self) -> float:
        exponent = max(0, self.reconnect_attempt)
        return min(self.policy.max_backoff_seconds, self.policy.initial_backoff_seconds * (2 ** exponent))

    def record_reconnect_attempt(self) -> bool:
        if self.reconnect_attempt >= self.policy.max_reconnect_attempts:
            return False
        self.reconnect_attempt += 1
        return True

    def _decision(self, health: FeedHealth, action: RecoveryAction, stale: Iterable[str], missing: Iterable[str], reason: str) -> RecoveryDecision:
        return RecoveryDecision(
            health=health,
            action=action,
            stale_symbols=tuple(stale),
            missing_symbols=tuple(missing),
            reconnect_attempt=self.reconnect_attempt,
            retry_delay_seconds=self.next_backoff() if action in (RecoveryAction.RECONNECT, RecoveryAction.REAUTHORIZE) else 0.0,
            reason=reason,
        )
