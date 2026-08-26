"""Runtime supervisor for a market-hours live collector.

Keeps supervision separate from the transport implementation. The host can call
supervise() after a collector cycle returns; during market hours a returned
collector is treated as recoverable rather than terminal.
"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class SupervisorPolicy:
    retry_seconds: float = 3.0
    max_stale_seconds: float = 35.0


class CollectorSupervisor:
    def __init__(self, policy: SupervisorPolicy | None = None):
        self.policy = policy or SupervisorPolicy()
        self.last_tick_monotonic: float | None = None
        self.running = False

    def record_tick(self, now: float | None = None) -> None:
        self.last_tick_monotonic = time.monotonic() if now is None else now
        self.running = True

    def feed_is_fresh(self, now: float | None = None) -> bool:
        if self.last_tick_monotonic is None:
            return False
        current = time.monotonic() if now is None else now
        return current - self.last_tick_monotonic <= self.policy.max_stale_seconds

    def retry_delay(self) -> float:
        return self.policy.retry_seconds
