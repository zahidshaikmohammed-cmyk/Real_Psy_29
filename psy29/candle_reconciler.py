from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from .candle_history import Candle


class CandleIntegrityError(ValueError):
    """Raised when a candle series cannot be trusted as a continuous series."""


@dataclass(frozen=True)
class ReconciliationResult:
    candles: tuple[Candle, ...]
    missing_timestamps: tuple[datetime, ...]
    duplicate_count: int
    out_of_order_count: int


def validate_candle(candle: Candle) -> None:
    if candle.timestamp.tzinfo is None:
        raise CandleIntegrityError("Candle timestamp must be timezone-aware")
    if candle.open <= 0 or candle.high <= 0 or candle.low <= 0 or candle.close <= 0:
        raise CandleIntegrityError("Candle prices must be positive")
    if candle.high < max(candle.open, candle.close):
        raise CandleIntegrityError("Candle high is below open/close")
    if candle.low > min(candle.open, candle.close):
        raise CandleIntegrityError("Candle low is above open/close")
    if candle.volume < 0:
        raise CandleIntegrityError("Candle volume cannot be negative")


def _same_candle(left: Candle, right: Candle) -> bool:
    return (
        left.timestamp == right.timestamp
        and left.open == right.open
        and left.high == right.high
        and left.low == right.low
        and left.close == right.close
        and left.volume == right.volume
    )


def _find_missing(candles: tuple[Candle, ...], interval_minutes: int) -> list[datetime]:
    missing: list[datetime] = []
    for previous, current in zip(candles, candles[1:]):
        expected = previous.timestamp + timedelta(minutes=interval_minutes)
        while expected < current.timestamp:
            missing.append(expected)
            expected += timedelta(minutes=interval_minutes)
    return missing


def reconcile_candles(
    existing: tuple[Candle, ...],
    incoming: tuple[Candle, ...],
    *,
    interval_minutes: int,
    fetch_missing: Callable[[datetime, datetime], tuple[Candle, ...]] | None = None,
) -> ReconciliationResult:
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")

    accepted: dict[datetime, Candle] = {}
    duplicate_count = 0
    out_of_order_count = 0
    last_timestamp: datetime | None = None

    for candle in (*existing, *incoming):
        validate_candle(candle)
        if last_timestamp is not None and candle.timestamp < last_timestamp:
            out_of_order_count += 1
        last_timestamp = candle.timestamp if last_timestamp is None else max(last_timestamp, candle.timestamp)

        previous = accepted.get(candle.timestamp)
        if previous is not None:
            if not _same_candle(previous, candle):
                raise CandleIntegrityError(
                    f"Conflicting candle values for timestamp {candle.timestamp.isoformat()}"
                )
            duplicate_count += 1
            continue
        accepted[candle.timestamp] = candle

    ordered = tuple(accepted[key] for key in sorted(accepted))
    missing = _find_missing(ordered, interval_minutes)

    if missing and fetch_missing is not None:
        recovered = fetch_missing(missing[0], missing[-1])
        for candle in recovered:
            validate_candle(candle)
            previous = accepted.get(candle.timestamp)
            if previous is not None and not _same_candle(previous, candle):
                raise CandleIntegrityError(
                    f"Recovery returned conflicting candle for {candle.timestamp.isoformat()}"
                )
            if previous is None:
                accepted[candle.timestamp] = candle
        ordered = tuple(accepted[key] for key in sorted(accepted))
        missing = _find_missing(ordered, interval_minutes)

    return ReconciliationResult(
        candles=ordered,
        missing_timestamps=tuple(missing),
        duplicate_count=duplicate_count,
        out_of_order_count=out_of_order_count,
    )
