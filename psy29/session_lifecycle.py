from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Callable
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)


class SessionPhase(str, Enum):
    NON_TRADING = "NON_TRADING"
    PRE_OPEN = "PRE_OPEN"
    OPEN = "OPEN"
    POST_CLOSE = "POST_CLOSE"


class SessionEvent(str, Enum):
    NONE = "NONE"
    NON_TRADING_STARTED = "NON_TRADING_STARTED"
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_OPENED = "SESSION_OPENED"
    SESSION_CLOSED = "SESSION_CLOSED"
    SESSION_ROLLOVER = "SESSION_ROLLOVER"


@dataclass(frozen=True)
class SessionSnapshot:
    trading_date: date | None
    session_id: str | None
    phase: SessionPhase
    previous_phase: SessionPhase | None
    event: SessionEvent
    reset_required: bool
    is_trading_day: bool
    market_open: time
    market_close: time


TradingDayCalendar = Callable[[date], bool]


def weekday_calendar(day: date) -> bool:
    return day.weekday() < 5


class SessionLifecycleController:
    """Authoritative NSE cash-session lifecycle state machine.

    It owns only lifecycle state. It does not alter prices, indicators,
    structure, signals, or trading strategy decisions.
    """

    def __init__(
        self,
        *,
        calendar: TradingDayCalendar = weekday_calendar,
        market_open: time = MARKET_OPEN,
        market_close: time = MARKET_CLOSE,
        timezone: ZoneInfo = IST,
    ) -> None:
        if market_open >= market_close:
            raise ValueError("market_open must precede market_close")
        self.calendar = calendar
        self.market_open = market_open
        self.market_close = market_close
        self.timezone = timezone
        self._trading_date: date | None = None
        self._phase: SessionPhase | None = None

    @property
    def current_phase(self) -> SessionPhase | None:
        return self._phase

    @property
    def current_trading_date(self) -> date | None:
        return self._trading_date

    def observe(self, now: datetime) -> SessionSnapshot:
        dt = self._normalize(now)
        day = dt.date()
        trading_day = self.calendar(day)

        if not trading_day:
            previous = self._phase
            event = SessionEvent.NONE if previous is SessionPhase.NON_TRADING else SessionEvent.NON_TRADING_STARTED
            self._trading_date = None
            self._phase = SessionPhase.NON_TRADING
            return self._snapshot(None, previous, event, False, False)

        phase = self._phase_for(dt.time())
        previous_phase = self._phase
        previous_date = self._trading_date
        new_date = previous_date != day
        reset_required = new_date

        if new_date:
            event = SessionEvent.SESSION_ROLLOVER if previous_date is not None else SessionEvent.SESSION_STARTED
        elif previous_phase == SessionPhase.PRE_OPEN and phase == SessionPhase.OPEN:
            event = SessionEvent.SESSION_OPENED
            reset_required = False
        elif previous_phase == SessionPhase.OPEN and phase == SessionPhase.POST_CLOSE:
            event = SessionEvent.SESSION_CLOSED
            reset_required = False
        else:
            event = SessionEvent.NONE
            reset_required = False

        self._trading_date = day
        self._phase = phase
        return self._snapshot(day, previous_phase, event, reset_required, True)

    def reset(self) -> None:
        self._trading_date = None
        self._phase = None

    def _phase_for(self, current: time) -> SessionPhase:
        if current < self.market_open:
            return SessionPhase.PRE_OPEN
        if current < self.market_close:
            return SessionPhase.OPEN
        return SessionPhase.POST_CLOSE

    def _normalize(self, now: datetime) -> datetime:
        if now.tzinfo is None:
            raise ValueError("session timestamp must be timezone-aware")
        return now.astimezone(self.timezone)

    def _snapshot(
        self,
        trading_date: date | None,
        previous_phase: SessionPhase | None,
        event: SessionEvent,
        reset_required: bool,
        is_trading_day: bool,
    ) -> SessionSnapshot:
        return SessionSnapshot(
            trading_date=trading_date,
            session_id=trading_date.isoformat() if trading_date else None,
            phase=self._phase or SessionPhase.NON_TRADING,
            previous_phase=previous_phase,
            event=event,
            reset_required=reset_required,
            is_trading_day=is_trading_day,
            market_open=self.market_open,
            market_close=self.market_close,
        )
