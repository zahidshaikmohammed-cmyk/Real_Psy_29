from datetime import datetime, time
from zoneinfo import ZoneInfo

import pytest

from psy29.session_lifecycle import (
    SessionEvent,
    SessionLifecycleController,
    SessionPhase,
)

IST = ZoneInfo("Asia/Kolkata")


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=IST)


def test_pre_open_starts_a_new_session_and_requires_reset():
    c = SessionLifecycleController()
    result = c.observe(dt("2026-08-26 09:00:00"))
    assert result.phase is SessionPhase.PRE_OPEN
    assert result.event is SessionEvent.SESSION_STARTED
    assert result.reset_required is True
    assert result.session_id == "2026-08-26"


def test_pre_open_is_idempotent_until_market_opens():
    c = SessionLifecycleController()
    c.observe(dt("2026-08-26 09:00:00"))
    result = c.observe(dt("2026-08-26 09:10:00"))
    assert result.phase is SessionPhase.PRE_OPEN
    assert result.event is SessionEvent.NONE
    assert result.reset_required is False


def test_exact_open_boundary_transitions_to_open():
    c = SessionLifecycleController()
    c.observe(dt("2026-08-26 09:00:00"))
    result = c.observe(dt("2026-08-26 09:15:00"))
    assert result.phase is SessionPhase.OPEN
    assert result.previous_phase is SessionPhase.PRE_OPEN
    assert result.event is SessionEvent.SESSION_OPENED
    assert result.reset_required is False


def test_exact_close_boundary_transitions_to_post_close():
    c = SessionLifecycleController()
    c.observe(dt("2026-08-26 09:15:00"))
    result = c.observe(dt("2026-08-26 15:30:00"))
    assert result.phase is SessionPhase.POST_CLOSE
    assert result.previous_phase is SessionPhase.OPEN
    assert result.event is SessionEvent.SESSION_CLOSED


def test_weekend_is_non_trading_and_has_no_session_id():
    c = SessionLifecycleController()
    result = c.observe(dt("2026-08-29 11:00:00"))
    assert result.phase is SessionPhase.NON_TRADING
    assert result.event is SessionEvent.NON_TRADING_STARTED
    assert result.is_trading_day is False
    assert result.session_id is None
    assert result.reset_required is False


def test_custom_calendar_rejects_exchange_holiday():
    holiday = {__import__("datetime").date(2026, 8, 27)}
    c = SessionLifecycleController(calendar=lambda day: day.weekday() < 5 and day not in holiday)
    result = c.observe(dt("2026-08-27 10:00:00"))
    assert result.phase is SessionPhase.NON_TRADING
    assert result.session_id is None


def test_next_trading_day_rolls_session_and_requires_reset_once():
    c = SessionLifecycleController()
    c.observe(dt("2026-08-26 15:30:00"))
    result = c.observe(dt("2026-08-27 09:00:00"))
    assert result.phase is SessionPhase.PRE_OPEN
    assert result.event is SessionEvent.SESSION_ROLLOVER
    assert result.reset_required is True
    again = c.observe(dt("2026-08-27 09:05:00"))
    assert again.event is SessionEvent.NONE
    assert again.reset_required is False


def test_same_session_date_does_not_create_duplicate_reset_after_restart_in_open():
    c = SessionLifecycleController()
    result = c.observe(dt("2026-08-26 10:00:00"))
    assert result.event is SessionEvent.SESSION_STARTED
    assert result.reset_required is True
    again = c.observe(dt("2026-08-26 10:01:00"))
    assert again.event is SessionEvent.NONE
    assert again.reset_required is False


def test_timezone_is_normalized_to_ist_before_classification():
    c = SessionLifecycleController()
    utc = ZoneInfo("UTC")
    result = c.observe(datetime(2026, 8, 26, 3, 45, tzinfo=utc))
    assert result.phase is SessionPhase.OPEN


def test_naive_timestamp_is_rejected():
    c = SessionLifecycleController()
    with pytest.raises(ValueError, match="timezone-aware"):
        c.observe(datetime(2026, 8, 26, 9, 15))


def test_invalid_market_window_is_rejected():
    with pytest.raises(ValueError, match="market_open"):
        SessionLifecycleController(market_open=time(15, 30), market_close=time(9, 15))


def test_reset_clears_lifecycle_state():
    c = SessionLifecycleController()
    c.observe(dt("2026-08-26 09:00:00"))
    c.reset()
    assert c.current_phase is None
    assert c.current_trading_date is None
