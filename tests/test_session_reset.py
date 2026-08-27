from datetime import date

from psy29.session_reset import checkpoint_is_for_trading_date, reset_for_trading_date


def _yesterday_state():
    return {
        "trading_date": "2026-08-26",
        "market_session_status": "POST_CLOSE",
        "source_status": "POST_CLOSE",
        "access_token_expiry": "old-token-expiry",
        "security_map": {"TEST": "1"},
        "stocks": {
            "TEST": {
                "candles": {"1m": [{"timestamp": "2026-08-26T09:15:00+05:30"}]},
                "ohlc": {"open": 100, "high": 110, "low": 90, "close": 105},
                "session_high": 110,
                "session_low": 90,
                "vwap": 103,
                "ema9": 104,
                "ema20": 102,
                "opening_range": {"status": "FORMED", "high": 110, "low": 90},
                "structure": {"trend": "UP"},
                "volume": 1000,
                "last_tick": "2026-08-26T15:29:00+05:30",
            }
        },
    }


def test_yesterdays_state_cannot_become_todays_live_state():
    state = _yesterday_state()
    assert reset_for_trading_date(state, date(2026, 8, 27)) is True
    assert state["trading_date"] == "2026-08-27"
    assert state["stocks"] == {}
    assert state["security_map"] == {}
    assert state["source_status"] == "WAITING_FOR_SESSION"


def test_todays_session_is_idempotent_after_reset():
    state = {"trading_date": "2026-08-27", "stocks": {"TODAY": {"candles": []}}}
    before = {k: v.copy() if isinstance(v, dict) else v for k, v in state.items()}
    assert reset_for_trading_date(state, date(2026, 8, 27)) is False
    assert state == before


def test_previous_day_checkpoint_is_not_eligible_for_today():
    yesterday = {"trading_date": "2026-08-26", "stocks": {"TEST": {}}}
    assert checkpoint_is_for_trading_date(yesterday, date(2026, 8, 27)) is False


def test_todays_checkpoint_is_eligible_only_for_same_date():
    today = {"trading_date": "2026-08-27", "stocks": {"TEST": {}}}
    assert checkpoint_is_for_trading_date(today, date(2026, 8, 27)) is True
    assert checkpoint_is_for_trading_date(today, date(2026, 8, 28)) is False


def test_reset_does_not_copy_previous_day_reference_data():
    state = _yesterday_state()
    reset_for_trading_date(state, date(2026, 8, 27))
    assert state["stocks"] == {}
    assert "previous_day" not in state


def test_reset_starts_empty_so_today_acquisition_owns_candles_and_indicators():
    state = _yesterday_state()
    reset_for_trading_date(state, date(2026, 8, 27))
    assert state["stocks"] == {}
    # No candle, OHLC, volume, timestamp, opening-range, structure, or
    # indicator state survives the trading-date boundary.
