from datetime import datetime

import pytest

from psy29.data_integrity import DataIntegrityError, validate_intraday_rows, validate_quote, validate_tick


TRADING_DATE = "2026-08-27"


def candle(ts="2026-08-27T09:15:00+05:30", epoch=1787795100, price=1900.0):
    return {"timestamp": ts, "epoch": epoch, "open": price, "high": price + 2, "low": price - 2, "close": price + 1, "volume": 100}


def test_rejects_foreign_year_candle():
    row = candle(ts="2035-08-27T09:15:00+05:30", epoch=2061171900)
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([row], TRADING_DATE)


def test_rejects_absurd_float_candle():
    row = candle(price=2.314896e-36)
    row["high"] = 2.314896e-36
    row["low"] = 2.314896e-36
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([row], TRADING_DATE)


def test_rejects_out_of_order_duplicate_candles():
    first = candle()
    second = candle(ts="2026-08-27T09:14:00+05:30", epoch=1787795040)
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([first, second], TRADING_DATE)


def test_rejects_quote_current_outside_day_range():
    with pytest.raises(DataIntegrityError):
        validate_quote({"current": 0.0001, "open": 1900, "high": 1910, "low": 1890, "close": 1880, "volume": 1000})


def test_rejects_future_live_tick():
    now = datetime.fromisoformat("2026-08-27T10:00:00+05:30")
    future_epoch = int(now.timestamp()) + 30
    with pytest.raises(DataIntegrityError):
        validate_tick(1900, 1000, future_epoch, 1890, 1910, 1880, now)


def test_accepts_valid_current_session_data():
    rows = validate_intraday_rows([candle()], TRADING_DATE)
    assert rows[0]["close"] == 1901.0
    quote = validate_quote({"current": 1901, "open": 1890, "high": 1910, "low": 1880, "close": 1875, "volume": 1000})
    assert quote["current"] == 1901
