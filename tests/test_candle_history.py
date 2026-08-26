from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from psy29.candle_history import CandleHistoryError, acquire_stock_session_history
from psy29.instrument_registry import Instrument, InstrumentRegistry

IST = ZoneInfo("Asia/Kolkata")


def registry():
    return InstrumentRegistry((Instrument("NESTLEIND", "123"),))


def candle_rows():
    return [
        {"timestamp": "2026-08-26T09:15:00+05:30", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
        {"timestamp": "2026-08-26T09:16:00+05:30", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1100},
    ]


def fetcher(_security_id, interval, _start, _end):
    return candle_rows()


def test_acquires_genuine_timeframes_and_previous_day(monkeypatch):
    class Client:
        def intraday_candles(self, *args):
            return fetcher(*args)

        def daily_history(self, *args):
            return [
                {"timestamp": "2026-08-25T09:15:00+05:30", "open": 90, "high": 110, "low": 88, "close": 105, "volume": 5000}
            ]

    history = acquire_stock_session_history(
        Client(), registry(), "NESTLEIND",
        now=datetime(2026, 8, 26, 10, 0, tzinfo=IST),
    )
    assert len(history.candles_1m) == 2
    assert len(history.candles_5m) == 2
    assert len(history.candles_15m) == 2
    assert len(history.candles_1h) == 2
    assert history.previous_day_high == 110
    assert history.previous_day_low == 88
    assert history.previous_day_close == 105


def test_rejects_duplicate_timestamps():
    rows = candle_rows() + [candle_rows()[0]]

    class Client:
        def intraday_candles(self, *args):
            return rows

        def daily_history(self, *args):
            return []

    with pytest.raises(CandleHistoryError):
        acquire_stock_session_history(
            Client(), registry(), "NESTLEIND",
            now=datetime(2026, 8, 26, 10, 0, tzinfo=IST),
        )
