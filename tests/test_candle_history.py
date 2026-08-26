from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from psy29.candle_history import CandleHistoryError, acquire_stock_session_history
from psy29.instrument_registry import Instrument, InstrumentRegistry

IST = ZoneInfo("Asia/Kolkata")


def registry():
    return InstrumentRegistry((Instrument("NESTLEIND", "123"),))


def candle_payload():
    return {
        "open": [100, 101],
        "high": [102, 103],
        "low": [99, 100],
        "close": [101, 102],
        "volume": [1000, 1100],
        "timestamp": [
            1787735700,
            1787735760,
        ],
    }


def daily_payload():
    return {
        "open": [90],
        "high": [110],
        "low": [88],
        "close": [105],
        "volume": [5000],
        "timestamp": [1787649300],
    }


def fetcher(_security_id, _interval, _start, _end):
    return candle_payload()


def test_acquires_genuine_timeframes_and_previous_day():
    class Client:
        def intraday(self, *args):
            return fetcher(*args)

        def historical_daily(self, *args):
            return daily_payload()

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
    rows = candle_payload()
    rows["timestamp"].append(rows["timestamp"][0])
    for key in ("open", "high", "low", "close", "volume"):
        rows[key].append(rows[key][0])

    class Client:
        def intraday(self, *args):
            return rows

        def historical_daily(self, *args):
            return daily_payload()

    with pytest.raises(CandleHistoryError):
        acquire_stock_session_history(
            Client(), registry(), "NESTLEIND",
            now=datetime(2026, 8, 26, 10, 0, tzinfo=IST),
        )
