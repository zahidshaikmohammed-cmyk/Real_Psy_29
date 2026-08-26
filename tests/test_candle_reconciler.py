from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from psy29.candle_history import Candle
from psy29.candle_reconciler import CandleIntegrityError, reconcile_candles

IST = ZoneInfo("Asia/Kolkata")


def c(minute, close=100):
    ts = datetime(2026, 8, 26, 9, minute, tzinfo=IST)
    return Candle(ts, close, close + 1, close - 1, close, 100)


def test_deduplicates_and_orders():
    result = reconcile_candles((c(17),), (c(15), c(16), c(17)), interval_minutes=1)
    assert [x.timestamp.minute for x in result.candles] == [15, 16, 17]
    assert result.duplicate_count == 1


def test_detects_missing_candle():
    result = reconcile_candles((c(15), c(17)), (), interval_minutes=1)
    assert result.missing_timestamps == (c(16).timestamp,)


def test_recovers_missing_candle():
    result = reconcile_candles(
        (c(15), c(17)), (), interval_minutes=1,
        fetch_missing=lambda _start, _end: (c(16),),
    )
    assert result.missing_timestamps == ()
    assert [x.timestamp.minute for x in result.candles] == [15, 16, 17]


def test_rejects_invalid_ohlc():
    bad = Candle(c(15).timestamp, 100, 99, 98, 100, 100)
    with pytest.raises(CandleIntegrityError):
        reconcile_candles((bad,), (), interval_minutes=1)


def test_rejects_conflicting_duplicate_candle():
    conflicting = Candle(c(15).timestamp, 101, 102, 100, 101, 100)
    with pytest.raises(CandleIntegrityError, match="Conflicting candle"):
        reconcile_candles((c(15),), (conflicting,), interval_minutes=1)
