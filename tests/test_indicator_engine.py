from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from psy29.candle_history import Candle, StockSessionHistory
from psy29.indicator_engine import IndicatorError, LiveIndicatorEngine, calculate_indicators

IST = ZoneInfo("Asia/Kolkata")


def candles(count=50, *, start_minute=15):
    base = datetime(2026, 8, 26, 9, start_minute, tzinfo=IST)
    rows = []
    for i in range(count):
        close = 100.0 + i * 0.5
        rows.append(
            Candle(
                timestamp=base + timedelta(minutes=i),
                open=close - 0.2,
                high=close + 0.8,
                low=close - 0.8,
                close=close,
                volume=1000 + i * 10,
            )
        )
    return tuple(rows)


def test_calculates_core_indicators_after_required_warmup():
    result = calculate_indicators(candles())
    assert len(result) == 50
    assert result[7].ema9 is None
    assert result[8].ema9 is not None
    assert result[18].ema20 is None
    assert result[19].ema20 is not None
    assert result[13].rsi14 is not None
    assert result[14].atr14 is not None
    assert result[33].macd_signal is not None
    assert result[19].volume_sma20 is not None
    assert result[-1].vwap is not None


def test_rising_series_has_strong_rsi_and_positive_macd():
    point = calculate_indicators(candles())[-1]
    assert point.rsi14 == pytest.approx(100.0)
    assert point.macd is not None and point.macd > 0
    assert point.macd_histogram is not None


def test_vwap_is_volume_weighted_typical_price():
    rows = (
        Candle(datetime(2026, 8, 26, 9, 15, tzinfo=IST), 9, 11, 9, 10, 100),
        Candle(datetime(2026, 8, 26, 9, 16, tzinfo=IST), 19, 21, 19, 20, 300),
    )
    result = calculate_indicators(rows)
    expected = ((10 * 100) + (20 * 300)) / 400
    assert result[-1].vwap == pytest.approx(expected)


def test_rejects_non_chronological_input():
    rows = list(candles(3))
    rows[2], rows[1] = rows[1], rows[2]
    with pytest.raises(IndicatorError):
        calculate_indicators(rows)


def test_rejects_invalid_timeframe():
    rows = candles()
    history = StockSessionHistory(
        symbol="NESTLEIND",
        trading_date=rows[0].timestamp.date(),
        previous_day_high=None,
        previous_day_low=None,
        previous_day_close=None,
        candles_1m=rows,
        candles_5m=rows,
        candles_15m=rows,
        candles_1h=rows,
        previous_daily_candles=rows,
    )
    engine = LiveIndicatorEngine(history)
    with pytest.raises(IndicatorError):
        engine.snapshot("2m")


def test_snapshot_and_all_snapshots_use_latest_point():
    rows = candles()
    history = StockSessionHistory(
        symbol="NESTLEIND",
        trading_date=rows[0].timestamp.date(),
        previous_day_high=None,
        previous_day_low=None,
        previous_day_close=None,
        candles_1m=rows,
        candles_5m=rows,
        candles_15m=rows,
        candles_1h=rows,
        previous_daily_candles=rows,
    )
    engine = LiveIndicatorEngine(history)
    snapshot = engine.snapshot("5m")
    assert snapshot.symbol == "NESTLEIND"
    assert snapshot.timeframe == "5m"
    assert snapshot.candle_count == 50
    assert snapshot.timestamp == rows[-1].timestamp
    assert snapshot.close == rows[-1].close
    assert set(engine.all_snapshots()) == {"1m", "5m", "15m", "1h"}
