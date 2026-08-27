from datetime import datetime

import pytest

import runner
from psy29.data_integrity import DataIntegrityError


TRADING_DATE = "2026-08-27"


def row(minute, opn, high, low, close, volume=100):
    ts = datetime.fromisoformat(f"{TRADING_DATE}T09:{minute:02d}:00+05:30")
    return {
        "timestamp": ts.isoformat(),
        "epoch": int(ts.timestamp()),
        "open": opn,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def test_canonical_session_ohlc_comes_only_from_validated_1m_candles():
    candles = [
        row(15, 1900, 1910, 1895, 1905),
        row(16, 1905, 1920, 1900, 1918),
        row(17, 1918, 1919, 1890, 1895),
    ]
    assert runner._canonical_session_ohlc(candles) == {
        "open": 1900,
        "high": 1920,
        "low": 1890,
        "close": 1895,
    }


def test_canonical_session_ohlc_rejects_corrupt_candle():
    candles = [row(15, 1900, 1890, 1880, 1885)]
    with pytest.raises(DataIntegrityError, match="invalid OHLC bounds"):
        runner._canonical_session_ohlc(candles)


def test_broker_aggregate_ohlc_cannot_override_validated_candle_range(monkeypatch):
    candles = [
        row(15, 1900, 1910, 1895, 1905),
        row(16, 1905, 1920, 1900, 1918),
        row(17, 1918, 1919, 1890, 1895),
    ]
    captured = {}

    monkeypatch.setattr(runner.main, "_original_rebuild_stock", lambda symbol, one_min, quote, prev: captured.update({"symbol": symbol, "candles": one_min, "quote": quote, "prev": prev}))
    runner._validated_rebuild_stock(
        "NESTLEIND",
        candles,
        {"current": 1901, "open": 9999, "high": 9000, "low": 1, "close": 2, "volume": 1000},
        {"high": 1930, "low": 1850, "close": 1900},
    )

    assert captured["quote"]["current"] == 1901.0
    assert captured["quote"]["open"] == 1900
    assert captured["quote"]["high"] == 1920
    assert captured["quote"]["low"] == 1890
    assert captured["quote"]["close"] == 1895
