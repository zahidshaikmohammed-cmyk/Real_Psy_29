from __future__ import annotations

import pytest
from pydantic import ValidationError

from psy29.normalized_api import normalize_market, normalize_stock


def stock(symbol: str = "NESTLEIND") -> dict:
    candle = {
        "timestamp": "2026-08-26T09:15:00+05:30",
        "epoch": 1787715900,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 10,
    }
    return {
        "symbol": symbol,
        "security_id": "123",
        "current_price": 100.5,
        "ohlc": {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5},
        "session_high": 101.0,
        "session_low": 99.0,
        "previous_day": {"high": 102.0, "low": 98.0, "close": 100.0},
        "volume": 10,
        "candles": {"1m": [candle], "5m": [], "15m": [], "1h": []},
        "vwap": 100.2,
        "ema9": 100.1,
        "ema20": 99.9,
        "opening_range": {"period": "09:15-09:30", "status": "FORMING", "high": 101.0, "low": 99.0},
        "structure": {"trend": "UP", "swing_high": None, "swing_low": None},
        "timestamp": "2026-08-26T09:16:00+05:30",
        "trading_date": "2026-08-26",
        "market_session_status": "OPEN",
        "data_source_status": "LIVE",
        "last_tick": "2026-08-26T09:16:00+05:30",
        "_private_cache": {"must_not": "leak"},
    }


def market() -> dict:
    stocks = {f"S{i:02d}": stock(f"S{i:02d}") for i in range(29)}
    return {
        "service": "PSY29 Live Data",
        "timestamp": "2026-08-26T09:16:00+05:30",
        "trading_date": "2026-08-26",
        "market_session_status": "OPEN",
        "data_source_status": "LIVE",
        "stocks_expected": 29,
        "stocks": stocks,
        "_internal": "must_not_leak",
    }


def test_normalizes_complete_29_stock_envelope():
    result = normalize_market(market())
    assert result["schema_version"] == "1.0"
    assert result["stocks_expected"] == 29
    assert result["stocks_loaded"] == 29
    assert len(result["stocks"]) == 29
    assert "_internal" not in result
    assert "_private_cache" not in result["stocks"]["S00"]


def test_normalization_is_json_safe_and_deterministic():
    result = normalize_market(market())
    assert result == normalize_market(result)
    assert isinstance(result["stocks"]["S00"]["candles"]["1m"][0]["epoch"], int)


def test_stock_normalization_removes_private_runtime_state():
    result = normalize_stock(stock())
    assert result["symbol"] == "NESTLEIND"
    assert "_private_cache" not in result


def test_missing_required_stock_field_is_rejected():
    bad = stock()
    del bad["security_id"]
    with pytest.raises(ValidationError):
        normalize_stock(bad)


def test_non_29_expected_count_is_rejected():
    bad = market()
    bad["stocks_expected"] = 28
    with pytest.raises(ValueError, match="stocks_expected"):
        normalize_market(bad)


def test_loaded_count_is_derived_from_actual_stock_map():
    bad = market()
    del bad["stocks"]["S28"]
    result = normalize_market(bad)
    assert result["stocks_loaded"] == 28
