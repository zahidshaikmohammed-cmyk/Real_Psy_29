from copy import deepcopy

from psy29.execution_schema import enrich_market_payload


def candle(ts, o, h, l, c, v=100):
    return {"timestamp": ts, "epoch": 1, "open": o, "high": h, "low": l, "close": c, "volume": v}


def payload():
    rows = [
        candle("2026-08-26T09:15:00+05:30", 100, 101, 99, 100.5),
        candle("2026-08-26T09:16:00+05:30", 100.5, 102, 100, 101),
        candle("2026-08-26T09:17:00+05:30", 101, 103, 100.5, 102),
    ]
    return {
        "service": "PSY29 Live Data", "timestamp": "2026-08-26T09:18:00+05:30",
        "trading_date": "2026-08-26", "market_session_status": "OPEN",
        "data_source_status": "LIVE", "stocks_expected": 29, "stocks_loaded": 1,
        "stocks": {"TEST": {
            "symbol": "TEST", "security_id": "1", "current_price": 102,
            "ohlc": {"open": 100, "high": 103, "low": 99, "close": 102},
            "session_high": 103, "session_low": 99,
            "previous_day": {"high": 105, "low": 95, "close": 100}, "volume": 300,
            "candles": {"1m": rows, "5m": [], "15m": [], "1h": []},
            "vwap": 101, "ema9": 101, "ema20": 100,
            "opening_range": {"period": "09:15-09:30", "status": "FORMING", "high": 103, "low": 99},
            "structure": {"trend": "UP", "swing_high": None, "swing_low": None},
            "timestamp": "2026-08-26T09:18:00+05:30", "trading_date": "2026-08-26",
            "market_session_status": "OPEN", "data_source_status": "LIVE",
            "last_tick": "2026-08-26T09:18:00+05:30",
        }},
    }


def test_original_payload_is_not_mutated_and_fields_unchanged():
    original = payload(); before = deepcopy(original); result = enrich_market_payload(original)
    assert original == before
    assert result["stocks"] == before["stocks"]
    assert "execution_enrichment" in result


def test_completed_candle_detection_works():
    result = enrich_market_payload(payload())
    e = result["execution_enrichment"]["stocks"]["TEST"]
    assert e["completed_candles"]["1m"]["count"] == 2
    assert e["completed_candles"]["1m"]["last_timestamp"] == "2026-08-26T09:16:00+05:30"


def test_candle_gaps_are_detected():
    p = payload(); p["stocks"]["TEST"]["candles"]["1m"].append(candle("2026-08-26T09:20:00+05:30", 102, 104, 101, 103))
    result = enrich_market_payload(p)
    gaps = result["execution_enrichment"]["stocks"]["TEST"]["candle_integrity"]["1m"]["gaps"]
    assert len(gaps) == 1 and gaps[0]["minutes"] == 3


def test_timeframe_indicators_are_calculated_from_candles():
    indicators = enrich_market_payload(payload())["execution_enrichment"]["stocks"]["TEST"]["timeframe_indicators"]["1m"]
    assert indicators["ema9"] is not None and indicators["ema20"] is not None and indicators["vwap"] is not None


def test_support_resistance_is_deterministic():
    levels = enrich_market_payload(payload())["execution_enrichment"]["stocks"]["TEST"]["support_resistance"]["1m"]
    assert levels == {"support": 99.0, "resistance": 103.0, "method": "candle_extrema"}


def test_unavailable_data_remains_null_or_unavailable():
    p = payload(); p["stocks"]["TEST"]["candles"] = {"1m": [], "5m": [], "15m": [], "1h": []}; p["stocks"]["TEST"]["timestamp"] = None
    e = enrich_market_payload(p)["execution_enrichment"]["stocks"]["TEST"]
    assert e["data_age_seconds"] is None
    assert e["execution_quality"]["status"] == "UNAVAILABLE"
    assert e["support_resistance"]["1m"]["method"] == "UNAVAILABLE"
    assert e["timeframe_indicators"]["1m"]["ema9"] is None


def test_opening_range_status_is_derived_without_rewriting_existing_state():
    p = payload(); p["stocks"]["TEST"]["opening_range"] = {"period": "09:15-09:30", "status": "FORMED", "high": 103, "low": 99}
    result = enrich_market_payload(p)
    assert result["execution_enrichment"]["stocks"]["TEST"]["opening_range"]["status"] == "FORMED"
    assert p["stocks"]["TEST"]["opening_range"]["status"] == "FORMED"


def test_stale_timestamps_are_not_modified():
    p = payload(); p["timestamp"] = "2026-08-26T12:00:00+05:30"
    p["stocks"]["TEST"]["timestamp"] = "2026-08-26T09:18:00+05:30"
    result = enrich_market_payload(p)
    assert p["timestamp"] == "2026-08-26T12:00:00+05:30"
    assert p["stocks"]["TEST"]["timestamp"] == "2026-08-26T09:18:00+05:30"
    assert result["execution_enrichment"]["stocks"]["TEST"]["data_age_seconds"] == 9720.0


def test_no_fabricated_values_for_missing_timeframes():
    result = enrich_market_payload(payload()); e = result["execution_enrichment"]["stocks"]["TEST"]
    for tf in ("5m", "15m", "1h"):
        assert e["completed_candles"][tf]["count"] == 0
        assert e["support_resistance"][tf]["support"] is None and e["support_resistance"][tf]["resistance"] is None
        assert e["timeframe_indicators"][tf]["ema9"] is None and e["timeframe_indicators"][tf]["ema20"] is None and e["timeframe_indicators"][tf]["vwap"] is None
