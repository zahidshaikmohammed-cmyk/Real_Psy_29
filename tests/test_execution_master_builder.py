import copy
from datetime import datetime, timezone, timedelta

from psy29.execution_master_builder import build_execution_master

IST = timezone(timedelta(hours=5, minutes=30))


def candle(ts, close=100, high=101, low=99, volume=1000):
    return {"timestamp": ts, "open": close - 0.5, "high": high, "low": low, "close": close, "volume": volume}


def minute_series(start="09:15", count=30, step=1):
    h, m = map(int, start.split(":"))
    rows = []
    for i in range(count):
        total = h * 60 + m + i * step
        hh, mm = divmod(total, 60)
        rows.append(candle(f"2026-08-27T{hh:02d}:{mm:02d}:00+05:30", close=100 + i, high=101 + i, low=99 + i, volume=1000 + i))
    return rows


def master(stocks=None, timestamp="2026-08-27T10:00:00+05:30", status="LIVE", safe=True):
    if stocks is None:
        rows = minute_series()
        stocks = {
            f"S{i:02d}": {
                "current_price": 120 + i,
                "volume": 50000 + i,
                "last_tick_timestamp": timestamp,
                "candles": {"1m": rows, "5m": rows[::5], "15m": rows[::15], "1h": []},
                "diagnostic": {"data_safe": safe},
            }
            for i in range(29)
        }
    return {"trading_date": "2026-08-27", "timestamp": timestamp, "data_source_status": status, "market_session_status": "OPEN", "stocks_expected": 29, "stocks_loaded": len(stocks), "diagnostic": {"data_safe": safe}, "stocks": stocks}


def test_source_payload_preserved_and_not_mutated():
    source = master()
    before = copy.deepcopy(source)
    result = build_execution_master(source, generated_at="fixed")
    assert source == before
    assert "execution_enrichment" not in source
    assert result["stocks"] == source["stocks"]


def test_all_29_stocks_processed_and_existing_fields_preserved():
    result = build_execution_master(master())
    assert len(result["execution_enrichment"]["stocks"]) == 29
    assert result["stocks"]["S00"]["candles"] == master()["stocks"]["S00"]["candles"]


def test_current_trading_date_is_preserved():
    source = master()
    result = build_execution_master(source)
    assert result["trading_date"] == "2026-08-27"
    assert result["execution_enrichment"]["source"]["trading_date"] == "2026-08-27"


def test_yesterday_is_never_relabelled_as_today():
    source = master(timestamp="2026-08-26T10:00:00+05:30")
    source["trading_date"] = "2026-08-26"
    result = build_execution_master(source)
    assert result["trading_date"] == "2026-08-26"
    assert result["execution_enrichment"]["source"]["trading_date"] == "2026-08-26"


def test_completed_candle_detection_for_all_timeframes():
    source = master()
    rows = minute_series(count=30)
    source["stocks"]["S00"]["candles"] = {"1m": rows, "5m": rows[::5], "15m": rows[::15], "1h": [candle("2026-08-27T09:00:00+05:30")]}
    result = build_execution_master(source)
    c = result["execution_enrichment"]["stocks"]["S00"]["completed_candles"]
    assert c["1m"]["candle_complete"] is True
    assert c["5m"]["candle_complete"] is True
    assert c["15m"]["candle_complete"] is True
    assert c["1h"]["candle_complete"] is True


def test_forming_candle_is_not_completed():
    source = master(timestamp="2026-08-27T10:00:30+05:30")
    source["stocks"]["S00"]["candles"]["1m"] = [candle("2026-08-27T10:00:00+05:30")]
    result = build_execution_master(source)
    c = result["execution_enrichment"]["stocks"]["S00"]["completed_candles"]["1m"]
    assert c["candle_complete"] is False
    assert c["latest_completed_timestamp"] is None


def test_candle_gaps_are_detected_not_repaired():
    source = master()
    source["stocks"]["S00"]["candles"]["1m"] = [candle("2026-08-27T09:15:00+05:30"), candle("2026-08-27T09:17:00+05:30")]
    before = copy.deepcopy(source["stocks"]["S00"]["candles"]["1m"])
    result = build_execution_master(source)
    integrity = result["execution_enrichment"]["stocks"]["S00"]["candle_integrity"]["1m"]
    assert integrity["status"] == "GAP"
    assert integrity["count"] == 2
    assert source["stocks"]["S00"]["candles"]["1m"] == before


def test_vwap_is_calculated_from_source_candles_only():
    source = master()
    source["stocks"]["S00"]["candles"]["1m"] = [candle("2026-08-27T09:15:00+05:30", close=10, high=11, low=9, volume=100), candle("2026-08-27T09:16:00+05:30", close=20, high=21, low=19, volume=100)]
    source["timestamp"] = "2026-08-27T09:18:00+05:30"
    v = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]["VWAP"]
    assert v["status"] == "CALCULATED"
    assert v["value"] == 15.0


def test_ema9_and_ema20_are_calculated_only_with_enough_completed_candles():
    source = master()
    rows = minute_series(count=25)
    source["stocks"]["S00"]["candles"]["1m"] = rows
    result = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]
    assert result["EMA9"]["status"] == "CALCULATED"
    assert result["EMA20"]["status"] == "CALCULATED"


def test_insufficient_indicator_history_is_explicit():
    source = master()
    source["stocks"]["S00"]["candles"]["1m"] = [candle("2026-08-27T09:15:00+05:30")]
    result = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]
    assert result["EMA9"]["value"] is None
    assert result["EMA9"]["status"] == "INSUFFICIENT_DATA"


def test_opening_range_is_derived_from_0915_to_0930_candles():
    source = master(timestamp="2026-08-27T09:45:00+05:30")
    source["stocks"]["S00"]["candles"]["1m"] = minute_series(count=30)
    source["stocks"]["S00"].pop("opening_range", None)
    result = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["opening_range"]
    assert result["period_start"] == "2026-08-27T09:15:00+05:30"
    assert result["period_end"] == "2026-08-27T09:30:00+05:30"
    assert result["status"] == "COMPLETE"
    assert result["high"] == 115
    assert result["low"] == 99
    assert result["range"] == 16


def test_opening_range_does_not_modify_source_field():
    source = master()
    source["stocks"]["S00"]["opening_range"] = {"status": "FORMING", "high": 999, "low": 1}
    before = copy.deepcopy(source["stocks"]["S00"]["opening_range"])
    build_execution_master(source)
    assert source["stocks"]["S00"]["opening_range"] == before


def test_support_resistance_is_deterministic():
    source = master()
    source["stocks"]["S00"]["candles"]["1m"] = [
        candle("2026-08-27T09:15:00+05:30", high=10, low=5),
        candle("2026-08-27T09:16:00+05:30", high=12, low=6),
        candle("2026-08-27T09:17:00+05:30", high=11, low=7),
    ]
    a = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["support_resistance"]
    b = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["support_resistance"]
    assert a == b
    assert 12.0 in a["resistances"]


def test_execution_quality_is_informational_and_source_safe_unchanged():
    source = master()
    source["diagnostic"]["data_safe"] = False
    result = build_execution_master(source)
    assert result["diagnostic"]["data_safe"] is False
    assert result["execution_enrichment"]["stocks"]["S00"]["execution_quality"]["status"] != "READY"


def test_missing_market_context_is_unavailable():
    result = build_execution_master(master())
    ctx = result["execution_enrichment"]["market_context"]
    assert ctx["NIFTY"] == {"status": "UNAVAILABLE", "value": None}
    assert ctx["BANKNIFTY"] == {"status": "UNAVAILABLE", "value": None}
    assert ctx["INDIA_VIX"] == {"status": "UNAVAILABLE", "value": None}


def test_missing_sector_live_data_is_unavailable():
    result = build_execution_master(master())
    item = result["execution_enrichment"]["sectors"]["items"]["S00"]
    assert item["sector_index"] is None
    assert item["sector_regime"] is None
    assert item["status"] == "UNAVAILABLE"


def test_missing_liquidity_depth_is_not_fabricated():
    result = build_execution_master(master())
    liq = result["execution_enrichment"]["stocks"]["S00"]["liquidity"]
    assert liq["bid"] is None and liq["ask"] is None
    assert liq["bid_quantity"] is None and liq["ask_quantity"] is None
    assert liq["imbalance"] is None


def test_missing_relative_strength_benchmark_is_unavailable():
    result = build_execution_master(master())
    rs = result["execution_enrichment"]["stocks"]["S00"]["relative_strength"]
    assert rs == {"value": None, "benchmark": None, "timeframe": None, "status": "UNAVAILABLE"}


def test_unsafe_source_never_becomes_live():
    source = master(status="DATA_UNSAFE", safe=False)
    result = build_execution_master(source)
    assert result["data_source_status"] == "DATA_UNSAFE"
    assert result["diagnostic"]["data_safe"] is False


def test_source_unavailable_never_becomes_live():
    source = master(status="SOURCE_UNAVAILABLE", safe=False)
    result = build_execution_master(source)
    assert result["data_source_status"] == "SOURCE_UNAVAILABLE"
    assert result["diagnostic"]["data_safe"] is False


def test_last_tick_missing_stays_unavailable():
    source = master()
    source["stocks"]["S00"].pop("last_tick_timestamp")
    source["stocks"]["S00"].pop("current_price")
    result = build_execution_master(source)
    tick = result["execution_enrichment"]["stocks"]["S00"]["last_tick_metadata"]
    assert tick["status"] == "UNAVAILABLE"
    assert tick["last_tick"] is None


def test_stale_timestamp_is_preserved():
    source = master()
    source["stocks"]["S00"]["last_tick_timestamp"] = "2026-08-27T09:30:00+05:30"
    result = build_execution_master(source)
    assert result["stocks"]["S00"]["last_tick_timestamp"] == "2026-08-27T09:30:00+05:30"
    assert result["execution_enrichment"]["stocks"]["S00"]["data_age"]["seconds"] == 1800.0


def test_generated_metadata_does_not_change_source_timestamp():
    source = master(timestamp="2026-08-27T09:45:00+05:30")
    result = build_execution_master(source, generated_at="2026-08-27T10:00:00+05:30")
    assert result["timestamp"] == "2026-08-27T09:45:00+05:30"
    assert result["execution_enrichment"]["generated_at"] == "2026-08-27T10:00:00+05:30"


def test_empty_stock_data_is_honest():
    result = build_execution_master({"stocks": {"EMPTY": {}}})
    stock = result["execution_enrichment"]["stocks"]["EMPTY"]
    assert stock["last_tick_metadata"]["status"] == "UNAVAILABLE"
    assert stock["data_age"]["status"] == "UNAVAILABLE"
    assert stock["candle_integrity"]["1m"]["status"] == "UNAVAILABLE"
    assert stock["liquidity"]["status"] == "UNAVAILABLE"


def test_builder_is_source_bounded_and_deterministic():
    source = master()
    a = build_execution_master(source, generated_at="fixed")
    b = build_execution_master(source, generated_at="fixed")
    assert a == b


def test_file_path_input_is_supported(tmp_path):
    import json
    source = master()
    path = tmp_path / "master.json"
    path.write_text(json.dumps(source), encoding="utf-8")
    result = build_execution_master(path, generated_at="fixed")
    assert len(result["execution_enrichment"]["stocks"]) == 29
