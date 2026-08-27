import copy
import json

from psy29.execution_master_builder import build_execution_master


def _stock(i):
    base = 100 + i
    candles = []
    for n in range(25):
        candles.append({
            "timestamp": f"2026-08-27T09:{15+n:02d}:00+05:30",
            "open": base+n,
            "high": base+n+2,
            "low": base+n-2,
            "close": base+n+1,
            "volume": 1000+n,
        })
    return {
        "last_tick": base + 30,
        "last_tick_timestamp": "2026-08-27T10:00:00+05:30",
        "candles": {"1m": candles, "5m": candles[:5], "15m": candles[:2], "1h": []},
        "ohlc": {"open": base, "high": base+30, "low": base-2, "close": base+30},
        "opening_range": {"status": "FORMING", "high": base+5, "low": base},
        "diagnostic": {"data_safe": True},
    }


def _master():
    return {
        "trading_date": "2026-08-27",
        "timestamp": "2026-08-27T10:00:00+05:30",
        "stocks": {f"S{i:02d}": _stock(i) for i in range(29)},
    }


def test_source_payload_remains_unchanged_and_enrichment_is_additive():
    source = _master()
    before = copy.deepcopy(source)
    result = build_execution_master(source, generated_at="2026-08-27T10:00:01+05:30")
    assert source == before
    assert "execution_enrichment" not in source
    assert result["stocks"] == source["stocks"]
    assert result["execution_enrichment"]["schema_version"] == "1.0"


def test_all_29_stocks_and_existing_fields_preserved():
    result = build_execution_master(_master())
    assert len(result["execution_enrichment"]["stocks"]) == 29
    for symbol in result["stocks"]:
        assert result["execution_enrichment"]["stocks"][symbol]


def test_missing_last_tick_and_market_context_are_honest():
    source = _master()
    source["stocks"]["S00"]["last_tick"] = None
    source["stocks"]["S00"]["last_tick_timestamp"] = None
    result = build_execution_master(source)
    assert result["execution_enrichment"]["stocks"]["S00"]["last_tick_metadata"]["last_tick"] is None
    assert result["execution_enrichment"]["stocks"]["S00"]["last_tick_metadata"]["status"] == "UNAVAILABLE"
    assert result["execution_enrichment"]["market_context"]["NIFTY"]["status"] == "UNAVAILABLE"
    assert result["execution_enrichment"]["market_context"]["INDIA_VIX"]["value"] is None


def test_sector_live_data_unavailable_and_no_fake_depth():
    source = _master()
    source["sector_mapping"] = {"S00": "TEST_SECTOR"}
    result = build_execution_master(source)
    sector = result["execution_enrichment"]["sectors"]["items"]["S00"]
    assert sector["sector"] == "TEST_SECTOR"
    assert sector["sector_index"] is None
    liq = result["execution_enrichment"]["stocks"]["S00"]["liquidity"]
    assert liq["bid"] is None and liq["ask"] is None
    assert liq["bid_quantity"] is None and liq["ask_quantity"] is None
    assert liq["imbalance"] is None


def test_completed_candle_detection_is_deterministic_and_forming_not_completed():
    source = _master()
    source["stocks"]["S00"]["candles"]["1m"] = [
        {"timestamp": "2026-08-27T09:59:00+05:30", "close": 1},
        {"timestamp": "2026-08-27T10:00:00+05:30", "close": 2},
    ]
    result = build_execution_master(source)
    c = result["execution_enrichment"]["stocks"]["S00"]["completed_candles"]["1m"]
    assert c["latest_completed_timestamp"] == "2026-08-27T09:59:00+05:30"
    assert c["candle_complete"] is True


def test_indicators_use_only_source_candles():
    source = _master()
    source["stocks"]["S00"]["candles"]["1m"] = [
        {"timestamp": f"2026-08-27T09:{15+i:02d}:00+05:30", "high": 10+i, "low": 8+i, "close": 9+i, "volume": 100}
        for i in range(25)
    ]
    result = build_execution_master(source)
    ind = result["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]
    assert ind["EMA9"]["status"] == "CALCULATED"
    assert ind["EMA20"]["status"] == "CALCULATED"


def test_support_resistance_is_deterministic():
    source = _master()
    rows = [
        {"timestamp": "2026-08-27T09:15:00+05:30", "high": 10, "low": 5},
        {"timestamp": "2026-08-27T09:16:00+05:30", "high": 12, "low": 6},
        {"timestamp": "2026-08-27T09:17:00+05:30", "high": 11, "low": 7},
    ]
    source["stocks"]["S00"]["candles"]["1m"] = rows
    a = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["support_resistance"]
    b = build_execution_master(source)["execution_enrichment"]["stocks"]["S00"]["support_resistance"]
    assert a == b
    assert 12.0 in a["resistances"]


def test_candle_gaps_reported_and_not_repaired():
    source = _master()
    source["stocks"]["S00"]["candles"]["1m"] = [
        {"timestamp": "2026-08-27T09:15:00+05:30", "close": 1},
        {"timestamp": "2026-08-27T09:17:00+05:30", "close": 3},
    ]
    result = build_execution_master(source)
    integrity = result["execution_enrichment"]["stocks"]["S00"]["candle_integrity"]["1m"]
    assert integrity["status"] == "GAP"
    assert integrity["count"] == 2
    assert [r["timestamp"] for r in source["stocks"]["S00"]["candles"]["1m"]] == [
        "2026-08-27T09:15:00+05:30", "2026-08-27T09:17:00+05:30"
    ]


def test_stale_timestamps_are_preserved_and_age_is_calculated():
    source = _master()
    source["stocks"]["S00"]["last_tick_timestamp"] = "2026-08-27T09:30:00+05:30"
    result = build_execution_master(source)
    age = result["execution_enrichment"]["stocks"]["S00"]["data_age"]
    assert age["seconds"] == 1800.0
    assert source["stocks"]["S00"]["last_tick_timestamp"] == "2026-08-27T09:30:00+05:30"


def test_opening_range_enrichment_does_not_modify_original():
    source = _master()
    before = copy.deepcopy(source["stocks"]["S00"]["opening_range"])
    result = build_execution_master(source)
    enriched = result["execution_enrichment"]["stocks"]["S00"]["opening_range"]
    assert enriched["status"] == "COMPLETE"
    assert source["stocks"]["S00"]["opening_range"] == before


def test_execution_quality_does_not_change_diagnostic_data_safe():
    source = _master()
    source["stocks"]["S00"]["diagnostic"]["data_safe"] = False
    result = build_execution_master(source)
    assert result["stocks"]["S00"]["diagnostic"]["data_safe"] is False
    assert result["execution_enrichment"]["stocks"]["S00"]["execution_quality"]["status"] != "AVAILABLE"


def test_invalid_or_unsafe_source_never_becomes_safe():
    source = {"stocks": {"S00": {"diagnostic": {"data_safe": False}, "candles": {}}}}
    result = build_execution_master(source)
    assert result["stocks"]["S00"]["diagnostic"]["data_safe"] is False


def test_empty_partial_stock_data_is_honest():
    result = build_execution_master({"stocks": {"EMPTY": {}}})
    stock = result["execution_enrichment"]["stocks"]["EMPTY"]
    assert stock["last_tick_metadata"]["status"] == "UNAVAILABLE"
    assert stock["data_age"]["status"] == "UNAVAILABLE"
    assert stock["liquidity"]["status"] == "UNAVAILABLE"
    assert stock["candle_integrity"]["1m"]["status"] == "UNAVAILABLE"


def test_builder_has_no_network_dependency():
    result = build_execution_master({"stocks": {}})
    assert result["execution_enrichment"]["stocks"] == {}


def test_file_path_input_works(tmp_path):
    source = _master()
    path = tmp_path / "master.json"
    path.write_text(json.dumps(source), encoding="utf-8")
    result = build_execution_master(path)
    assert result["trading_date"] == source["trading_date"]
    assert len(result["execution_enrichment"]["stocks"]) == 29


def test_generated_at_is_metadata_only():
    source = _master()
    result = build_execution_master(source, generated_at="fixed")
    assert result["execution_enrichment"]["generated_at"] == "fixed"
    assert source["trading_date"] == "2026-08-27"


def test_builder_returns_deep_copy_for_nested_source_objects():
    source = _master()
    result = build_execution_master(source)
    result["stocks"]["S00"]["ohlc"]["close"] = 999999
    assert source["stocks"]["S00"]["ohlc"]["close"] != 999999


def test_insufficient_indicator_history_is_explicit():
    source = _master()
    source["stocks"]["S00"]["candles"]["1m"] = [
        {"timestamp": "2026-08-27T09:15:00+05:30", "high": 10, "low": 9, "close": 9.5, "volume": 100}
    ]
    result = build_execution_master(source)
    assert result["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]["EMA9"]["value"] is None
    assert result["execution_enrichment"]["stocks"]["S00"]["indicators"]["1m"]["EMA9"]["status"] == "INSUFFICIENT_DATA"


def test_source_opening_range_object_is_not_shared_with_output():
    source = _master()
    result = build_execution_master(source)
    result["execution_enrichment"]["stocks"]["S00"]["opening_range"]["high"] = 999
    assert source["stocks"]["S00"]["opening_range"]["high"] != 999


def test_unsafe_diagnostic_object_is_preserved_exactly():
    source = _master()
    source["stocks"]["S01"]["diagnostic"] = {"data_safe": False, "reason": "source gate"}
    before = copy.deepcopy(source["stocks"]["S01"]["diagnostic"])
    result = build_execution_master(source)
    assert result["stocks"]["S01"]["diagnostic"] == before
