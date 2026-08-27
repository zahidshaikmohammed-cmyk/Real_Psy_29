from __future__ import annotations

import copy
import json
from unittest.mock import Mock, patch

import requests
from fastapi.testclient import TestClient

import execution_service.app as service
from execution_service.app import app

client = TestClient(app)


def source_payload(safe=True, count=29):
    stocks = {f"S{i:02d}": {"candles": {"1m": [], "5m": [], "15m": [], "1h": []}, "diagnostic": {"data_safe": safe}, "last_tick": 100 + i, "last_tick_timestamp": "2026-08-27T10:00:00+05:30"} for i in range(count)}
    return {"trading_date": "2026-08-27", "timestamp": "2026-08-27T10:00:00+05:30", "data_source_status": "LIVE" if safe else "DATA_UNSAFE", "stocks_expected": 29, "stocks_loaded": count, "diagnostic": {"data_safe": safe}, "stocks": stocks}


def reset():
    service._cache.update(payload=None, fetched_at=None, error_code=None, error_message=None)


def response_for(payload):
    response = Mock(); response.raise_for_status.return_value = None; response.json.return_value = payload; return response


def test_health_endpoint():
    assert client.get("/health").json() == {"service": "PSY29 Execution Enrichment", "status": "OK"}


def test_status_endpoint():
    reset(); assert client.get("/status").json()["source_stocks_expected"] == 29


def test_valid_source_invokes_builder_and_returns_json():
    reset(); source = source_payload()
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: {**copy.deepcopy(p), "execution_enrichment": {"ok": True}}) as builder:
        r = client.get("/execution.json")
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/json") and r.json()["execution_enrichment"] == {"ok": True}
    builder.assert_called_once()


def test_invalid_json_source():
    reset(); response = Mock(); response.raise_for_status.return_value = None; response.json.side_effect = ValueError("bad json")
    with patch("execution_service.app.requests.get", return_value=response):
        r = client.get("/execution.json")
    assert r.status_code == 503 and r.json()["error_code"] == "SOURCE_INVALID_JSON"


def test_unavailable_source_does_not_crash_health():
    reset()
    with patch("execution_service.app.requests.get", side_effect=requests.RequestException("down")):
        assert client.get("/execution.json").status_code == 503
        assert client.get("/health").status_code == 200


def test_unsafe_source_is_blocked_without_builder():
    reset(); source = source_payload(False); before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master") as builder:
        r = client.get("/execution.json")
    assert r.json()["enrichment_status"] == "BLOCKED_SOURCE_UNSAFE" and r.json()["execution_enrichment"] is None
    builder.assert_not_called(); assert source == before


def test_29_stock_source_and_source_preservation():
    reset(); source = source_payload(); before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: {**copy.deepcopy(p), "execution_enrichment": {"stocks": {}}}):
        result = client.get("/execution.json").json()
    assert len(result["stocks"]) == 29 and source == before


def test_partial_source_is_blocked():
    reset(); source = source_payload(count=28)
    with patch("execution_service.app.requests.get", return_value=response_for(source)):
        r = client.get("/execution.json")
    assert r.json()["enrichment_status"] == "BLOCKED_SOURCE_UNSAFE" and r.json()["execution_enrichment"] is None


def test_cache_busting_read_only_source():
    reset(); source = source_payload()
    with patch("execution_service.app.requests.get", return_value=response_for(source)) as get:
        with patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
            client.get("/execution.json")
    args, kwargs = get.call_args
    assert args[0] == service.SOURCE_URL and "t" in kwargs["params"] and kwargs["headers"]["Cache-Control"] == "no-cache"


def test_stale_timestamp_is_preserved():
    reset(); source = source_payload(); source["timestamp"] = "2026-08-27T09:15:00+05:30"
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        result = client.get("/execution.json").json()
    assert result["timestamp"] == "2026-08-27T09:15:00+05:30"


def test_no_candle_repair():
    reset(); source = source_payload(); source["stocks"]["S00"]["candles"]["1m"] = [{"timestamp": "2026-08-27T09:15:00+05:30"}, {"timestamp": "2026-08-27T09:17:00+05:30"}]; before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    assert source == before


def test_no_fake_depth():
    reset(); source = source_payload()
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        result = client.get("/execution.json").json()
    assert result["stocks"]["S00"].get("bid") is None


def test_status_after_fetch():
    reset(); source = source_payload()
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    body = client.get("/status").json()
    assert body["source_trading_date"] == source["trading_date"] and body["source_status"] == "LIVE" and body["source_stocks_loaded"] == 29 and body["enrichment_status"] == "AVAILABLE"


def test_root_endpoint():
    body = client.get("/").json(); assert body["service"] == "PSY29 Execution Enrichment" and "/execution.json" in body["endpoints"]
