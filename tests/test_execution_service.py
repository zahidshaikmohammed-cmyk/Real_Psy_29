from __future__ import annotations

import copy
import threading
import time
from datetime import datetime
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
    with service._cache_lock:
        service._cache.update(payload=None, fetched_at_monotonic=None, last_successful_fetch=None, error_code=None, error_message=None)
        service._fetch_inflight = None


def response_for(payload):
    response = Mock(); response.raise_for_status.return_value = None; response.json.return_value = payload; return response


def test_health_endpoint():
    assert client.get("/health").json() == {"service": "PSY29 Execution Enrichment", "status": "OK"}


def test_status_endpoint():
    reset()
    with patch("execution_service.app.requests.get", side_effect=requests.RequestException("down")):
        body = client.get("/status").json()
    assert body["source_stocks_expected"] == 29
    assert body["enrichment_status"] == "SOURCE_UNAVAILABLE"


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


def test_status_after_fetch_has_human_readable_timestamp():
    reset(); source = source_payload()
    with patch("execution_service.app.requests.get", return_value=response_for(source)), patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    body = client.get("/status").json()
    datetime.fromisoformat(body["last_source_fetch"])
    assert body["source_trading_date"] == source["trading_date"] and body["source_status"] == "LIVE" and body["source_stocks_loaded"] == 29 and body["enrichment_status"] == "AVAILABLE"


def test_root_endpoint():
    body = client.get("/").json(); assert body["service"] == "PSY29 Execution Enrichment" and "/execution.json" in body["endpoints"]


def test_single_flight_concurrent_requests_share_one_source_fetch():
    reset(); source = source_payload(); started = threading.Event(); release = threading.Event(); calls = 0; calls_lock = threading.Lock(); results = []
    def slow_fetch():
        nonlocal calls
        with calls_lock: calls += 1
        started.set(); release.wait(timeout=3)
        return source, None, None
    def request(): results.append(service._get_cached_or_fetch())
    with patch("execution_service.app._fetch_source", side_effect=slow_fetch):
        t1 = threading.Thread(target=request); t2 = threading.Thread(target=request)
        t1.start(); assert started.wait(timeout=1); t2.start(); time.sleep(0.05); assert calls == 1; release.set(); t1.join(3); t2.join(3)
    assert calls == 1 and len(results) == 2 and results[0][0] == source and results[1][0] == source


def test_cache_prevents_duplicate_fetches_within_ttl():
    reset(); source = source_payload()
    with patch("execution_service.app._fetch_source", return_value=(source, None, None)) as fetch:
        assert service._get_cached_or_fetch()[0] == source
        assert service._get_cached_or_fetch()[0] == source
    fetch.assert_called_once()


def test_expired_cache_performs_new_fetch():
    reset(); source = source_payload(); clock = {"now": 100.0}
    with patch("execution_service.app.time.monotonic", side_effect=lambda: clock["now"]), patch("execution_service.app._fetch_source", return_value=(source, None, None)) as fetch:
        service._get_cached_or_fetch(); clock["now"] += service.REFRESH_SECONDS + 0.001; service._get_cached_or_fetch()
    assert fetch.call_count == 2


def test_429_is_not_retried_within_failure_ttl():
    reset()
    with patch("execution_service.app._fetch_source", return_value=(None, "SOURCE_FETCH_FAILED", "429 Too Many Requests")) as fetch:
        first = service._get_cached_or_fetch(); second = service._get_cached_or_fetch()
    assert first[0] is None and first[1] == "SOURCE_FETCH_FAILED" and second == first
    fetch.assert_called_once()


def test_source_failure_never_produces_live_execution_data():
    reset()
    with patch("execution_service.app._fetch_source", return_value=(None, "SOURCE_FETCH_FAILED", "429 Too Many Requests")):
        r = client.get("/execution.json")
    body = r.json(); assert r.status_code == 503 and body["status"] == "SOURCE_UNAVAILABLE" and body["execution_enrichment"] is None and body.get("data_source_status") != "LIVE"


def test_existing_enrichment_behavior_is_delegated_unchanged():
    reset(); source = source_payload(); enriched = {**copy.deepcopy(source), "execution_enrichment": {"sentinel": True}}
    with patch("execution_service.app._fetch_source", return_value=(source, None, None)), patch("execution_service.app.build_execution_master", return_value=enriched) as builder:
        result = client.get("/execution.json").json()
    builder.assert_called_once(); assert result["execution_enrichment"] == {"sentinel": True}


def test_source_url_remains_read_only():
    assert service.SOURCE_URL == "https://psy29-live-data-hardening.onrender.com/data.txt"


def test_status_and_execution_share_one_cached_source_fetch():
    reset(); source = source_payload()
    with patch("execution_service.app._fetch_source", return_value=(source, None, None)) as fetch, patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        assert client.get("/status").status_code == 200
        assert client.get("/execution.json").status_code == 200
    fetch.assert_called_once()
