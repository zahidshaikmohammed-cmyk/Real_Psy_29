from __future__ import annotations

import copy
import json
from pathlib import Path
from unittest.mock import Mock, patch
import sys

sys.path.insert(0, str(Path(__file__).parents[1]))

from fastapi.testclient import TestClient
import execution_service.app as service
from execution_service.app import app

client = TestClient(app)


def source_payload(safe=True, count=29):
    stocks = {
        f"S{i:02d}": {
            "candles": {"1m": [], "5m": [], "15m": [], "1h": []},
            "diagnostic": {"data_safe": safe},
            "last_tick": 100 + i,
            "last_tick_timestamp": "2026-08-27T10:00:00+05:30",
        }
        for i in range(count)
    }
    return {
        "trading_date": "2026-08-27",
        "timestamp": "2026-08-27T10:00:00+05:30",
        "data_source_status": "LIVE" if safe else "DATA_UNSAFE",
        "stocks_expected": 29,
        "stocks_loaded": count,
        "diagnostic": {"data_safe": safe},
        "stocks": stocks,
    }


def reset():
    service._cache.update(payload=None, fetched_at=None, error_code=None, error_message=None)


def resp(payload):
    r = Mock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


def test_health():
    assert client.get("/health").json() == {"service": "PSY29 Execution Enrichment", "status": "OK"}


def test_status():
    reset()
    body = client.get("/status").json()
    assert body["source_stocks_expected"] == 29


def test_valid_source_and_builder():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: {**copy.deepcopy(p), "execution_enrichment": {"ok": True}}) as builder:
        r = client.get("/execution.json")
    assert r.status_code == 200 and r.json()["execution_enrichment"]["ok"] is True
    builder.assert_called_once()


def test_invalid_json():
    reset()
    r = Mock()
    r.raise_for_status.return_value = None
    r.json.side_effect = ValueError("bad")
    with patch("execution_service.app.requests.get", return_value=r):
        out = client.get("/execution.json")
    assert out.status_code == 503 and out.json()["error_code"] == "SOURCE_INVALID_JSON"


def test_unavailable_source():
    reset()
    import requests
    with patch("execution_service.app.requests.get", side_effect=requests.RequestException("down")):
        out = client.get("/execution.json")
    assert out.status_code == 503 and out.json()["status"] == "SOURCE_UNAVAILABLE"


def test_unsafe_source_blocked():
    reset()
    source = source_payload(False)
    before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master") as builder:
        out = client.get("/execution.json")
    assert out.json()["enrichment_status"] == "BLOCKED_SOURCE_UNSAFE"
    assert out.json()["execution_enrichment"] is None
    builder.assert_not_called()
    assert source == before


def test_29_stocks():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        assert len(client.get("/execution.json").json()["stocks"]) == 29


def test_source_not_mutated():
    reset()
    source = source_payload()
    before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    assert source == before


def test_enrichment_is_isolated():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: {**copy.deepcopy(p), "execution_enrichment": {"x": 1}}):
        result = client.get("/execution.json").json()
    assert "execution_enrichment" not in source and result["execution_enrichment"] == {"x": 1}


def test_json_response():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        r = client.get("/execution.json")
    assert r.headers["content-type"].startswith("application/json")
    json.loads(r.text)


def test_source_read_only_and_cache_busted():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)) as get:
        client.get("/execution.json")
    args, kw = get.call_args
    assert args[0] == service.SOURCE_URL and "t" in kw["params"]
    assert kw["headers"]["Cache-Control"] == "no-cache"


def test_stale_timestamp_preserved():
    reset()
    source = source_payload()
    source["timestamp"] = "2026-08-27T09:15:00+05:30"
    before = source["timestamp"]
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        out = client.get("/execution.json").json()
    assert out["timestamp"] == before


def test_no_candle_repair():
    reset()
    source = source_payload()
    source["stocks"]["S00"]["candles"]["1m"] = [
        {"timestamp": "2026-08-27T09:15:00+05:30"},
        {"timestamp": "2026-08-27T09:17:00+05:30"},
    ]
    before = copy.deepcopy(source)
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    assert source == before


def test_no_fake_depth():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        result = client.get("/execution.json").json()
    assert result["stocks"]["S00"].get("bid") is None


def test_source_failure_does_not_crash_health():
    reset()
    import requests
    with patch("execution_service.app.requests.get", side_effect=requests.RequestException("down")):
        assert client.get("/health").status_code == 200


def test_partial_source_blocked():
    reset()
    source = source_payload(count=28)
    with patch("execution_service.app.requests.get", return_value=resp(source)):
        out = client.get("/execution.json")
    assert out.json()["enrichment_status"] == "BLOCKED_SOURCE_UNSAFE"
    assert out.json()["execution_enrichment"] is None


def test_status_after_fetch():
    reset()
    source = source_payload()
    with patch("execution_service.app.requests.get", return_value=resp(source)), \
         patch("execution_service.app.build_execution_master", side_effect=lambda p, generated_at=None: copy.deepcopy(p)):
        client.get("/execution.json")
    body = client.get("/status").json()
    assert body["source_trading_date"] == source["trading_date"]
    assert body["source_status"] == "LIVE" and body["source_stocks_loaded"] == 29
    assert body["enrichment_status"] == "AVAILABLE"
