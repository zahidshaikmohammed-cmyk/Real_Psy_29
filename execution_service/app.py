from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

from psy29.execution_master_builder import build_execution_master

SOURCE_URL = "https://psy29-live-data-hardening.onrender.com/data.txt"
REFRESH_SECONDS = 45.0
TIMEOUT_SECONDS = 15.0

app = FastAPI(title="PSY29 Execution Enrichment", version="1.0.0")
_cache: dict[str, Any] = {
    "payload": None,
    "fetched_at_monotonic": None,
    "last_successful_fetch": None,
    "error_code": None,
    "error_message": None,
}
_cache_lock = threading.RLock()
_fetch_inflight: threading.Event | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _source_snapshot(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"source_trading_date": None, "source_timestamp": None, "source_status": None, "source_stocks_loaded": 0, "source_stocks_expected": 29}
    stocks = payload.get("stocks")
    loaded = payload.get("stocks_loaded")
    if loaded is None and isinstance(stocks, dict):
        loaded = len(stocks)
    return {"source_trading_date": payload.get("trading_date"), "source_timestamp": payload.get("timestamp"), "source_status": payload.get("data_source_status"), "source_stocks_loaded": loaded or 0, "source_stocks_expected": payload.get("stocks_expected", 29)}


def _fetch_source():
    try:
        response = requests.get(SOURCE_URL, params={"t": str(time.time_ns())}, timeout=TIMEOUT_SECONDS, headers={"Cache-Control": "no-cache", "Pragma": "no-cache"})
        response.raise_for_status()
    except requests.RequestException as exc:
        return None, "SOURCE_FETCH_FAILED", str(exc)
    try:
        payload = response.json()
    except ValueError as exc:
        return None, "SOURCE_INVALID_JSON", str(exc)
    if not isinstance(payload, dict):
        return None, "SOURCE_INVALID_STRUCTURE", "Source JSON root must be an object."
    return payload, None, None


def _source_safe(payload: dict[str, Any]):
    stocks = payload.get("stocks")
    expected = payload.get("stocks_expected", 29)
    loaded = payload.get("stocks_loaded")
    if loaded is None and isinstance(stocks, dict):
        loaded = len(stocks)
    if not isinstance(stocks, dict):
        return False, "SOURCE_INVALID_STRUCTURE", "Source stocks must be an object."
    if loaded != expected:
        return False, "SOURCE_STOCK_COUNT_MISMATCH", f"stocks_loaded={loaded}, stocks_expected={expected}"
    diagnostic = payload.get("diagnostic")
    if not isinstance(diagnostic, dict) or diagnostic.get("data_safe") is not True:
        return False, "SOURCE_UNSAFE", "Source diagnostic.data_safe is not true."
    return True, None, None


def _cache_is_fresh(now: float) -> bool:
    fetched = _cache["fetched_at_monotonic"]
    if fetched is None:
        return False
    return now - fetched < REFRESH_SECONDS


def _cached_result():
    return copy.deepcopy(_cache["payload"]), _cache["error_code"], _cache["error_message"]


def _get_cached_or_fetch():
    """Return one cached result, with at most one upstream fetch in flight."""
    global _fetch_inflight
    with _cache_lock:
        if _cache_is_fresh(time.monotonic()):
            return _cached_result()
        if _fetch_inflight is None:
            event = threading.Event()
            _fetch_inflight = event
            owner = True
        else:
            event = _fetch_inflight
            owner = False
    if not owner:
        event.wait(timeout=TIMEOUT_SECONDS + 5.0)
        with _cache_lock:
            return _cached_result()
    try:
        payload, code, message = _fetch_source()
        completed_monotonic = time.monotonic()
        successful_fetch = _utc_now() if payload is not None and code is None else None
        with _cache_lock:
            _cache["payload"] = copy.deepcopy(payload) if payload is not None else None
            _cache["fetched_at_monotonic"] = completed_monotonic
            _cache["last_successful_fetch"] = successful_fetch
            _cache["error_code"] = code
            _cache["error_message"] = message
            return _cached_result()
    finally:
        with _cache_lock:
            event = _fetch_inflight
            _fetch_inflight = None
            if event is not None:
                event.set()


def _status_from_source(payload, code, message):
    snap = _source_snapshot(payload)
    if code:
        enrichment_status, error_code, error_message = "SOURCE_UNAVAILABLE", code, message
    elif payload:
        safe, gate_code, gate_message = _source_safe(payload)
        enrichment_status, error_code, error_message = (("AVAILABLE", None, None) if safe else ("BLOCKED_SOURCE_UNSAFE", gate_code, gate_message))
    else:
        enrichment_status, error_code, error_message = "SOURCE_UNAVAILABLE", "NO_SOURCE_FETCH", "No source payload fetched yet."
    with _cache_lock:
        last_successful_fetch = _cache["last_successful_fetch"]
    return {"service": "PSY29 Execution Enrichment", "source_url": SOURCE_URL, "last_source_fetch": last_successful_fetch, **snap, "enrichment_status": enrichment_status, "error_code": error_code, "error_message": error_message}


@app.get("/")
def root():
    return {"service": "PSY29 Execution Enrichment", "status": "OK", "endpoints": ["/health", "/execution.json", "/execution.txt", "/status"]}


@app.get("/health")
def health():
    return {"service": "PSY29 Execution Enrichment", "status": "OK"}


@app.get("/status")
def status():
    payload, code, message = _get_cached_or_fetch()
    return _status_from_source(payload, code, message)


def _execution_payload():
    payload, code, message = _get_cached_or_fetch()
    if payload is None:
        return None, JSONResponse(status_code=503, content={"service": "PSY29 Execution Enrichment", "status": "SOURCE_UNAVAILABLE", "execution_enrichment": None, "error_code": code or "SOURCE_UNAVAILABLE", "error_message": message or "Source unavailable."})
    safe, gate_code, gate_message = _source_safe(payload)
    if not safe:
        result = copy.deepcopy(payload)
        result["execution_enrichment"] = None
        result["enrichment_status"] = "BLOCKED_SOURCE_UNSAFE"
        result["error_code"] = gate_code
        result["error_message"] = gate_message
        return result, None
    try:
        return build_execution_master(payload, generated_at=_utc_now()), None
    except Exception as exc:
        return None, JSONResponse(status_code=500, content={"service": "PSY29 Execution Enrichment", "status": "ENRICHMENT_FAILED", "execution_enrichment": None, "error_code": "ENRICHMENT_FAILED", "error_message": str(exc)})


@app.get("/execution.json")
def execution_json():
    result, error = _execution_payload()
    if error is not None:
        return error
    return JSONResponse(content=result)


@app.get("/execution.txt")
def execution_txt():
    result, error = _execution_payload()
    if error is not None:
        return PlainTextResponse(error.body.decode("utf-8"), status_code=error.status_code, media_type="text/plain")
    import json
    return PlainTextResponse(json.dumps(result, ensure_ascii=False, separators=(",", ":")), media_type="text/plain")
