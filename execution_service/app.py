from __future__ import annotations

import copy
import time
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from psy29.execution_master_builder import build_execution_master

SOURCE_URL = "https://psy29-live-data-hardening.onrender.com/data.txt"
REFRESH_SECONDS = 45.0
TIMEOUT_SECONDS = 15.0

app = FastAPI(title="PSY29 Execution Enrichment", version="1.0.0")
_cache: dict[str, Any] = {
    "payload": None,
    "fetched_at": None,
    "error_code": None,
    "error_message": None,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _source_snapshot(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "source_trading_date": None,
            "source_timestamp": None,
            "source_status": None,
            "source_stocks_loaded": 0,
            "source_stocks_expected": 29,
        }
    stocks = payload.get("stocks")
    loaded = payload.get("stocks_loaded")
    if loaded is None and isinstance(stocks, dict):
        loaded = len(stocks)
    return {
        "source_trading_date": payload.get("trading_date"),
        "source_timestamp": payload.get("timestamp"),
        "source_status": payload.get("data_source_status"),
        "source_stocks_loaded": loaded or 0,
        "source_stocks_expected": payload.get("stocks_expected", 29),
    }


def _fetch_source() -> tuple[dict[str, Any] | None, str | None, str | None]:
    try:
        response = requests.get(
            SOURCE_URL,
            params={"t": str(time.time_ns())},
            timeout=TIMEOUT_SECONDS,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
        )
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


def _source_safe(payload: dict[str, Any]) -> tuple[bool, str | None, str | None]:
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


def _get_cached_or_fetch() -> tuple[dict[str, Any] | None, str | None, str | None]:
    now = time.monotonic()
    if (
        _cache["payload"] is not None
        and _cache["fetched_at"] is not None
        and now - _cache["fetched_at"] < REFRESH_SECONDS
    ):
        return copy.deepcopy(_cache["payload"]), _cache["error_code"], _cache["error_message"]

    payload, code, message = _fetch_source()
    _cache.update(
        payload=copy.deepcopy(payload) if payload is not None else None,
        fetched_at=now,
        error_code=code,
        error_message=message,
    )
    return copy.deepcopy(payload), code, message


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "PSY29 Execution Enrichment",
        "status": "OK",
        "endpoints": ["/health", "/execution.json", "/status"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"service": "PSY29 Execution Enrichment", "status": "OK"}


@app.get("/status")
def status() -> dict[str, Any]:
    payload = copy.deepcopy(_cache["payload"])
    snapshot = _source_snapshot(payload)
    if _cache["error_code"]:
        enrichment_status = "SOURCE_UNAVAILABLE"
        error_code = _cache["error_code"]
        error_message = _cache["error_message"]
    elif payload is None:
        enrichment_status = "SOURCE_UNAVAILABLE"
        error_code = "SOURCE_NOT_FETCHED"
        error_message = "No source payload has been fetched yet."
    else:
        safe, gate_code, gate_message = _source_safe(payload)
        enrichment_status = "AVAILABLE" if safe else "BLOCKED_SOURCE_UNSAFE"
        error_code = gate_code
        error_message = gate_message
    return {
        "service": "PSY29 Execution Enrichment",
        "source_url": SOURCE_URL,
        "last_source_fetch": _cache["fetched_at"],
        **snapshot,
        "enrichment_status": enrichment_status,
        "error_code": error_code,
        "error_message": error_message,
    }


@app.get("/execution.json")
def execution_json():
    payload, code, message = _get_cached_or_fetch()
    if payload is None:
        return JSONResponse(
            status_code=503,
            content={
                "service": "PSY29 Execution Enrichment",
                "status": "SOURCE_UNAVAILABLE",
                "execution_enrichment": None,
                "error_code": code or "SOURCE_UNAVAILABLE",
                "error_message": message or "Source unavailable.",
            },
        )

    safe, gate_code, gate_message = _source_safe(payload)
    if not safe:
        result = copy.deepcopy(payload)
        result["execution_enrichment"] = None
        result["enrichment_status"] = "BLOCKED_SOURCE_UNSAFE"
        result["error_code"] = gate_code
        result["error_message"] = gate_message
        return JSONResponse(content=result)

    try:
        result = build_execution_master(payload, generated_at=_utc_now())
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "service": "PSY29 Execution Enrichment",
                "status": "ENRICHMENT_FAILED",
                "execution_enrichment": None,
                "error_code": "ENRICHMENT_FAILED",
                "error_message": str(exc),
            },
        )
    return JSONResponse(content=result)


__all__ = ["app", "SOURCE_URL"]
