from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import sys
import threading
import time
from typing import Any

import requests

LOGGER = logging.getLogger("psy29.github_master")
REPOSITORY = "zahidshaikmohammed-cmyk/Real_Psy_29"
BRANCH = "production-hardening"
PATH = "public/psy29_master.json"
API_URL = f"https://api.github.com/repos/{REPOSITORY}/contents/{PATH}"
INTERVAL_SECONDS = 45.0
_started = False


def _payload() -> dict[str, Any] | None:
    process = sys.modules.get("__main__")
    if process is None or not hasattr(process, "_machine_payload"):
        process = sys.modules.get("runner")
    if process is None or not hasattr(process, "_machine_payload"):
        return None
    payload = process._machine_payload()
    diagnostic = payload.get("diagnostic") or {}
    stocks = payload.get("stocks")
    if payload.get("data_source_status") != "LIVE":
        return None
    if payload.get("stocks_expected") != 29 or not isinstance(stocks, dict) or len(stocks) != 29:
        return None
    if diagnostic.get("data_safe") is not True:
        return None
    return payload


def _bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _current(token: str):
    r = requests.get(API_URL, params={"ref": BRANCH}, headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, timeout=20)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    body = r.json()
    encoded = str(body.get("content", "")).replace("\n", "")
    return body.get("sha"), base64.b64decode(encoded, validate=True)


def publish_once() -> bool:
    token = os.getenv("GITHUB_LIVE_DATA_TOKEN")
    if not token:
        LOGGER.warning("GitHub master publisher: token_present=false")
        return False
    payload = _payload()
    if payload is None:
        LOGGER.info("GitHub master publisher: validated_payload=false")
        return False
    content = _bytes(payload)
    sha, existing = _current(token)
    if existing is not None and hashlib.sha256(existing).digest() == hashlib.sha256(content).digest():
        return True
    body: dict[str, Any] = {
        "message": "Update validated PSY29 master data [skip render]",
        "content": base64.b64encode(content).decode("ascii"),
        "branch": BRANCH,
    }
    if sha:
        body["sha"] = sha
    r = requests.put(API_URL, headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}, json=body, timeout=20)
    r.raise_for_status()
    LOGGER.info("Published validated PSY29 master: stocks=%d", len(payload["stocks"]))
    return True


def _worker():
    while True:
        try:
            publish_once()
        except Exception as exc:
            LOGGER.warning("GitHub master publish failed: %s", exc)
        time.sleep(INTERVAL_SECONDS)


def start():
    global _started
    if _started or not os.getenv("GITHUB_LIVE_DATA_TOKEN"):
        LOGGER.warning("GitHub master publisher not started: token_present=%s", bool(os.getenv("GITHUB_LIVE_DATA_TOKEN")))
        return
    _started = True
    LOGGER.info("GitHub master publisher started: interval=%ss", int(INTERVAL_SECONDS))
    threading.Thread(target=_worker, name="psy29-github-master", daemon=True).start()
