from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
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


def _validated_payload() -> dict[str, Any] | None:
    runner = __import__("runner")
    payload = runner._machine_payload()
    diagnostic = payload.get("diagnostic") or {}
    stocks = payload.get("stocks")
    if (
        payload.get("data_source_status") != "LIVE"
        or payload.get("stocks_expected") != 29
        or not isinstance(stocks, dict)
        or len(stocks) != 29
        or diagnostic.get("data_safe") is not True
    ):
        return None
    return payload


def _payload_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _get_current_file(token: str) -> tuple[str | None, bytes | None]:
    response = requests.get(
        API_URL,
        params={"ref": BRANCH},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=20,
    )
    if response.status_code == 404:
        return None, None
    response.raise_for_status()
    body = response.json()
    sha = body.get("sha")
    encoded = body.get("content", "").replace("\n", "")
    try:
        content = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise RuntimeError("GitHub master file contains invalid base64") from exc
    return sha, content


def publish_once() -> bool:
    token = os.getenv("GITHUB_LIVE_DATA_TOKEN")
    if not token:
        LOGGER.warning("GitHub master publisher disabled: GITHUB_LIVE_DATA_TOKEN is missing")
        return False

    payload = _validated_payload()
    if payload is None:
        return False

    content = _payload_bytes(payload)
    desired_hash = hashlib.sha256(content).hexdigest()
    sha, existing = _get_current_file(token)
    if existing is not None and hashlib.sha256(existing).hexdigest() == desired_hash:
        return True

    encoded = base64.b64encode(content).decode("ascii")
    body: dict[str, Any] = {
        "message": "Update validated PSY29 master data",
        "content": encoded,
        "branch": BRANCH,
    }
    if sha:
        body["sha"] = sha

    response = requests.put(
        API_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json=body,
        timeout=20,
    )
    response.raise_for_status()
    LOGGER.info("Published validated PSY29 master: stocks=%d status=%s safe=%s", len(payload["stocks"]), payload["data_source_status"], payload["diagnostic"]["data_safe"])
    return True


def _worker() -> None:
    while True:
        try:
            publish_once()
        except Exception as exc:
            LOGGER.warning("GitHub master publish failed: %s", exc)
        time.sleep(INTERVAL_SECONDS)


def start() -> None:
    global _started
    if _started or not os.getenv("GITHUB_LIVE_DATA_TOKEN"):
        return
    _started = True
    threading.Thread(target=_worker, name="psy29-github-master", daemon=True).start()
