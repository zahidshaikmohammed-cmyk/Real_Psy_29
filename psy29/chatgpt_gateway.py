"""Minimal public JSON gateway for machine/web retrieval.

This module deliberately contains no market logic. It exposes the already
normalized live state from runner.py as a small, cache-resistant JSON route.
"""

from __future__ import annotations

import json
import time

from fastapi.responses import Response

import runner


def _response() -> Response:
    payload = runner._machine_payload()
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return Response(
        content=body,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Surrogate-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
            "X-Content-Type-Options": "nosniff",
            "X-PSY29-Generated-At": str(time.time()),
        },
    )


app = runner.app


@app.get("/api/v1/live.json", include_in_schema=False)
def chatgpt_live_json() -> Response:
    return _response()


@app.get("/api/v1/market.json", include_in_schema=False)
def chatgpt_market_json() -> Response:
    return _response()
