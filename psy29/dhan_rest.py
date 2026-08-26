from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import requests

from .config import DhanConfig

BASE_URL = "https://api.dhan.co/v2"


class DhanRestError(RuntimeError):
    """Raised when a Dhan REST request fails or returns an invalid response."""


@dataclass(frozen=True)
class DhanRestClient:
    config: DhanConfig
    access_token: str
    session: requests.Session | None = None
    timeout: float = 20.0

    def _session(self) -> requests.Session:
        return self.session or requests.Session()

    def _headers(self, *, client_id: bool = False) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.access_token,
        }
        if client_id:
            headers["client-id"] = self.config.client_id
        return headers

    def _post(
        self,
        path: str,
        payload: Mapping[str, Any],
        *,
        client_id: bool = False,
    ) -> dict[str, Any]:
        try:
            response = self._session().post(
                f"{BASE_URL}{path}",
                headers=self._headers(client_id=client_id),
                json=dict(payload),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise DhanRestError(f"Dhan REST request failed: {path}") from exc

        try:
            body = response.json()
        except ValueError as exc:
            raise DhanRestError(
                f"Dhan REST returned non-JSON response: {path} (HTTP {response.status_code})"
            ) from exc

        if response.status_code >= 400:
            message = body.get("errorMessage") or body.get("message") or "request failed"
            code = body.get("errorCode") or body.get("errorType")
            detail = f"Dhan REST error on {path}: {message}"
            if code:
                detail += f" ({code})"
            raise DhanRestError(detail)

        if not isinstance(body, dict):
            raise DhanRestError(f"Dhan REST returned an unexpected payload: {path}")
        return body

    def historical_daily(
        self,
        security_id: str,
        from_date: str,
        to_date: str,
        *,
        oi: bool = False,
    ) -> dict[str, Any]:
        return self._post(
            "/charts/historical",
            {
                "securityId": security_id,
                "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY",
                "expiryCode": 0,
                "oi": oi,
                "fromDate": from_date,
                "toDate": to_date,
            },
        )

    def intraday(
        self,
        security_id: str,
        interval: int,
        from_datetime: datetime | str,
        to_datetime: datetime | str,
        *,
        oi: bool = False,
    ) -> dict[str, Any]:
        if interval not in {1, 5, 15, 25, 60}:
            raise ValueError("Dhan intraday interval must be one of 1, 5, 15, 25, 60 minutes")

        def format_datetime(value: datetime | str) -> str:
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d %H:%M:%S")
            return value

        return self._post(
            "/charts/intraday",
            {
                "securityId": security_id,
                "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY",
                "interval": str(interval),
                "oi": oi,
                "fromDate": format_datetime(from_datetime),
                "toDate": format_datetime(to_datetime),
            },
        )

    def ltp(self, security_ids: list[str]) -> dict[str, Any]:
        return self._post(
            "/marketfeed/ltp",
            {"NSE_EQ": [int(security_id) for security_id in security_ids]},
            client_id=True,
        )

    def ohlc(self, security_ids: list[str]) -> dict[str, Any]:
        return self._post(
            "/marketfeed/ohlc",
            {"NSE_EQ": [int(security_id) for security_id in security_ids]},
            client_id=True,
        )

    def quote(self, security_ids: list[str]) -> dict[str, Any]:
        return self._post(
            "/marketfeed/quote",
            {"NSE_EQ": [int(security_id) for security_id in security_ids]},
            client_id=True,
        )
