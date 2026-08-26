from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pyotp
import requests

from .config import DhanConfig

AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"


class DhanAuthenticationError(RuntimeError):
    """Raised when Dhan access-token generation fails."""


@dataclass(frozen=True)
class AccessToken:
    value: str
    expiry_time: datetime | None


def _parse_expiry(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise DhanAuthenticationError(
            f"Dhan returned an invalid expiryTime: {value!r}"
        ) from exc


def generate_access_token(
    config: DhanConfig,
    *,
    session: requests.Session | None = None,
    timeout: float = 20.0,
) -> AccessToken:
    client = session or requests.Session()
    totp = pyotp.TOTP(config.totp_secret).now()

    try:
        response = client.post(
            AUTH_URL,
            params={
                "dhanClientId": config.client_id,
                "pin": config.pin,
                "totp": totp,
            },
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise DhanAuthenticationError("Unable to reach Dhan authentication service") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise DhanAuthenticationError(
            f"Dhan authentication returned non-JSON response (HTTP {response.status_code})"
        ) from exc

    if response.status_code >= 400:
        message = payload.get("errorMessage") or payload.get("message") or "authentication failed"
        raise DhanAuthenticationError(f"Dhan authentication failed: {message}")

    token = payload.get("accessToken")
    if not isinstance(token, str) or not token:
        raise DhanAuthenticationError("Dhan authentication returned no accessToken")

    return AccessToken(
        value=token,
        expiry_time=_parse_expiry(payload.get("expiryTime")),
    )
