from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(RuntimeError):
    """Raised when required runtime configuration is missing or invalid."""


@dataclass(frozen=True)
class DhanConfig:
    client_id: str
    pin: str
    totp_secret: str


def load_dhan_config(environ: dict[str, str] | None = None) -> DhanConfig:
    env = os.environ if environ is None else environ
    required = ("DHAN_CLIENT_ID", "DHAN_PIN", "DHAN_TOTP_SECRET")
    missing = [name for name in required if not str(env.get(name, "")).strip()]
    if missing:
        raise ConfigurationError(
            "Missing required Dhan environment variables: " + ", ".join(missing)
        )

    client_id = str(env["DHAN_CLIENT_ID"]).strip()
    pin = str(env["DHAN_PIN"]).strip()
    totp_secret = str(env["DHAN_TOTP_SECRET"]).strip().replace(" ", "")

    if not client_id.isdigit():
        raise ConfigurationError("DHAN_CLIENT_ID must contain only digits")
    if not pin.isdigit():
        raise ConfigurationError("DHAN_PIN must contain only digits")
    if not totp_secret:
        raise ConfigurationError("DHAN_TOTP_SECRET is empty")

    return DhanConfig(
        client_id=client_id,
        pin=pin,
        totp_secret=totp_secret,
    )
