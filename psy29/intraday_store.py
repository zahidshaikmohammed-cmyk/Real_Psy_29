from __future__ import annotations

import json
import os
from datetime import date
from typing import Any

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:  # pragma: no cover - exercised only when dependency is absent
    psycopg2 = None
    Json = None


class IntradayStore:
    """Best-effort durable storage for the current trading session.

    Persistence is enabled when DATABASE_URL is present. Database failures never
    take down the market collector; the in-memory engine remains authoritative.
    """

    def __init__(self, url: str | None = None):
        self.url = url or os.getenv("DATABASE_URL")
        self._conn = None
        self.enabled = bool(self.url and psycopg2 is not None)
        self.last_error: str | None = None

    def connect(self) -> bool:
        if not self.enabled:
            return False
        if self._conn is not None and self._conn.closed == 0:
            return True
        try:
            self._conn = psycopg2.connect(self.url, connect_timeout=5)
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS psy29_intraday_session (
                        trading_date DATE NOT NULL,
                        symbol TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (trading_date, symbol)
                    )
                    """
                )
            self.last_error = None
            return True
        except Exception as exc:
            self.last_error = str(exc)
            self._conn = None
            return False

    def save_stock(self, trading_date: str, symbol: str, payload: dict[str, Any]) -> bool:
        if not self.connect():
            return False
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO psy29_intraday_session (trading_date, symbol, payload, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (trading_date, symbol)
                    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                    """,
                    (date.fromisoformat(trading_date), symbol, Json(payload)),
                )
            return True
        except Exception as exc:
            self.last_error = str(exc)
            return False

    def save_market(self, trading_date: str, stocks: dict[str, dict[str, Any]]) -> int:
        saved = 0
        for symbol, payload in stocks.items():
            if self.save_stock(trading_date, symbol, payload):
                saved += 1
        return saved

    def load_session(self, trading_date: str) -> dict[str, dict[str, Any]]:
        if not self.connect():
            return {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, payload FROM psy29_intraday_session WHERE trading_date = %s",
                    (date.fromisoformat(trading_date),),
                )
                return {symbol: payload for symbol, payload in cur.fetchall()}
        except Exception as exc:
            self.last_error = str(exc)
            return {}

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "connected": bool(self._conn is not None and self._conn.closed == 0),
            "error": self.last_error,
        }
