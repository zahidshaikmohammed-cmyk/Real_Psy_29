from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class CandleModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    timestamp: str
    epoch: int
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None


class NormalizedStock(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str
    security_id: str
    current_price: float | None
    ohlc: dict[str, float | None]
    session_high: float | None
    session_low: float | None
    previous_day: dict[str, Any]
    volume: int | None
    candles: dict[str, list[CandleModel]]
    vwap: float | None
    ema9: float | None
    ema20: float | None
    opening_range: dict[str, Any]
    structure: dict[str, Any]
    timestamp: str
    trading_date: str
    market_session_status: str
    data_source_status: str
    last_tick: str | None


class NormalizedMarketResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    service: str = "PSY29 Live Data"
    timestamp: str
    trading_date: str | None
    market_session_status: str
    data_source_status: str
    stocks_expected: int = Field(default=29, ge=0)
    stocks_loaded: int = Field(ge=0)
    stocks: dict[str, NormalizedStock]


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items() if not k.startswith("_")}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def normalize_stock(raw: dict[str, Any]) -> dict[str, Any]:
    """Return one deterministic, JSON-safe stock object with no internal fields."""
    return NormalizedStock.model_validate(_clean(raw)).model_dump(mode="json")


def normalize_market(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the complete 29-stock machine-readable envelope."""
    cleaned = _clean(raw)
    stocks = cleaned.get("stocks", {})
    normalized = NormalizedMarketResponse.model_validate(
        {**cleaned, "stocks": {symbol: normalize_stock(stock) for symbol, stock in stocks.items()},
               "stocks_loaded": len(stocks)}
    )
    if normalized.stocks_expected != 29:
        raise ValueError("stocks_expected must remain 29")
    return normalized.model_dump(mode="json")


def validate_stock_response(raw: dict[str, Any]) -> None:
    """Raise ValidationError when a stock response violates the public schema."""
    NormalizedStock.model_validate(_clean(raw))
