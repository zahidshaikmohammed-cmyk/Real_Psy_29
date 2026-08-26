from __future__ import annotations

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


class DiagnosticModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    error_code: str | None = None
    error_message: str | None = None
    error_time: str | None = None
    stage: str | None = None
    affected_stocks: list[str] = Field(default_factory=list)
    last_good_tick: str | None = None
    recovery_action: str | None = None
    recovery_attempts: int = 0
    recovery_limit: int | None = None
    data_safe: bool = True


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
    diagnostic: DiagnosticModel = Field(default_factory=lambda: DiagnosticModel(status="OK"))


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items() if not k.startswith("_")}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def _diagnostic(cleaned: dict[str, Any], stocks: dict[str, Any]) -> dict[str, Any]:
    """Build a public, machine-readable diagnosis without leaking private runtime state."""
    source = str(cleaned.get("data_source_status") or "UNKNOWN")
    now = cleaned.get("timestamp")
    affected = [symbol for symbol, stock in stocks.items() if isinstance(stock, dict) and stock.get("data_source_status") == "ERROR"]
    stock_errors = []
    for symbol in affected:
        message = stocks[symbol].get("error")
        if message:
            stock_errors.append(f"{symbol}: {message}")

    if stock_errors:
        return {
            "status": "ERROR",
            "error_code": "STOCK_INITIALIZATION_FAILED",
            "error_message": "; ".join(stock_errors),
            "error_time": now,
            "stage": "INITIALIZATION",
            "affected_stocks": affected,
            "recovery_action": "RETRY_INITIALIZATION_OR_RECONNECT",
            "data_safe": False,
        }
    if source == "ERROR":
        return {
            "status": "ERROR",
            "error_code": "COLLECTOR_FAILURE",
            "error_message": "Collector failed; inspect the service log for the originating exception.",
            "error_time": now,
            "stage": "COLLECTOR",
            "recovery_action": "RETRY_AFTER_30_SECONDS",
            "data_safe": False,
        }
    if source == "RECONNECTING":
        return {
            "status": "RECOVERING",
            "error_code": "DHAN_WEBSOCKET_DISCONNECTED",
            "error_message": "Dhan WebSocket disconnected; automatic reconnect is in progress.",
            "error_time": now,
            "stage": "WEBSOCKET",
            "recovery_action": "RECONNECT",
            "data_safe": False,
        }
    if source == "DISCONNECTED":
        return {
            "status": "ERROR",
            "error_code": "DHAN_WEBSOCKET_DISCONNECTED",
            "error_message": "Dhan WebSocket is disconnected and the feed is not currently live.",
            "error_time": now,
            "stage": "WEBSOCKET",
            "recovery_action": "RECONNECT",
            "data_safe": False,
        }
    if source == "STARTING":
        return {
            "status": "NOT_READY",
            "error_code": "COLLECTOR_NOT_STARTED",
            "error_message": "Collector has not entered the active market session yet.",
            "error_time": now,
            "stage": "STARTUP",
            "recovery_action": "WAIT_FOR_MARKET_SESSION",
            "data_safe": False,
        }
    if source == "BACKFILLING":
        return {
            "status": "RECOVERING",
            "error_code": "HISTORICAL_BACKFILL_IN_PROGRESS",
            "error_message": "Historical/session backfill is still in progress.",
            "error_time": now,
            "stage": "INITIALIZATION",
            "recovery_action": "WAIT_FOR_BACKFILL",
            "data_safe": False,
        }
    if source == "LIVE":
        return {"status": "OK", "data_safe": True, "last_good_tick": cleaned.get("last_update")}
    return {
        "status": "NOT_READY",
        "error_code": f"SOURCE_STATUS_{source.upper()}",
        "error_message": f"Live data source status is {source}.",
        "error_time": now,
        "stage": "FEED",
        "recovery_action": "INSPECT_FEED_STATE",
        "data_safe": False,
    }


def normalize_stock(raw: dict[str, Any]) -> dict[str, Any]:
    """Return one deterministic, JSON-safe stock object with no internal fields."""
    return NormalizedStock.model_validate(_clean(raw)).model_dump(mode="json")


def normalize_market(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the complete 29-stock machine-readable envelope."""
    cleaned = _clean(raw)
    stocks = cleaned.get("stocks", {})
    diagnostic = cleaned.get("diagnostic") or _diagnostic(cleaned, stocks)
    normalized = NormalizedMarketResponse.model_validate(
        {**cleaned, "stocks": {symbol: normalize_stock(stock) for symbol, stock in stocks.items()},
         "stocks_loaded": len(stocks), "diagnostic": diagnostic}
    )
    if normalized.stocks_expected != 29:
        raise ValueError("stocks_expected must remain 29")
    return normalized.model_dump(mode="json")


def validate_stock_response(raw: dict[str, Any]) -> None:
    """Raise ValidationError when a stock response violates the public schema."""
    NormalizedStock.model_validate(_clean(raw))
