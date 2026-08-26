from __future__ import annotations

import csv
import io
from dataclasses import dataclass

import requests


class InstrumentRegistryError(RuntimeError):
    """Raised when the Dhan instrument registry cannot be built safely."""


STOCKS: tuple[str, ...] = (
    "NESTLEIND", "VEDL", "ICICIPRULI", "KALYANKJIL", "KOTAKBANK",
    "BANDHANBNK", "BANKBARODA", "TITAN", "INFY", "DLF", "TCS",
    "MAXHEALTH", "KFINTECH", "PRESTIGE", "BHEL", "RBLBANK", "HCLTECH",
    "ICICIGI", "HDFCLIFE", "MARICO", "LUPIN", "COFORGE", "TECHM",
    "SWIGGY", "PERSISTENT", "OBEROIRLTY", "SUPREMEIND", "LAURUSLABS",
    "AMBUJACEM",
)

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
EXPECTED_EXCHANGE = "NSE"
EXPECTED_SEGMENT = "E"
EXPECTED_INSTRUMENT = "EQUITY"


@dataclass(frozen=True)
class Instrument:
    symbol: str
    security_id: str
    exchange_segment: str = "NSE_EQ"
    instrument: str = "EQUITY"


@dataclass(frozen=True)
class InstrumentRegistry:
    instruments: tuple[Instrument, ...]

    @property
    def by_symbol(self) -> dict[str, Instrument]:
        return {instrument.symbol: instrument for instrument in self.instruments}

    @property
    def by_security_id(self) -> dict[str, Instrument]:
        return {instrument.security_id: instrument for instrument in self.instruments}


def _value(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = (row.get(name) or "").strip()
        if value:
            return value
    return ""


def build_registry(csv_text: str) -> InstrumentRegistry:
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise InstrumentRegistryError("Dhan instrument master is empty")

    found: dict[str, Instrument] = {}
    for row in rows:
        exchange = _value(row, "SEM_EXM_EXCH_ID", "EXCH_ID")
        segment = _value(row, "SEM_SEGMENT", "SEGMENT")
        instrument = _value(row, "SEM_INSTRUMENT_NAME", "INSTRUMENT")
        symbol = _value(row, "SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME", "SYMBOL_NAME")
        security_id = _value(row, "SEM_SMST_SECURITY_ID", "SECURITY_ID")

        if (
            exchange != EXPECTED_EXCHANGE
            or segment != EXPECTED_SEGMENT
            or instrument != EXPECTED_INSTRUMENT
            or not symbol
            or not security_id
        ):
            continue

        if symbol in STOCKS:
            candidate = Instrument(symbol=symbol, security_id=security_id)
            previous = found.get(symbol)
            if previous and previous != candidate:
                raise InstrumentRegistryError(
                    f"Duplicate conflicting instrument mapping for {symbol}"
                )
            found[symbol] = candidate

    missing = [symbol for symbol in STOCKS if symbol not in found]
    if missing:
        raise InstrumentRegistryError(
            f"Instrument registry incomplete: missing {len(missing)} of {len(STOCKS)}: {missing}"
        )

    instruments = tuple(found[symbol] for symbol in STOCKS)
    security_ids = [item.security_id for item in instruments]
    if len(set(security_ids)) != len(security_ids):
        raise InstrumentRegistryError("Instrument registry contains duplicate security IDs")

    return InstrumentRegistry(instruments=instruments)


def load_registry(session: requests.Session | None = None) -> InstrumentRegistry:
    client = session or requests.Session()
    try:
        response = client.get(SCRIP_MASTER_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise InstrumentRegistryError("Unable to download Dhan instrument master") from exc

    return build_registry(response.text)
