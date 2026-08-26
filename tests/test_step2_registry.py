import unittest

from psy29.instrument_registry import (
    STOCKS,
    InstrumentRegistryError,
    build_registry,
)


HEADER = "SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_INSTRUMENT_NAME,SEM_TRADING_SYMBOL,SEM_SMST_SECURITY_ID"


def master_csv(symbols: list[str], ids: list[str] | None = None) -> str:
    ids = ids or [str(10000 + i) for i in range(len(symbols))]
    rows = [HEADER]
    rows.extend(f"NSE,E,EQUITY,{symbol},{security_id}" for symbol, security_id in zip(symbols, ids))
    return "\n".join(rows) + "\n"


class RegistryTests(unittest.TestCase):
    def test_requires_all_29_stocks(self):
        registry = build_registry(master_csv(list(STOCKS)))
        self.assertEqual(len(registry.instruments), 29)
        self.assertEqual(tuple(i.symbol for i in registry.instruments), STOCKS)
        self.assertEqual(len(registry.by_security_id), 29)

    def test_rejects_missing_stock(self):
        with self.assertRaises(InstrumentRegistryError):
            build_registry(master_csv(list(STOCKS[:-1])))

    def test_rejects_duplicate_security_id(self):
        ids = [str(10000 + i) for i in range(29)]
        ids[-1] = ids[0]
        with self.assertRaises(InstrumentRegistryError):
            build_registry(master_csv(list(STOCKS), ids))

    def test_ignores_non_nse_equity_rows(self):
        csv = master_csv(list(STOCKS[:-1])) + "NSE,D,EQUITY,AMBUJACEM,99999\n"
        with self.assertRaises(InstrumentRegistryError):
            build_registry(csv)

    def test_accepts_only_expected_equity_instrument(self):
        rows = [HEADER]
        for index, symbol in enumerate(STOCKS):
            instrument = "EQUITY" if symbol != "VEDL" else "FUTIDX"
            rows.append(f"NSE,E,{instrument},{symbol},{20000 + index}")
        with self.assertRaises(InstrumentRegistryError):
            build_registry("\n".join(rows) + "\n")


if __name__ == "__main__":
    unittest.main()
