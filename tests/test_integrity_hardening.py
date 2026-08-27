from datetime import datetime
import struct

import pytest

from psy29.data_integrity import DataIntegrityError, validate_intraday_rows, validate_live_quote


TRADING_DATE = "2026-08-27"


def candle(minute: int, price: float = 1900.0, volume: int = 100):
    ts = f"2026-08-27T09:{minute:02d}:00+05:30"
    epoch = int(datetime.fromisoformat(ts).timestamp())
    return {"timestamp": ts, "epoch": epoch, "open": price, "high": price + 2,
            "low": price - 2, "close": price + 1, "volume": volume}


def test_duplicate_candles_are_hard_failure():
    rows = [candle(15) for _ in range(15)]
    with pytest.raises(DataIntegrityError, match="duplicate candle"):
        validate_intraday_rows(rows, TRADING_DATE)


def test_non_chronological_candles_are_hard_failure():
    rows = [candle(15), candle(17), candle(16)] + [candle(i) for i in range(18, 30)]
    with pytest.raises(DataIntegrityError, match="non-chronological candle"):
        validate_intraday_rows(rows, TRADING_DATE)


def test_future_candle_is_hard_failure():
    row = candle(15)
    row["timestamp"] = "2026-08-27T14:00:00+05:30"
    row["epoch"] = int(datetime.fromisoformat(row["timestamp"]).timestamp())
    with pytest.raises(DataIntegrityError, match="future candle"):
        validate_intraday_rows([row], TRADING_DATE)


def test_zero_quote_volume_is_hard_failure():
    with pytest.raises(DataIntegrityError, match="zero/negative quote volume"):
        validate_live_quote({"current": 1900, "volume": 0})


def test_dhan_websocket_packet_uses_canonical_offsets():
    from main import parse_quote_packet

    packet = bytearray(50)
    packet[0] = 4
    struct.pack_into("<i", packet, 4, 123456)
    struct.pack_into("<f", packet, 8, 1900.5)
    struct.pack_into("<h", packet, 12, 7)
    struct.pack_into("<i", packet, 14, 1787802360)
    struct.pack_into("<f", packet, 18, 1900.25)
    struct.pack_into("<i", packet, 22, 1000)
    struct.pack_into("<i", packet, 26, 10)
    struct.pack_into("<i", packet, 30, 20)
    struct.pack_into("<f", packet, 34, 1890.0)
    struct.pack_into("<f", packet, 38, 1885.0)
    struct.pack_into("<f", packet, 42, 1910.0)
    struct.pack_into("<f", packet, 46, 1880.0)

    parsed = parse_quote_packet(bytes(packet))
    assert parsed[0] == 123456
    assert parsed[1] == pytest.approx(1900.5)
    assert parsed[2] == 1000
    assert parsed[3] == 1787802360
    assert parsed[4] == pytest.approx(1890.0)
    assert parsed[5] == pytest.approx(1910.0)
    assert parsed[6] == pytest.approx(1880.0)
