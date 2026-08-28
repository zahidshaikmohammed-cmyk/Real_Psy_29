from datetime import datetime
import struct

import pytest

from psy29.data_integrity import (
    DHAN_QUOTE_PACKET_FORMAT,
    DHAN_QUOTE_PACKET_SIZE,
    DataIntegrityError,
    parse_dhan_quote_packet,
    validate_intraday_rows,
    validate_live_quote,
    validate_live_tick,
    validate_quote,
    validate_tick,
)


TRADING_DATE = "2026-08-27"


def candle(ts="2026-08-27T09:15:00+05:30", epoch=1787802300, price=1900.0):
    return {"timestamp": ts, "epoch": epoch, "open": price, "high": price + 2, "low": price - 2, "close": price + 1, "volume": 100}


def test_rejects_foreign_year_candle():
    row = candle(ts="2035-08-27T09:15:00+05:30", epoch=2061171900)
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([row], TRADING_DATE)


def test_rejects_absurd_float_candle():
    row = candle(price=2.314896e-36)
    row["high"] = 2.314896e-36
    row["low"] = 2.314896e-36
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([row], TRADING_DATE)


def test_rejects_out_of_order_duplicate_candles():
    first = candle()
    second = candle(ts="2026-08-27T09:14:00+05:30", epoch=1787802240)
    with pytest.raises(DataIntegrityError):
        validate_intraday_rows([first, second], TRADING_DATE)


def test_rejects_quote_current_outside_day_range():
    with pytest.raises(DataIntegrityError):
        validate_quote({"current": 0.0001, "open": 1900, "high": 1910, "low": 1890, "close": 1880, "volume": 1000})


def test_strict_quote_rejects_impossible_ohlc():
    with pytest.raises(DataIntegrityError, match="invalid quote OHLC bounds"):
        validate_quote({"current": 1900, "open": 1900, "high": 1890, "low": 1880, "close": 1885, "volume": 1000})


def test_live_quote_keeps_ltp_separate_from_broker_aggregate_ohlc():
    live = validate_live_quote({"current": 1900, "open": 2500, "high": 2400, "low": 1000, "close": 2300, "volume": 1000})
    assert live == {"current": 1900.0, "volume": 1000}


def test_rejects_future_live_tick():
    now = datetime.fromisoformat("2026-08-27T10:00:00+05:30")
    future_epoch = int(now.timestamp()) + 30
    with pytest.raises(DataIntegrityError):
        validate_tick(1900, 1000, future_epoch, 1890, 1910, 1880, now)
    with pytest.raises(DataIntegrityError):
        validate_live_tick(1900, 1000, future_epoch, now)


def test_rejects_stale_live_tick():
    now = datetime.fromisoformat("2026-08-27T10:00:00+05:30")
    stale_epoch = int(now.timestamp()) - 301
    with pytest.raises(DataIntegrityError, match="stale live tick"):
        validate_live_tick(1900, 1000, stale_epoch, now)


def test_accepts_valid_current_session_data():
    rows = validate_intraday_rows([candle()], TRADING_DATE)
    assert rows[0]["close"] == 1901.0
    quote = validate_quote({"current": 1901, "open": 1890, "high": 1910, "low": 1880, "close": 1875, "volume": 1000})
    assert quote["current"] == 1901
    live = validate_live_tick(1901, 1000, int(datetime.fromisoformat("2026-08-27T09:16:00+05:30").timestamp()), datetime.fromisoformat("2026-08-27T09:16:02+05:30"))
    assert live[:2] == (1901.0, 1000)


def test_dhan_quote_packet_uses_zero_based_wire_offsets():
    packet = struct.pack(
        DHAN_QUOTE_PACKET_FORMAT,
        4,
        DHAN_QUOTE_PACKET_SIZE,
        1,
        1333,
        285.25,
        17,
        1787891400,
        284.75,
        123456,
        700,
        900,
        280.0,
        0.0,
        290.0,
        275.0,
    )
    parsed = parse_dhan_quote_packet(packet)
    assert parsed is not None
    security_id, ltp, volume, ltt, day_open, day_high, day_low = parsed
    assert security_id == 1333
    assert ltp == pytest.approx(285.25)
    assert volume == 123456
    assert ltt == 1787891400
    assert day_open == pytest.approx(280.0)
    assert day_high == pytest.approx(290.0)
    assert day_low == pytest.approx(275.0)


def test_dhan_quote_packet_rejects_bad_framing_and_wrong_segment():
    packet = struct.pack(
        DHAN_QUOTE_PACKET_FORMAT,
        4,
        DHAN_QUOTE_PACKET_SIZE,
        2,
        1333,
        285.25,
        17,
        1787891400,
        284.75,
        123456,
        700,
        900,
        280.0,
        0.0,
        290.0,
        275.0,
    )
    assert parse_dhan_quote_packet(packet) is None
    bad_length = bytearray(packet)
    bad_length[1:3] = struct.pack("<H", 49)
    assert parse_dhan_quote_packet(bytes(bad_length)) is None
    assert parse_dhan_quote_packet(packet[:DHAN_QUOTE_PACKET_SIZE - 1]) is None
