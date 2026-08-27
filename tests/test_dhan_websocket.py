import struct

from psy29.dhan_websocket import (
    parse_disconnect_packet,
    parse_quote_packet,
    subscription_messages,
    websocket_url,
)
from psy29.instrument_registry import Instrument, InstrumentRegistry


def registry():
    return InstrumentRegistry((Instrument("NESTLEIND", "123"), Instrument("VEDL", "456")))


def quote_packet():
    # Dhan v2 Quote packet: 8-byte header + documented 42-byte payload.
    header = struct.pack("<BhBi", 4, 50, 0, 123)
    body = struct.pack(
        "<fhiffiiiffff",
        2500.5, 10, 1720000000, 2501.0, 100000, 2000, 3000,
        2490.0, 2505.0, 2510.0, 2480.0,
    )
    assert len(header + body) == 50
    return header + body


def test_websocket_url():
    assert websocket_url("token", "client").startswith("wss://api-feed.dhan.co?version=2")


def test_subscription_uses_quote_mode_and_all_instruments():
    messages = subscription_messages(registry())
    assert len(messages) == 1
    assert messages[0]["RequestCode"] == 17
    assert messages[0]["InstrumentCount"] == 2
    assert messages[0]["InstrumentList"][0]["SecurityId"] == "123"


def test_quote_packet_decodes_documented_fields():
    tick = parse_quote_packet(quote_packet(), registry())
    assert tick is not None
    assert tick.symbol == "NESTLEIND"
    assert tick.ltp == 2500.5
    assert tick.last_trade_quantity == 10
    assert tick.last_trade_epoch == 1720000000
    assert tick.average_trade_price == 2501.0
    assert tick.volume == 100000
    assert tick.total_sell_quantity == 2000
    assert tick.total_buy_quantity == 3000
    assert tick.day_open == 2490.0
    assert tick.day_close == 2505.0
    assert tick.day_high == 2510.0
    assert tick.day_low == 2480.0


def test_quote_packet_rejects_invalid_price_decode():
    payload = bytearray(quote_packet())
    payload[8:12] = struct.pack("<f", -1.0)
    assert parse_quote_packet(bytes(payload), registry()) is None


def test_unknown_security_is_ignored():
    payload = bytearray(quote_packet())
    payload[4:8] = struct.pack("<i", 999)
    assert parse_quote_packet(bytes(payload), registry()) is None


def test_disconnect_packet_decodes_reason():
    payload = struct.pack("<BhBi h", 50, 10, 0, 123, 807)
    event = parse_disconnect_packet(payload)
    assert event is not None
    assert event.reason_code == 807
