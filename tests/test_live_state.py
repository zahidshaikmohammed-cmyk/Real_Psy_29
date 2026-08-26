from psy29.dhan_websocket import QuoteTick
from psy29.instrument_registry import Instrument, InstrumentRegistry
from psy29.live_state import LiveTickStateEngine


def registry():
    return InstrumentRegistry((Instrument("NESTLEIND", "123"), Instrument("VEDL", "456")))


def tick(symbol="NESTLEIND", security_id="123", epoch=100):
    return QuoteTick(symbol, security_id, 2500.5, 10, epoch, 2501.0, 100000, 2000, 3000, 2490.0, 2505.0, 2510.0, 2480.0)


def test_applies_latest_quote():
    engine = LiveTickStateEngine(registry())
    assert engine.apply_quote(tick(), 101.0)
    state = engine.get("NESTLEIND")
    assert state["current_price"] == 2500.5
    assert state["last_trade_epoch"] == 100
    assert state["update_count"] == 1


def test_rejects_older_trade_update():
    engine = LiveTickStateEngine(registry())
    assert engine.apply_quote(tick(epoch=100), 101.0)
    assert not engine.apply_quote(tick(epoch=99), 102.0)
    assert engine.get("NESTLEIND")["current_price"] == 2500.5


def test_rejects_wrong_symbol_security_mapping():
    engine = LiveTickStateEngine(registry())
    assert not engine.apply_quote(tick(symbol="VEDL", security_id="123"), 101.0)


def test_complete_requires_all_instruments():
    engine = LiveTickStateEngine(registry())
    assert not engine.complete()
    assert engine.apply_quote(tick(), 101.0)
    assert not engine.complete()
    assert engine.apply_quote(tick("VEDL", "456"), 101.0)
    assert engine.complete()


def test_reset_clears_session_state():
    engine = LiveTickStateEngine(registry())
    engine.apply_quote(tick(), 101.0)
    engine.reset()
    state = engine.get("NESTLEIND")
    assert state["current_price"] is None
    assert state["update_count"] == 0
