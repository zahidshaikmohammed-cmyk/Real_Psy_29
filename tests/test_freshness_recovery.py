import pytest

from psy29.freshness_recovery import FeedHealth, FreshnessPolicy, FreshnessRecoveryEngine, RecoveryAction

SYMBOLS = ("NESTLEIND", "VEDL", "AMBUJACEM")


def engine():
    e = FreshnessRecoveryEngine(SYMBOLS)
    e.on_connected(now=0.0)
    e.on_authorized()
    return e


def test_all_instruments_fresh_is_live():
    e = engine()
    e.on_tick("NESTLEIND", 1.0)
    e.on_tick("VEDL", 1.0)
    e.on_tick("AMBUJACEM", 1.0)
    result = e.observe(5.0)
    assert result.health is FeedHealth.LIVE
    assert result.action is RecoveryAction.NONE


def test_129_seconds_is_not_stale():
    e = engine()
    e.on_tick("NESTLEIND", 0.0)
    e.on_tick("VEDL", 0.0)
    e.on_tick("AMBUJACEM", 0.0)
    result = e.observe(129.0)
    assert result.health is not FeedHealth.STALE
    assert result.stale_symbols == ()


def test_greater_than_129_seconds_is_stale():
    e = engine()
    e.on_tick("NESTLEIND", 0.0)
    e.on_tick("VEDL", 0.0)
    e.on_tick("AMBUJACEM", 0.0)
    result = e.observe(129.001)
    assert result.health is FeedHealth.STALE


def test_missing_instrument_uses_rest_reconciliation_before_reconnect():
    e = engine()
    e.on_tick("NESTLEIND", 1.0)
    e.on_tick("VEDL", 1.0)
    result = e.observe(10.0)
    assert result.health is FeedHealth.DEGRADED
    assert result.action is RecoveryAction.REST_RECONCILE
    assert result.missing_symbols == ("AMBUJACEM",)


def test_long_gap_requires_reconnect():
    e = engine()
    e.on_tick("NESTLEIND", 1.0)
    e.on_tick("VEDL", 1.0)
    e.on_tick("AMBUJACEM", 1.0)
    result = e.observe(36.0)
    assert result.health is FeedHealth.STALE
    assert result.action is RecoveryAction.RECONNECT


def test_disconnect_auth_codes_require_reauthorization():
    e = engine()
    result = e.on_disconnect(807)
    assert result.health is FeedHealth.AUTH_EXPIRED
    assert result.action is RecoveryAction.REAUTHORIZE


def test_disconnect_non_auth_code_requires_reconnect():
    e = engine()
    result = e.on_disconnect(50)
    assert result.health is FeedHealth.DISCONNECTED
    assert result.action is RecoveryAction.RECONNECT


def test_reconnect_backoff_is_bounded_and_attempts_are_capped():
    policy = FreshnessPolicy(max_reconnect_attempts=2, initial_backoff_seconds=1, max_backoff_seconds=2)
    e = FreshnessRecoveryEngine(SYMBOLS, policy)
    assert e.record_reconnect_attempt() is True
    assert e.next_backoff() == 2
    assert e.record_reconnect_attempt() is True
    assert e.record_reconnect_attempt() is False


def test_policy_rejects_invalid_threshold_order():
    with pytest.raises(ValueError):
        FreshnessPolicy(live_after_seconds=10, degraded_after_seconds=5)
