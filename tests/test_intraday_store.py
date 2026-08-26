from psy29.intraday_store import IntradayStore


def test_store_is_safe_when_database_url_is_absent(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    store = IntradayStore()
    assert store.enabled is False
    assert store.save_market("2026-08-27", {"VEDL": {"candles": {"1m": []}}}) == 0
    assert store.load_session("2026-08-27") == {}


def test_store_status_reports_disabled_without_database():
    store = IntradayStore(None)
    assert store.status()["enabled"] is False
    assert store.status()["connected"] is False


def test_store_uses_database_url_when_present(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    store = IntradayStore()
    assert store.url == "postgresql://example"
    assert store.enabled is True
