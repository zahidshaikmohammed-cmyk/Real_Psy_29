from psy29.collector_supervisor import CollectorSupervisor


def test_no_tick_is_not_fresh():
    assert not CollectorSupervisor().feed_is_fresh(10.0)


def test_recent_tick_is_fresh():
    s = CollectorSupervisor()
    s.record_tick(10.0)
    assert s.feed_is_fresh(44.0)


def test_stale_tick_is_not_fresh():
    s = CollectorSupervisor()
    s.record_tick(10.0)
    assert not s.feed_is_fresh(45.1)
