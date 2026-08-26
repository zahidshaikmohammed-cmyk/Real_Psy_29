from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

import pytest

from psy29.candle_history import Candle
from psy29.structure_engine import StructureError, opening_range, structure_snapshot

IST = ZoneInfo("Asia/Kolkata")


def rows():
    base = datetime(2026, 8, 26, 9, 15, tzinfo=IST)
    out = []
    values = [(100, 101, 99, 100.5), (100.5, 103, 100, 102.5), (102.5, 104, 102, 103.5),
              (103.5, 105, 103, 104.5), (104.5, 106, 104, 105.5), (105.5, 107, 105, 106.5),
              (106.5, 108, 106, 107.5), (107.5, 109, 107, 108.5), (108.5, 110, 108, 109.5),
              (109.5, 111, 109, 110.5), (110.5, 112, 110, 111.5), (111.5, 113, 111, 112.5),
              (112.5, 114, 112, 113.5), (113.5, 115, 113, 114.5), (114.5, 116, 114, 115.5)]
    for i, (o, h, l, c) in enumerate(values):
        out.append(Candle(base + timedelta(minutes=i), o, h, l, c, 1000))
    return tuple(out)


def test_opening_range_uses_915_to_before_930():
    result = opening_range(rows())
    assert result.high == 116
    assert result.low == 99
    assert result.range_size == 17
    assert result.candle_count == 15


def test_structure_tracks_session_extremes_and_previous_day_levels():
    result = structure_snapshot(rows(), previous_day_high=120, previous_day_low=90, previous_day_close=105)
    assert result.session_high == 116
    assert result.session_low == 99
    assert result.previous_day_high == 120
    assert result.previous_day_low == 90
    assert result.previous_day_close == 105


def test_empty_structure_is_neutral():
    result = structure_snapshot([])
    assert result.trend == "NEUTRAL"
    assert result.opening_range.candle_count == 0


def test_rejects_non_chronological_structure_input():
    data = list(rows())
    data[3], data[4] = data[4], data[3]
    with pytest.raises(StructureError):
        structure_snapshot(data)
