from datetime import datetime, timezone

import pytest

from goldbot.weekend_guard import (
    WeekendDecision,
    decision_to_metadata,
    evaluate_weekend,
    widened_stop_price,
)


def _fri(hour: int) -> datetime:
    # 2026-04-03 is a Friday (weekday=4).
    return datetime(2026, 4, 3, hour, 0, tzinfo=timezone.utc)


def _mon(hour: int) -> datetime:
    return datetime(2026, 4, 6, hour, 0, tzinfo=timezone.utc)


def _decision(now: datetime, **overrides) -> WeekendDecision:
    defaults = dict(
        enabled=True,
        flatten_weekday=4,
        flatten_hour_utc=20,
        stop_widen_enabled=True,
        stop_widen_hour_utc=19,
        block_new_entries_hour_utc=19,
    )
    defaults.update(overrides)
    return evaluate_weekend(now, **defaults)


def test_disabled_returns_no_action():
    d = _decision(_fri(20), enabled=False)
    assert d == WeekendDecision(False, False, False, "weekend_guard_disabled")


def test_non_friday_returns_no_action():
    d = _decision(_mon(20))
    assert not d.flatten and not d.widen_stops and not d.block_new_entries


def test_friday_before_widen_window_no_action():
    d = _decision(_fri(15))
    assert not d.widen_stops and not d.block_new_entries and not d.flatten


def test_friday_widen_window_widens_only():
    d = _decision(_fri(19), block_new_entries_hour_utc=20)
    assert d.widen_stops
    assert not d.flatten
    assert not d.block_new_entries


def test_friday_block_window_blocks_entries_and_widens():
    d = _decision(_fri(19))
    assert d.block_new_entries
    assert d.widen_stops
    assert not d.flatten


def test_friday_flatten_window_flattens_everything():
    d = _decision(_fri(20))
    assert d.flatten and d.widen_stops and d.block_new_entries
    assert "flatten" in d.reason


def test_widen_disabled_does_not_widen():
    d = _decision(_fri(19), stop_widen_enabled=False, block_new_entries_hour_utc=20)
    assert not d.widen_stops


def test_widened_stop_long_uses_largest_cushion():
    # gap_cushion = 2000 * 0.012 = 24; atr_cushion = 5 * 2 = 10. Gap dominates.
    new_stop = widened_stop_price(
        direction="LONG",
        entry_price=2000.0,
        current_stop=1995.0,  # 5 below
        atr=5.0,
        atr_mult=2.0,
        max_weekend_gap_pct=0.012,
    )
    assert new_stop == pytest.approx(2000.0 - 24.0)


def test_widened_stop_short_pushes_stop_up():
    new_stop = widened_stop_price(
        direction="SHORT",
        entry_price=2000.0,
        current_stop=2005.0,
        atr=5.0,
        atr_mult=2.0,
        max_weekend_gap_pct=0.012,
    )
    assert new_stop == pytest.approx(2024.0)


def test_widened_stop_never_tightens_long():
    # current stop already wider than the cushion → keep the wider stop.
    new_stop = widened_stop_price(
        direction="LONG",
        entry_price=2000.0,
        current_stop=1900.0,
        atr=5.0,
        atr_mult=2.0,
        max_weekend_gap_pct=0.012,
    )
    assert new_stop == 1900.0


def test_decision_to_metadata_serializes_all_fields():
    md = decision_to_metadata(_decision(_fri(20)))
    assert set(md.keys()) == {
        "weekend_flatten",
        "weekend_widen_stops",
        "weekend_block_new_entries",
        "weekend_reason",
    }
    assert md["weekend_flatten"] is True
