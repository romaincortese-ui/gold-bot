from datetime import datetime, timezone

import pytest

from goldbot.backtest_microstructure import (
    GOLD_HOURLY_SPREAD_MULTIPLIER,
    SpreadModel,
    exit_slippage_cost,
    financing_charge,
    hours_between,
    is_weekend_gap_boundary,
    weekend_gap_adjusted_stop,
)


@pytest.fixture
def model() -> SpreadModel:
    return SpreadModel(base_spread=0.20, news_window_minutes=2, news_multiplier=6.0)


def test_hourly_profile_tokyo_wider_than_overlap(model: SpreadModel):
    tokyo = model.effective_spread(datetime(2026, 4, 1, 1, 0, tzinfo=timezone.utc))
    overlap = model.effective_spread(datetime(2026, 4, 1, 13, 0, tzinfo=timezone.utc))
    assert tokyo > overlap
    # sanity: NY overlap multiplier is around 0.7 of base
    assert overlap == pytest.approx(0.20 * GOLD_HOURLY_SPREAD_MULTIPLIER[13])


def test_news_window_inflates_spread(model: SpreadModel):
    event = datetime(2026, 4, 1, 12, 30, tzinfo=timezone.utc)
    inside = model.effective_spread(datetime(2026, 4, 1, 12, 31, tzinfo=timezone.utc), [event])
    outside = model.effective_spread(datetime(2026, 4, 1, 12, 10, tzinfo=timezone.utc), [event])
    assert inside == pytest.approx(0.20 * 6.0)
    assert outside < inside


def test_exit_slippage_scales_with_half_spread():
    assert exit_slippage_cost(half_spread=0.1, slippage_multiplier=1.5) == pytest.approx(0.15)
    assert exit_slippage_cost(half_spread=0.0, slippage_multiplier=1.5) == 0.0


def test_weekend_gap_boundary_detects_friday_to_monday():
    fri = datetime(2026, 4, 3, 21, 0, tzinfo=timezone.utc)
    mon = datetime(2026, 4, 6, 0, 0, tzinfo=timezone.utc)
    assert is_weekend_gap_boundary(fri, mon)


def test_weekend_gap_boundary_ignores_intraday_jumps():
    a = datetime(2026, 4, 2, 1, 0, tzinfo=timezone.utc)
    b = datetime(2026, 4, 2, 5, 0, tzinfo=timezone.utc)
    assert not is_weekend_gap_boundary(a, b)


def test_weekend_gap_stops_long_below_open():
    stopped, fill = weekend_gap_adjusted_stop(
        direction="LONG",
        stop_price=2000.0,
        monday_open_price=1990.0,
        weekend_was_crossed=True,
    )
    assert stopped is True
    assert fill == 1990.0


def test_weekend_gap_does_not_stop_long_above_stop():
    stopped, fill = weekend_gap_adjusted_stop(
        direction="LONG",
        stop_price=2000.0,
        monday_open_price=2010.0,
        weekend_was_crossed=True,
    )
    assert stopped is False
    assert fill == 2000.0


def test_weekend_gap_stops_short_above_open():
    stopped, fill = weekend_gap_adjusted_stop(
        direction="SHORT",
        stop_price=2000.0,
        monday_open_price=2025.0,
        weekend_was_crossed=True,
    )
    assert stopped is True
    assert fill == 2025.0


def test_weekend_gap_no_stop_when_weekend_not_crossed():
    stopped, fill = weekend_gap_adjusted_stop(
        direction="LONG",
        stop_price=2000.0,
        monday_open_price=1900.0,
        weekend_was_crossed=False,
    )
    assert stopped is False
    assert fill == 2000.0


def test_financing_charge_long_only():
    long_cost = financing_charge(direction="LONG", notional=100_000.0, hours_held=24.0, long_apr=0.05, short_apr=0.0)
    short_cost = financing_charge(direction="SHORT", notional=100_000.0, hours_held=24.0, long_apr=0.05, short_apr=0.0)
    assert long_cost > 0
    assert short_cost == 0.0
    expected = 100_000.0 * 0.05 * (24.0 / (365.0 * 24.0))
    assert long_cost == pytest.approx(expected)


def test_financing_zero_for_no_holding_time():
    assert financing_charge(direction="LONG", notional=100_000.0, hours_held=0.0, long_apr=0.05, short_apr=0.0) == 0.0


def test_hours_between_basic():
    a = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    b = datetime(2026, 4, 2, 0, 0, tzinfo=timezone.utc)
    assert hours_between(a, b) == 12.0
    assert hours_between(b, a) == 0.0  # never negative
