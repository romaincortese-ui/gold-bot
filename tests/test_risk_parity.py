from datetime import datetime, timedelta, timezone
from math import isclose

from goldbot.risk_parity import (
    compute_risk_parity_weights,
    realised_daily_vol,
    should_rebalance_now,
)


def test_realised_daily_vol_empty_and_single():
    assert realised_daily_vol([]) == 0.0
    assert realised_daily_vol([1.0]) == 0.0
    assert realised_daily_vol([1.0, 1.0, 1.0]) == 0.0


def test_realised_daily_vol_positive():
    vol = realised_daily_vol([1.0, -1.0, 1.0, -1.0])
    assert isclose(vol, 1.0, abs_tol=1e-9)


def test_weights_equal_vol_contribution():
    # Both sleeves have equal vol -> 50/50 weights.
    gold = [0.1, -0.1] * 10
    fx = [0.1, -0.1] * 10
    decision = compute_risk_parity_weights(
        gold_pnl=gold, fx_pnl=fx, current_gold_weight=0.3
    )
    assert decision.rebalanced
    assert isclose(decision.gold_weight, 0.5, abs_tol=1e-9)
    assert isclose(decision.fx_weight, 0.5, abs_tol=1e-9)


def test_weights_tilt_away_from_high_vol_sleeve():
    # Gold vol is 2x fx vol -> gold weight should be lower.
    gold = [0.2, -0.2] * 10
    fx = [0.1, -0.1] * 10
    decision = compute_risk_parity_weights(
        gold_pnl=gold, fx_pnl=fx, current_gold_weight=0.5
    )
    assert decision.gold_weight < 0.5
    assert decision.fx_weight > 0.5


def test_weights_clamped_to_bounds():
    # Gold vol near zero -> gold would get ~100% weight but clamped to max.
    gold = [1e-9, -1e-9] * 10
    fx = [0.5, -0.5] * 10
    decision = compute_risk_parity_weights(
        gold_pnl=gold, fx_pnl=fx, current_gold_weight=0.5,
        min_weight=0.20, max_weight=0.80,
    )
    assert decision.gold_weight <= 0.80


def test_insufficient_observations():
    decision = compute_risk_parity_weights(
        gold_pnl=[0.1] * 5, fx_pnl=[0.1] * 5, current_gold_weight=0.5,
    )
    assert not decision.rebalanced
    assert decision.reason == "insufficient_observations"
    assert decision.gold_weight == 0.5


def test_within_rebalance_threshold_no_change():
    gold = [0.1, -0.1] * 10
    fx = [0.1, -0.1] * 10
    # Current weight already at target -> no rebalance.
    decision = compute_risk_parity_weights(
        gold_pnl=gold, fx_pnl=fx, current_gold_weight=0.5,
        rebalance_threshold=0.05,
    )
    assert not decision.rebalanced
    assert decision.reason == "within_rebalance_threshold"


def test_zero_vol_both_sleeves():
    zeros = [0.0] * 30
    decision = compute_risk_parity_weights(
        gold_pnl=zeros, fx_pnl=zeros, current_gold_weight=0.4,
    )
    assert not decision.rebalanced
    assert decision.gold_weight == 0.4


def test_should_rebalance_now_first_time():
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    assert should_rebalance_now(
        last_rebalance_at=None, now=now, min_interval_days=7
    )


def test_should_rebalance_now_too_soon():
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    last = now - timedelta(days=3)
    assert not should_rebalance_now(
        last_rebalance_at=last, now=now, min_interval_days=7
    )


def test_should_rebalance_now_elapsed():
    now = datetime(2026, 4, 15, tzinfo=timezone.utc)
    last = now - timedelta(days=8)
    assert should_rebalance_now(
        last_rebalance_at=last, now=now, min_interval_days=7
    )
