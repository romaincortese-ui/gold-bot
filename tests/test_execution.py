import pytest

from goldbot.execution import (
    ExecutionPlan,
    build_execution_plan,
    build_limit_entry_price,
    reconcile_partial_fill,
)


def test_limit_entry_long_above_mid():
    price = build_limit_entry_price(direction="LONG", bid=2000.0, ask=2000.40, spread_multiplier=1.25)
    # mid = 2000.20, half_spread = 0.20, offset = 0.25
    assert price == pytest.approx(2000.45, abs=1e-5)


def test_limit_entry_short_below_mid():
    price = build_limit_entry_price(direction="SHORT", bid=2000.0, ask=2000.40, spread_multiplier=1.25)
    assert price == pytest.approx(1999.95, abs=1e-5)


def test_limit_entry_zero_multiplier_uses_mid():
    price = build_limit_entry_price(direction="LONG", bid=100.0, ask=100.10, spread_multiplier=0.0)
    assert price == pytest.approx(100.05, abs=1e-5)


def test_limit_entry_bad_direction():
    with pytest.raises(ValueError):
        build_limit_entry_price(direction="FLAT", bid=100.0, ask=100.10, spread_multiplier=1.0)


def test_limit_entry_bad_quote():
    with pytest.raises(ValueError):
        build_limit_entry_price(direction="LONG", bid=0.0, ask=100.0, spread_multiplier=1.0)
    with pytest.raises(ValueError):
        build_limit_entry_price(direction="LONG", bid=100.0, ask=99.0, spread_multiplier=1.0)


def test_reconcile_partial_fill_half():
    adj = reconcile_partial_fill(
        requested_size=100.0,
        filled_size=40.0,
        risk_per_unit=5.0,
        original_tp_distance=20.0,
    )
    assert adj.filled_size == 40.0
    assert adj.fill_ratio == 0.4
    assert adj.adjusted_risk_amount == 200.0
    assert adj.adjusted_tp_distance == 20.0


def test_reconcile_partial_fill_clamps_overfill():
    adj = reconcile_partial_fill(
        requested_size=100.0,
        filled_size=150.0,
        risk_per_unit=5.0,
        original_tp_distance=20.0,
    )
    assert adj.filled_size == 100.0
    assert adj.fill_ratio == 1.0


def test_reconcile_partial_fill_zero_fill():
    adj = reconcile_partial_fill(
        requested_size=50.0,
        filled_size=0.0,
        risk_per_unit=5.0,
        original_tp_distance=20.0,
    )
    assert adj.fill_ratio == 0.0
    assert adj.adjusted_risk_amount == 0.0


def test_reconcile_partial_fill_rejects_bad_request():
    with pytest.raises(ValueError):
        reconcile_partial_fill(requested_size=0.0, filled_size=1.0, risk_per_unit=1.0, original_tp_distance=1.0)


def test_build_execution_plan_market():
    plan = build_execution_plan(
        use_limit_entry=False,
        direction="LONG",
        bid=2000.0,
        ask=2000.40,
        limit_spread_multiplier=1.25,
        limit_timeout_seconds=3,
        guaranteed_stop=False,
    )
    assert plan.order_type == "MARKET"
    assert plan.limit_price is None
    assert plan.time_in_force == "FOK"
    assert plan.cancel_after_seconds is None
    assert plan.guaranteed_stop is False


def test_build_execution_plan_limit_with_guaranteed_stop():
    plan = build_execution_plan(
        use_limit_entry=True,
        direction="LONG",
        bid=2000.0,
        ask=2000.40,
        limit_spread_multiplier=1.25,
        limit_timeout_seconds=3,
        guaranteed_stop=True,
    )
    assert plan.order_type == "LIMIT"
    assert plan.limit_price == pytest.approx(2000.45, abs=1e-5)
    assert plan.time_in_force == "GTD"
    assert plan.cancel_after_seconds == 3
    assert plan.guaranteed_stop is True


def test_build_execution_plan_enforces_min_timeout():
    plan = build_execution_plan(
        use_limit_entry=True,
        direction="SHORT",
        bid=2000.0,
        ask=2000.40,
        limit_spread_multiplier=1.25,
        limit_timeout_seconds=0,
        guaranteed_stop=False,
    )
    assert plan.cancel_after_seconds == 1
