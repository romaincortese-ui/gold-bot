from goldbot.sizing import compute_risk_amount


def test_disabled_uses_legacy_cap() -> None:
    d = compute_risk_amount(
        nav=100_000.0,
        atr=5.0,
        stop_distance=5.0,
        target_nav_bps=25.0,
        legacy_max_trade_risk=500.0,
        available_gold_risk=2_000.0,
        enabled=False,
    )
    assert d.source == "disabled"
    assert d.risk_amount == 500.0


def test_zero_atr_falls_back_to_legacy() -> None:
    d = compute_risk_amount(
        nav=100_000.0,
        atr=0.0,
        stop_distance=5.0,
        target_nav_bps=25.0,
        legacy_max_trade_risk=500.0,
        available_gold_risk=2_000.0,
    )
    assert d.source == "legacy_cap"
    assert d.risk_amount == 500.0


def test_vol_target_clamps_to_legacy_cap() -> None:
    # vol_risk = 100_000 * 25 / 10_000 * (5/5) = 250 (below legacy cap 500)
    d = compute_risk_amount(
        nav=100_000.0,
        atr=5.0,
        stop_distance=5.0,
        target_nav_bps=25.0,
        legacy_max_trade_risk=500.0,
        available_gold_risk=2_000.0,
    )
    assert d.source == "vol_target"
    assert abs(d.risk_amount - 250.0) < 1e-6


def test_vol_target_never_exceeds_legacy_cap() -> None:
    # Big target_nav_bps should be clamped by legacy cap.
    d = compute_risk_amount(
        nav=100_000.0,
        atr=5.0,
        stop_distance=5.0,
        target_nav_bps=500.0,  # very aggressive
        legacy_max_trade_risk=500.0,
        available_gold_risk=2_000.0,
    )
    assert d.risk_amount == 500.0


def test_floor_prevents_sub_quarter_sizing() -> None:
    # Very low NAV -> vol_risk tiny; floor kicks in at 25% of legacy cap.
    d = compute_risk_amount(
        nav=1_000.0,
        atr=5.0,
        stop_distance=5.0,
        target_nav_bps=25.0,
        legacy_max_trade_risk=500.0,
        available_gold_risk=2_000.0,
        floor_fraction=0.25,
    )
    assert d.risk_amount >= 125.0  # 0.25 * 500


def test_available_budget_is_hard_ceiling() -> None:
    d = compute_risk_amount(
        nav=100_000.0,
        atr=5.0,
        stop_distance=5.0,
        target_nav_bps=25.0,
        legacy_max_trade_risk=500.0,
        available_gold_risk=100.0,  # nearly exhausted
    )
    assert d.risk_amount == 100.0
