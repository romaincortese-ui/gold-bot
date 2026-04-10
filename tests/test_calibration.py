from datetime import datetime, timezone

from goldbot.calibration import (
    build_calibration,
    get_strategy_adjustment,
    validate_calibration,
)


def test_build_calibration_from_report():
    report = {
        "total_trades": 5,
        "win_rate": 0.8,
        "profit_factor": 2.5,
        "total_pnl": 120.0,
        "by_strategy": {
            "TREND_PULLBACK": {
                "trades": 3,
                "win_rate": 1.0,
                "profit_factor": 999.0,
                "total_pnl": 100.0,
                "expectancy": 33.33,
            },
            "MACRO_BREAKOUT": {
                "trades": 2,
                "win_rate": 0.5,
                "profit_factor": 1.1,
                "total_pnl": 20.0,
                "expectancy": 10.0,
            },
        },
    }
    now = datetime.now(timezone.utc)
    calibration = build_calibration(
        report,
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )
    assert calibration["total_trades"] == 5
    assert "strategy_adjustments" in calibration
    assert "TREND_PULLBACK" in calibration["strategy_adjustments"]
    assert "MACRO_BREAKOUT" in calibration["strategy_adjustments"]
    # High-performing strategy should get a positive score offset
    tp = calibration["strategy_adjustments"]["TREND_PULLBACK"]
    assert tp["risk_mult"] >= 1.0
    assert tp["block_reason"] is None


def test_build_calibration_blocks_underperformer():
    report = {
        "total_trades": 20,
        "win_rate": 0.25,
        "profit_factor": 0.5,
        "total_pnl": -200.0,
        "by_strategy": {
            "EXHAUSTION_REVERSAL": {
                "trades": 20,
                "win_rate": 0.25,
                "profit_factor": 0.5,
                "total_pnl": -200.0,
                "expectancy": -10.0,
            },
        },
    }
    calibration = build_calibration(
        report,
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )
    adj = calibration["strategy_adjustments"]["EXHAUSTION_REVERSAL"]
    assert adj["block_reason"] is not None
    assert adj["risk_mult"] == 0.5


def test_validate_calibration_rejects_stale():
    data = {
        "generated_at": "2020-01-01T00:00:00+00:00",
        "total_trades": 10,
    }
    valid, reason = validate_calibration(data, max_age_hours=24.0, min_total_trades=2)
    assert not valid
    assert "stale" in reason


def test_validate_calibration_rejects_insufficient_trades():
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_trades": 0,
    }
    valid, reason = validate_calibration(data, min_total_trades=2)
    assert not valid
    assert "insufficient" in reason


def test_validate_calibration_accepts_good():
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_trades": 5,
    }
    valid, reason = validate_calibration(data)
    assert valid
    assert reason is None


def test_get_strategy_adjustment_no_calibration():
    adj = get_strategy_adjustment(None, "TREND_PULLBACK")
    assert adj["score_offset"] == 0.0
    assert adj["risk_mult"] == 1.0
    assert adj["block_reason"] is None


def test_get_strategy_adjustment_missing_strategy():
    cal = {"strategy_adjustments": {"OTHER": {"score_offset": 5.0, "risk_mult": 1.1, "block_reason": None}}}
    adj = get_strategy_adjustment(cal, "TREND_PULLBACK")
    assert adj["score_offset"] == 0.0
    assert adj["risk_mult"] == 1.0


def test_empty_report_produces_neutral_calibration():
    report = {"total_trades": 0, "win_rate": 0.0, "profit_factor": 0.0, "total_pnl": 0.0, "by_strategy": {}}
    calibration = build_calibration(
        report,
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )
    assert calibration["total_trades"] == 0
    assert calibration["strategy_adjustments"] == {}
