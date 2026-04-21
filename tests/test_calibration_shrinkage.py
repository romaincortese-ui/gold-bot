from goldbot.calibration import (
    CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT,
    CALIBRATION_MIN_TRADES_FOR_BLOCK,
    CALIBRATION_SHRINKAGE_DENOMINATOR,
    _derive_strategy_adjustment,
    _shrink_toward_neutral,
)


def _outperformer(trades: int) -> dict:
    return {
        "trades": trades,
        "win_rate": 0.65,
        "profit_factor": 1.6,
        "expectancy": 12.0,
    }


def _underperformer(trades: int) -> dict:
    return {
        "trades": trades,
        "win_rate": 0.40,
        "profit_factor": 0.7,
        "expectancy": -3.0,
    }


def test_shrink_toward_neutral_zero_trades_collapses_to_neutral():
    mult, off = _shrink_toward_neutral(1.5, -10.0, trades=0, denominator=200)
    assert mult == 1.0
    assert off == 0.0


def test_shrink_toward_neutral_full_sample_preserves_signal():
    mult, off = _shrink_toward_neutral(1.5, -10.0, trades=200, denominator=200)
    assert mult == 1.5
    assert off == -10.0


def test_shrink_toward_neutral_half_sample_half_signal():
    mult, off = _shrink_toward_neutral(1.5, -10.0, trades=100, denominator=200)
    assert abs(mult - 1.25) < 1e-9
    assert abs(off - (-5.0)) < 1e-9


def test_below_min_trades_is_neutral():
    adj = _derive_strategy_adjustment(
        _outperformer(trades=CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT - 1),
        min_trades_for_adjustment=CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT,
        min_trades_for_block=CALIBRATION_MIN_TRADES_FOR_BLOCK,
        shrinkage_denominator=CALIBRATION_SHRINKAGE_DENOMINATOR,
    )
    assert adj == {"score_offset": 0.0, "risk_mult": 1.0, "block_reason": None}


def test_outperformer_at_low_sample_is_shrunk():
    n = CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT  # 40 / 200 = 0.2 weight
    adj = _derive_strategy_adjustment(
        _outperformer(trades=n),
        min_trades_for_adjustment=CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT,
        min_trades_for_block=CALIBRATION_MIN_TRADES_FOR_BLOCK,
        shrinkage_denominator=CALIBRATION_SHRINKAGE_DENOMINATOR,
    )
    # raw mult would be ~1.18-1.25, shrunk by 0.2 → ~1.04-1.05
    assert 1.0 < adj["risk_mult"] < 1.1
    assert adj["score_offset"] > 0
    assert adj["block_reason"] is None


def test_underperformer_below_block_threshold_is_only_dampened():
    adj = _derive_strategy_adjustment(
        _underperformer(trades=CALIBRATION_MIN_TRADES_FOR_BLOCK - 1),
        min_trades_for_adjustment=CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT,
        min_trades_for_block=CALIBRATION_MIN_TRADES_FOR_BLOCK,
        shrinkage_denominator=CALIBRATION_SHRINKAGE_DENOMINATOR,
    )
    assert adj["block_reason"] is None
    assert adj["risk_mult"] < 1.0
    assert adj["score_offset"] < 0


def test_block_only_fires_at_block_threshold():
    metrics = {
        "trades": 200,
        "win_rate": 0.20,
        "profit_factor": 0.4,
        "expectancy": -12.0,
    }
    adj = _derive_strategy_adjustment(
        metrics,
        min_trades_for_adjustment=40,
        min_trades_for_block=80,
        shrinkage_denominator=200,
    )
    assert adj["block_reason"] is not None
    assert adj["risk_mult"] == 0.5
