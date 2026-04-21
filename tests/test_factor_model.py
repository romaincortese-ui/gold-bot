from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

from goldbot.factor_model import (
    GoldFactorSignal,
    apply_factor_overlay,
    compute_factor_score,
    load_factor_signal_from_macro_state,
)
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


def _signal(**overrides) -> GoldFactorSignal:
    base = dict(
        as_of=datetime.now(timezone.utc),
        tips_weekly_change_bps=-5.0,       # falling real yields — bullish
        tips_change_std_bps=5.0,
        dxy_weekly_change_pct=-0.005,      # weaker dollar — bullish
        dxy_change_std_pct=0.005,
        gld_weekly_flow_pct=0.01,          # inflows — bullish
        gld_flow_std_pct=0.01,
    )
    base.update(overrides)
    return GoldFactorSignal(**base)


def _opp(direction: str = "LONG", score: float = 50.0) -> Opportunity:
    return Opportunity(
        strategy="TREND_PULLBACK",
        direction=direction,
        score=score,
        entry_price=2500.0,
        stop_price=2480.0,
        take_profit_price=2540.0,
        risk_per_unit=20.0,
        rationale="test",
        metadata={"risk_multiplier": 1.0},
    )


def test_score_sign_flips_for_headwind_factors():
    # Rising TIPS + rising DXY + falling GLD = all three bearish for gold.
    signal = _signal(
        tips_weekly_change_bps=5.0,
        dxy_weekly_change_pct=0.005,
        gld_weekly_flow_pct=-0.01,
    )
    breakdown = compute_factor_score(signal)
    assert breakdown.aggregate < 0
    assert breakdown.tips_z < 0   # sign-flipped rising TIPS -> negative
    assert breakdown.dxy_z < 0
    assert breakdown.gld_z < 0
    assert breakdown.contributing_factor_count == 3


def test_score_bullish_all_three():
    breakdown = compute_factor_score(_signal())
    assert breakdown.aggregate > 0
    assert breakdown.contributing_factor_count == 3


def test_missing_factor_skipped_and_renormalised():
    signal = _signal(tips_weekly_change_bps=None, tips_change_std_bps=None)
    breakdown = compute_factor_score(signal)
    assert breakdown.tips_z is None
    assert breakdown.contributing_factor_count == 2
    assert breakdown.aggregate > 0


def test_no_contributing_factors_returns_zero():
    signal = _signal(
        tips_weekly_change_bps=None, tips_change_std_bps=None,
        dxy_weekly_change_pct=None, dxy_change_std_pct=None,
        gld_weekly_flow_pct=None, gld_flow_std_pct=None,
    )
    breakdown = compute_factor_score(signal)
    assert breakdown.contributing_factor_count == 0
    assert breakdown.aggregate == 0.0


def test_aggregate_clamped_to_unit_interval():
    signal = _signal(
        tips_weekly_change_bps=-100.0, tips_change_std_bps=1.0,
        dxy_weekly_change_pct=-1.0, dxy_change_std_pct=0.01,
        gld_weekly_flow_pct=1.0, gld_flow_std_pct=0.01,
    )
    breakdown = compute_factor_score(signal)
    assert -1.0 <= breakdown.aggregate <= 1.0


def test_apply_disabled_passthrough():
    settings = build_settings()
    opp = _opp()
    out = apply_factor_overlay(settings, opp, _signal())
    assert out.score == 50.0
    assert out.metadata["risk_multiplier"] == 1.0


def test_apply_aligned_long_boosts_size_and_score():
    settings = replace(build_settings(), factor_model_enabled=True)
    opp = _opp("LONG", score=50.0)
    out = apply_factor_overlay(settings, opp, _signal())
    assert out.score > 50.0
    assert out.metadata["risk_multiplier"] > 1.0
    assert out.metadata["factor_overlay_reason"] == "factor_align_boost"


def test_apply_opposed_short_reduces_size():
    settings = replace(build_settings(), factor_model_enabled=True)
    # Bullish-gold factors, but we're SHORT -> factors oppose direction.
    opp = _opp("SHORT", score=50.0)
    out = apply_factor_overlay(settings, opp, _signal())
    assert out.score < 50.0
    assert out.metadata["risk_multiplier"] < 1.0
    assert out.metadata["factor_overlay_reason"] == "factor_oppose_reduce"


def test_apply_no_signal_passthrough():
    settings = replace(build_settings(), factor_model_enabled=True)
    opp = _opp()
    before = opp.score
    out = apply_factor_overlay(settings, opp, None)
    assert out.score == before


def test_load_from_macro_state_happy(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    payload = {
        "gold_factor_model": {
            "as_of": now.isoformat(),
            "tips_weekly_change_bps": -3.0,
            "tips_change_std_bps": 5.0,
            "dxy_weekly_change_pct": -0.002,
            "dxy_change_std_pct": 0.005,
            "gld_weekly_flow_pct": 0.008,
            "gld_flow_std_pct": 0.01,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_factor_signal_from_macro_state(
        str(state_path), now, max_age_hours=168
    )
    assert signal is not None
    assert signal.tips_weekly_change_bps == -3.0


def test_load_from_macro_state_stale(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stale = now - timedelta(hours=200)
    payload = {
        "gold_factor_model": {
            "as_of": stale.isoformat(),
            "tips_weekly_change_bps": -3.0,
            "tips_change_std_bps": 5.0,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_factor_signal_from_macro_state(
        str(state_path), now, max_age_hours=168
    ) is None
