from datetime import datetime, timedelta, timezone

import pandas as pd

from goldbot.real_yields import RealYieldSignal, apply_real_yield_overlay, build_real_yield_signal
from tests.test_strategies import build_settings
from goldbot.models import Opportunity


def test_build_real_yield_signal_computes_change_bps() -> None:
    now = datetime(2026, 4, 7, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        {
            "time": [now - timedelta(days=6), now - timedelta(days=1)],
            "nominal_10y": [4.1, 4.25],
            "tips_10y": [1.9, 2.05],
            "real_yield_10y": [1.9, 2.05],
        }
    )

    signal = build_real_yield_signal(frame, now, lookback_days=5)

    assert signal is not None
    assert round(signal.real_yield_change_bps or 0.0, 2) == 15.0


def test_apply_real_yield_overlay_reduces_long_risk() -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "real_yield_filter_enabled": True,
        }
    )
    opportunity = Opportunity(
        strategy="TREND_PULLBACK",
        direction="LONG",
        score=70.0,
        entry_price=3000.0,
        stop_price=2995.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )
    signal = RealYieldSignal(
        as_of=datetime(2026, 4, 7, tzinfo=timezone.utc),
        nominal_10y=4.2,
        tips_10y=1.5,
        real_yield_10y=1.5,
        real_yield_change_bps=10.0,
    )

    filtered = apply_real_yield_overlay(settings, opportunity, signal)

    assert filtered is not None
    assert filtered.metadata["risk_multiplier"] == 0.5


def test_apply_real_yield_overlay_vetoes_adverse_short() -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "real_yield_filter_enabled": True,
        }
    )
    opportunity = Opportunity(
        strategy="EXHAUSTION_REVERSAL",
        direction="SHORT",
        score=74.0,
        entry_price=3000.0,
        stop_price=3005.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )
    signal = RealYieldSignal(
        as_of=datetime(2026, 4, 7, tzinfo=timezone.utc),
        nominal_10y=4.0,
        tips_10y=1.7,
        real_yield_10y=1.7,
        real_yield_change_bps=-18.0,
    )

    assert apply_real_yield_overlay(settings, opportunity, signal) is None


def test_level_gate_vetoes_long_when_real_yields_high_and_rising() -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "real_yield_filter_enabled": True,
            "real_yield_level_gate_enabled": True,
        }
    )
    opportunity = Opportunity(
        strategy="TREND_PULLBACK",
        direction="LONG",
        score=72.0,
        entry_price=3000.0,
        stop_price=2995.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )
    signal = RealYieldSignal(
        as_of=datetime(2026, 4, 7, tzinfo=timezone.utc),
        nominal_10y=4.5,
        tips_10y=2.0,              # above 1.80 veto level
        real_yield_10y=2.0,
        real_yield_change_bps=3.0, # and still rising
    )
    assert apply_real_yield_overlay(settings, opportunity, signal) is None


def test_level_gate_vetoes_short_when_real_yields_low_and_falling() -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "real_yield_filter_enabled": True,
            "real_yield_level_gate_enabled": True,
        }
    )
    opportunity = Opportunity(
        strategy="EXHAUSTION_REVERSAL",
        direction="SHORT",
        score=72.0,
        entry_price=3000.0,
        stop_price=3005.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )
    signal = RealYieldSignal(
        as_of=datetime(2026, 4, 7, tzinfo=timezone.utc),
        nominal_10y=3.5,
        tips_10y=0.9,               # below 1.00 veto level for SHORT
        real_yield_10y=0.9,
        real_yield_change_bps=-3.0, # and still falling
    )
    assert apply_real_yield_overlay(settings, opportunity, signal) is None


def test_level_gate_passes_long_when_level_moderate() -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "real_yield_filter_enabled": True,
            "real_yield_level_gate_enabled": True,
        }
    )
    opportunity = Opportunity(
        strategy="TREND_PULLBACK",
        direction="LONG",
        score=72.0,
        entry_price=3000.0,
        stop_price=2995.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )
    signal = RealYieldSignal(
        as_of=datetime(2026, 4, 7, tzinfo=timezone.utc),
        nominal_10y=3.8,
        tips_10y=1.5,              # below 1.80 veto level
        real_yield_10y=1.5,
        real_yield_change_bps=-2.0,
    )
    out = apply_real_yield_overlay(settings, opportunity, signal)
    assert out is not None
    assert out.metadata.get("macro_filter") != "real_yield_level_veto_long"