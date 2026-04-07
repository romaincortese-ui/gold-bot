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
        tips_10y=2.0,
        real_yield_10y=2.0,
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