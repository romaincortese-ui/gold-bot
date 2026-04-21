from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

from goldbot.miners_overlay import (
    MinersSignal,
    apply_miners_overlay,
    load_miners_signal_from_macro_state,
    signal_to_payload,
)
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


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


def test_disabled_passthrough():
    settings = build_settings()
    signal = MinersSignal(
        as_of=datetime.now(timezone.utc),
        gdx_daily_change_pct=0.02,
        nem_daily_change_pct=0.02,
        gld_shares_outstanding_change_pct=0.01,
    )
    opp = _opp()
    before = opp.score
    out = apply_miners_overlay(settings, opp, signal)
    assert out.score == before
    assert out.metadata["risk_multiplier"] == 1.0


def test_no_signal_passthrough():
    settings = replace(build_settings(), miners_overlay_enabled=True)
    opp = _opp()
    before = opp.score
    out = apply_miners_overlay(settings, opp, None)
    assert out.score == before


def test_long_confirm_boosts_score_and_size():
    settings = replace(build_settings(), miners_overlay_enabled=True)
    signal = MinersSignal(
        as_of=datetime.now(timezone.utc),
        gdx_daily_change_pct=0.02,   # 2%
        nem_daily_change_pct=0.02,
        gld_shares_outstanding_change_pct=None,
    )
    opp = _opp("LONG", score=50.0)
    out = apply_miners_overlay(settings, opp, signal, gold_daily_change_pct=0.005)
    # divergence = (0.02*0.7 + 0.02*0.3) - 0.005 = 0.015 >= 0.005 threshold
    assert out.score > 50.0
    assert out.metadata["risk_multiplier"] > 1.0
    assert out.metadata["miners_overlay_reason"].startswith("miners_confirm_long")


def test_short_confirm_boosts_score():
    settings = replace(build_settings(), miners_overlay_enabled=True)
    signal = MinersSignal(
        as_of=datetime.now(timezone.utc),
        gdx_daily_change_pct=-0.03,
        nem_daily_change_pct=-0.02,
        gld_shares_outstanding_change_pct=None,
    )
    opp = _opp("SHORT", score=50.0)
    out = apply_miners_overlay(settings, opp, signal, gold_daily_change_pct=0.0)
    assert out.score > 50.0
    assert "miners_confirm_short" in out.metadata["miners_overlay_reason"]
    # short never gets the LONG-only size boost
    assert out.metadata["risk_multiplier"] == 1.0


def test_long_divergence_fade_reduces_score():
    settings = replace(build_settings(), miners_overlay_enabled=True)
    signal = MinersSignal(
        as_of=datetime.now(timezone.utc),
        gdx_daily_change_pct=-0.02,
        nem_daily_change_pct=-0.02,
        gld_shares_outstanding_change_pct=None,
    )
    opp = _opp("LONG", score=50.0)
    out = apply_miners_overlay(settings, opp, signal, gold_daily_change_pct=0.005)
    assert out.score < 50.0
    assert "miners_divergence_fade_long" in out.metadata["miners_overlay_reason"]


def test_etf_flow_adds_score_for_long():
    settings = replace(build_settings(), miners_overlay_enabled=True)
    signal = MinersSignal(
        as_of=datetime.now(timezone.utc),
        gdx_daily_change_pct=None,
        nem_daily_change_pct=None,
        gld_shares_outstanding_change_pct=0.01,
    )
    opp = _opp("LONG", score=50.0)
    out = apply_miners_overlay(settings, opp, signal)
    assert out.score > 50.0
    assert "etf_flow" in out.metadata["miners_overlay_reason"]


def test_load_from_macro_state_happy(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    payload = {
        "miners": {
            "as_of": now.isoformat(),
            "gdx_daily_change_pct": 0.015,
            "nem_daily_change_pct": 0.01,
            "gld_shares_outstanding_change_pct": 0.003,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_miners_signal_from_macro_state(
        str(state_path), now, max_age_hours=24
    )
    assert signal is not None
    assert signal.gdx_daily_change_pct == 0.015


def test_load_from_macro_state_stale(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stale = now - timedelta(hours=48)
    payload = {
        "miners": {
            "as_of": stale.isoformat(),
            "gdx_daily_change_pct": 0.015,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_miners_signal_from_macro_state(
        str(state_path), now, max_age_hours=24
    )
    assert signal is None


def test_signal_payload_roundtrip():
    signal = MinersSignal(
        as_of=datetime(2026, 4, 15, tzinfo=timezone.utc),
        gdx_daily_change_pct=0.01,
        nem_daily_change_pct=0.005,
        gld_shares_outstanding_change_pct=0.002,
    )
    payload = signal_to_payload(signal)
    assert payload is not None
    assert payload["gdx_daily_change_pct"] == 0.01
    assert signal_to_payload(None) is None
