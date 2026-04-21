from dataclasses import replace
from datetime import datetime, timezone
import json

from goldbot.co_trade import (
    CoTradeSignal,
    apply_co_trade_gates,
    load_co_trade_signal_from_macro_state,
    signal_to_payload,
)
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


def _opportunity(direction: str, score: float = 60.0) -> Opportunity:
    return Opportunity(
        strategy="TREND_PULLBACK",
        direction=direction,
        score=score,
        entry_price=2000.0,
        stop_price=1990.0 if direction == "LONG" else 2010.0,
        take_profit_price=2020.0 if direction == "LONG" else 1980.0,
        risk_per_unit=10.0,
        rationale="test",
    )


def _settings(**overrides):
    base = build_settings()
    return replace(
        base,
        co_trade_gates_enabled=True,
        co_trade_state_max_age_hours=24,
        co_trade_es_risk_on_long_veto_pct=0.015,
        co_trade_cnh_stress_short_veto_pct=0.004,
        co_trade_dxy_weak_favourable_pct=-0.003,
        co_trade_favourable_size_mult=1.25,
        **overrides,
    )


def _signal(**fields):
    base = {
        "as_of": datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc),
        "es_daily_change_pct": 0.0,
        "cnh_daily_change_pct": 0.0,
        "dxy_daily_change_pct": 0.0,
    }
    base.update(fields)
    return CoTradeSignal(**base)


def test_co_trade_disabled_is_passthrough():
    settings = replace(build_settings(), co_trade_gates_enabled=False)
    opp = _opportunity("LONG")
    signal = _signal(es_daily_change_pct=0.03)
    assert apply_co_trade_gates(settings, opp, signal) is opp


def test_co_trade_no_signal_is_passthrough():
    settings = _settings()
    opp = _opportunity("LONG")
    assert apply_co_trade_gates(settings, opp, None) is opp


def test_co_trade_risk_on_vetoes_long():
    settings = _settings()
    opp = _opportunity("LONG")
    signal = _signal(es_daily_change_pct=0.02)  # +2% ES
    assert apply_co_trade_gates(settings, opp, signal) is None
    assert opp.metadata["co_trade_filter"] == "risk_on_equity_long_veto"


def test_co_trade_risk_on_does_not_veto_short():
    settings = _settings()
    opp = _opportunity("SHORT")
    signal = _signal(es_daily_change_pct=0.02)
    assert apply_co_trade_gates(settings, opp, signal) is opp


def test_co_trade_cnh_stress_vetoes_short():
    settings = _settings()
    opp = _opportunity("SHORT")
    signal = _signal(cnh_daily_change_pct=0.006)  # +0.6% USD/CNH
    assert apply_co_trade_gates(settings, opp, signal) is None
    assert opp.metadata["co_trade_filter"] == "cnh_stress_short_veto"


def test_co_trade_cnh_stress_does_not_veto_long():
    settings = _settings()
    opp = _opportunity("LONG")
    signal = _signal(cnh_daily_change_pct=0.006)
    result = apply_co_trade_gates(settings, opp, signal)
    assert result is opp


def test_co_trade_weak_dxy_boosts_long_size():
    settings = _settings()
    opp = _opportunity("LONG")
    opp.metadata["risk_multiplier"] = 1.0
    signal = _signal(dxy_daily_change_pct=-0.005)  # DXY -0.5%
    result = apply_co_trade_gates(settings, opp, signal)
    assert result is opp
    assert result.metadata["risk_multiplier"] == 1.25
    assert result.metadata["co_trade_filter"] == "dxy_weak_long_boost"


def test_co_trade_weak_dxy_does_not_boost_short():
    settings = _settings()
    opp = _opportunity("SHORT")
    opp.metadata["risk_multiplier"] = 1.0
    signal = _signal(dxy_daily_change_pct=-0.005)
    result = apply_co_trade_gates(settings, opp, signal)
    assert result is opp
    assert result.metadata.get("risk_multiplier", 1.0) == 1.0


def test_co_trade_load_from_macro_state(tmp_path):
    path = tmp_path / "macro.json"
    now = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    payload = {
        "co_trade": {
            "as_of": "2026-04-10T13:30:00+00:00",
            "es_daily_change_pct": 0.012,
            "cnh_daily_change_pct": -0.001,
            "dxy_daily_change_pct": 0.002,
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_co_trade_signal_from_macro_state(str(path), now, max_age_hours=24)
    assert signal is not None
    assert signal.es_daily_change_pct == 0.012
    assert signal.cnh_daily_change_pct == -0.001
    assert signal.dxy_daily_change_pct == 0.002


def test_co_trade_stale_payload_returns_none(tmp_path):
    path = tmp_path / "macro.json"
    now = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    payload = {
        "co_trade": {
            "as_of": "2026-04-05T13:30:00+00:00",
            "es_daily_change_pct": 0.0,
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_co_trade_signal_from_macro_state(str(path), now, max_age_hours=24) is None


def test_signal_to_payload_roundtrip():
    signal = _signal(es_daily_change_pct=0.012, cnh_daily_change_pct=-0.002, dxy_daily_change_pct=0.004)
    payload = signal_to_payload(signal)
    assert payload["es_daily_change_pct"] == 0.012
    assert payload["cnh_daily_change_pct"] == -0.002
    assert payload["dxy_daily_change_pct"] == 0.004
    assert payload["as_of"] == signal.as_of.isoformat()
