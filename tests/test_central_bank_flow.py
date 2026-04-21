from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

from goldbot.central_bank_flow import (
    CentralBankFlowSignal,
    apply_central_bank_short_veto,
    load_central_bank_flow_from_macro_state,
    signal_to_payload,
)
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


def _opp(strategy: str = "EXHAUSTION_REVERSAL", direction: str = "SHORT") -> Opportunity:
    return Opportunity(
        strategy=strategy,
        direction=direction,
        score=50.0,
        entry_price=2500.0,
        stop_price=2520.0,
        take_profit_price=2460.0,
        risk_per_unit=20.0,
        rationale="test",
        metadata={},
    )


def _signal(tonnes: float = 350.0) -> CentralBankFlowSignal:
    return CentralBankFlowSignal(
        as_of=datetime.now(timezone.utc),
        quarter_label="2026Q1",
        net_buying_tonnes=tonnes,
    )


def test_disabled_passthrough():
    settings = build_settings()
    out = apply_central_bank_short_veto(settings, _opp(), _signal(400.0))
    assert out is not None
    assert "central_bank_filter" not in out.metadata


def test_vetoes_exhaustion_short_when_high_demand():
    settings = replace(build_settings(), central_bank_flow_enabled=True)
    out = apply_central_bank_short_veto(settings, _opp(), _signal(400.0))
    assert out is None


def test_below_threshold_passthrough():
    settings = replace(build_settings(), central_bank_flow_enabled=True)
    out = apply_central_bank_short_veto(settings, _opp(), _signal(150.0))
    assert out is not None
    assert out.metadata["central_bank_net_tonnes"] == 150.0
    assert "central_bank_filter" not in out.metadata


def test_long_is_never_vetoed():
    settings = replace(build_settings(), central_bank_flow_enabled=True)
    out = apply_central_bank_short_veto(
        settings, _opp(direction="LONG"), _signal(500.0)
    )
    assert out is not None


def test_other_strategy_short_not_vetoed():
    settings = replace(build_settings(), central_bank_flow_enabled=True)
    out = apply_central_bank_short_veto(
        settings, _opp(strategy="TREND_PULLBACK"), _signal(500.0)
    )
    assert out is not None


def test_no_signal_passthrough():
    settings = replace(build_settings(), central_bank_flow_enabled=True)
    out = apply_central_bank_short_veto(settings, _opp(), None)
    assert out is not None


def test_load_happy(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    payload = {
        "central_bank_flow": {
            "as_of": (now - timedelta(days=30)).isoformat(),
            "quarter_label": "2026Q1",
            "net_buying_tonnes": 322.5,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_central_bank_flow_from_macro_state(
        str(state_path), now, max_age_days=100
    )
    assert signal is not None
    assert signal.net_buying_tonnes == 322.5
    assert signal.quarter_label == "2026Q1"


def test_load_stale(tmp_path: Path):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    payload = {
        "central_bank_flow": {
            "as_of": (now - timedelta(days=200)).isoformat(),
            "quarter_label": "2025Q4",
            "net_buying_tonnes": 200.0,
        }
    }
    state_path = tmp_path / "macro.json"
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_central_bank_flow_from_macro_state(
        str(state_path), now, max_age_days=100
    ) is None


def test_payload_roundtrip():
    signal = _signal(280.0)
    payload = signal_to_payload(signal)
    assert payload is not None
    assert payload["net_buying_tonnes"] == 280.0
    assert signal_to_payload(None) is None
