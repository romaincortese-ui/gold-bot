import json
import math
from datetime import datetime, timezone

import pytest

from goldbot.options_iv import (
    build_options_iv_signal,
    evaluate_options_iv_gate,
    load_options_iv_signal_from_macro_state,
    signal_to_payload,
    should_gate_strategy,
)
from goldbot.config import Settings
from tests.test_strategies import build_settings
from dataclasses import replace


def test_build_signal_derives_implied_1d():
    as_of = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    signal = build_options_iv_signal(0.15, as_of)
    assert signal is not None
    expected = 0.15 * math.sqrt(1.0 / 252)
    assert math.isclose(signal.implied_1d_move_pct, expected, rel_tol=1e-6)


def test_build_signal_rejects_non_positive_iv():
    as_of = datetime(2026, 4, 10, tzinfo=timezone.utc)
    assert build_options_iv_signal(0.0, as_of) is None
    assert build_options_iv_signal(-0.1, as_of) is None


def test_gate_passes_when_realised_above_threshold():
    result = evaluate_options_iv_gate(
        realised_move_pct=0.009,
        implied_1d_move_pct=0.010,
        threshold_fraction=0.60,
    )
    assert result.passed is True
    assert result.ratio == pytest.approx(0.9)


def test_gate_fails_when_realised_below_threshold():
    result = evaluate_options_iv_gate(
        realised_move_pct=0.003,
        implied_1d_move_pct=0.010,
        threshold_fraction=0.60,
    )
    assert result.passed is False
    assert result.reason == "realised_below_threshold_fraction"


def test_gate_passes_when_implied_zero():
    result = evaluate_options_iv_gate(
        realised_move_pct=0.001,
        implied_1d_move_pct=0.0,
        threshold_fraction=0.60,
    )
    assert result.passed is True
    assert result.reason == "no_implied_move_data"


def test_gate_uses_abs_value_of_realised_move():
    result = evaluate_options_iv_gate(
        realised_move_pct=-0.009,
        implied_1d_move_pct=0.010,
        threshold_fraction=0.60,
    )
    assert result.passed is True
    assert result.ratio == pytest.approx(0.9)


def test_should_gate_strategy_enabled_only_for_macro_breakout():
    settings = replace(build_settings(), options_iv_gate_enabled=True)
    assert should_gate_strategy(settings, "MACRO_BREAKOUT")
    assert not should_gate_strategy(settings, "TREND_PULLBACK")
    assert not should_gate_strategy(settings, "EXHAUSTION_REVERSAL")


def test_should_gate_strategy_disabled_returns_false():
    settings = replace(build_settings(), options_iv_gate_enabled=False)
    assert not should_gate_strategy(settings, "MACRO_BREAKOUT")


def test_load_options_iv_from_macro_state(tmp_path):
    path = tmp_path / "macro.json"
    now = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    payload = {
        "options_iv": {
            "as_of": "2026-04-10T10:00:00+00:00",
            "atm_iv_1m": 0.18,
            "implied_1d_move_pct": 0.01134,
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    signal = load_options_iv_signal_from_macro_state(str(path), now, max_age_hours=24)
    assert signal is not None
    assert signal.atm_iv_1m == 0.18
    assert math.isclose(signal.implied_1d_move_pct, 0.01134, rel_tol=1e-6)


def test_load_options_iv_rejects_stale(tmp_path):
    path = tmp_path / "macro.json"
    now = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    payload = {
        "options_iv": {
            "as_of": "2026-04-05T10:00:00+00:00",
            "atm_iv_1m": 0.18,
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_options_iv_signal_from_macro_state(str(path), now, max_age_hours=24) is None


def test_signal_to_payload_roundtrip():
    signal = build_options_iv_signal(0.20, datetime(2026, 4, 1, tzinfo=timezone.utc))
    payload = signal_to_payload(signal)
    assert payload["atm_iv_1m"] == 0.20
    assert payload["implied_1d_move_pct"] > 0
