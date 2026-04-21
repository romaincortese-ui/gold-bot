import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from goldbot.cftc import (
    CFTCSignal,
    apply_cftc_overlay,
    build_cftc_signal,
    load_cftc_signal_from_macro_state,
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


def _settings_enabled(**overrides):
    base = build_settings()
    return replace(
        base,
        cftc_filter_enabled=True,
        cftc_state_max_age_days=10,
        cftc_extreme_percentile=0.85,
        cftc_extreme_score_offset=8.0,
        **overrides,
    )


def _settings_disabled():
    base = build_settings()
    return replace(
        base,
        cftc_filter_enabled=False,
        cftc_state_max_age_days=10,
        cftc_extreme_percentile=0.85,
        cftc_extreme_score_offset=8.0,
    )


def test_build_cftc_signal_returns_none_for_empty_history():
    assert build_cftc_signal([], datetime.now(timezone.utc)) is None


def test_build_cftc_signal_computes_top_percentile():
    history = [
        {"date": (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=7 * i)).isoformat(),
         "managed_money_net_pct": float(i),
         "commercial_net_short": -float(i)}
        for i in range(10)
    ]
    sig = build_cftc_signal(history, datetime(2026, 4, 1, tzinfo=timezone.utc))
    assert sig is not None
    assert sig.managed_money_net_pct == 9.0
    assert sig.managed_money_percentile_2y > 0.85  # top of the window
    assert sig.commercial_net_short_change_wow == -1.0


def test_signal_payload_roundtrip(tmp_path):
    sig = CFTCSignal(
        as_of=datetime(2026, 3, 28, 21, 30, tzinfo=timezone.utc),
        managed_money_net_pct=12.5,
        managed_money_percentile_2y=0.92,
        commercial_net_short_change_wow=-3.4,
    )
    payload = {"cftc": signal_to_payload(sig)}
    file = tmp_path / "macro.json"
    file.write_text(json.dumps(payload), encoding="utf-8")
    loaded = load_cftc_signal_from_macro_state(str(file), datetime(2026, 3, 30, tzinfo=timezone.utc), max_age_days=10)
    assert loaded is not None
    assert loaded.managed_money_percentile_2y == 0.92


def test_load_cftc_rejects_stale(tmp_path):
    sig = CFTCSignal(
        as_of=datetime(2026, 1, 1, tzinfo=timezone.utc),
        managed_money_net_pct=0.0,
        managed_money_percentile_2y=0.5,
        commercial_net_short_change_wow=0.0,
    )
    file = tmp_path / "macro.json"
    file.write_text(json.dumps({"cftc": signal_to_payload(sig)}), encoding="utf-8")
    loaded = load_cftc_signal_from_macro_state(str(file), datetime(2026, 4, 1, tzinfo=timezone.utc), max_age_days=10)
    assert loaded is None


def test_overlay_disabled_passthrough():
    s = _settings_disabled()
    opp = _opportunity("LONG")
    sig = CFTCSignal(
        as_of=datetime.now(timezone.utc),
        managed_money_net_pct=20.0,
        managed_money_percentile_2y=0.95,
        commercial_net_short_change_wow=0.0,
    )
    out = apply_cftc_overlay(s, opp, sig)
    assert out.score == 60.0


def test_overlay_fades_crowded_long():
    s = _settings_enabled()
    opp = _opportunity("LONG", score=60.0)
    sig = CFTCSignal(
        as_of=datetime.now(timezone.utc),
        managed_money_net_pct=20.0,
        managed_money_percentile_2y=0.95,
        commercial_net_short_change_wow=0.0,
    )
    out = apply_cftc_overlay(s, opp, sig)
    assert out.score == 52.0
    assert out.metadata["cftc_score_offset"] == -8.0
    assert out.metadata["cftc_filter_reason"] == "cftc_extreme_crowded_long"


def test_overlay_favours_short_when_longs_crowded():
    s = _settings_enabled()
    opp = _opportunity("SHORT", score=60.0)
    sig = CFTCSignal(
        as_of=datetime.now(timezone.utc),
        managed_money_net_pct=20.0,
        managed_money_percentile_2y=0.95,
        commercial_net_short_change_wow=0.0,
    )
    out = apply_cftc_overlay(s, opp, sig)
    assert out.score == 68.0
    assert out.metadata["cftc_score_offset"] == 8.0


def test_overlay_neutral_in_middle_of_range():
    s = _settings_enabled()
    opp = _opportunity("LONG", score=60.0)
    sig = CFTCSignal(
        as_of=datetime.now(timezone.utc),
        managed_money_net_pct=0.0,
        managed_money_percentile_2y=0.5,
        commercial_net_short_change_wow=0.0,
    )
    out = apply_cftc_overlay(s, opp, sig)
    assert out.score == 60.0
    assert "cftc_score_offset" not in out.metadata


def test_overlay_none_signal_passthrough():
    s = _settings_enabled()
    opp = _opportunity("LONG", score=60.0)
    out = apply_cftc_overlay(s, opp, None)
    assert out.score == 60.0
