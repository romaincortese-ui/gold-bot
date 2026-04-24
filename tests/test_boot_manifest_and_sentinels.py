"""Gate A G2 / G4 / G5 regression coverage (memo 1 §7).

Verifies:
- ``_record_event`` refuses to persist ``boom`` test-sentinel messages.
- ``_is_test_sentinel`` only matches exact tokens, not substrings of real
  errors.
- ``_log_boot_manifest`` emits a single ``[BOOT]`` line carrying the
  environment, execution mode, and every overlay flag; emits a WARNING
  when ``EXECUTION_MODE=signal_only`` on a ``practice`` account.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from goldbot.runtime import GoldBotRuntime


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def test_record_event_rejects_boom_sentinel(now):
    state: dict = {}
    GoldBotRuntime._record_event(state, "runtime_error", "Cycle error: boom", now=now)
    assert state.get("events", []) == []


def test_record_event_rejects_bare_boom(now):
    state: dict = {}
    GoldBotRuntime._record_event(state, "runtime_error", "boom", now=now)
    assert state.get("events", []) == []


def test_record_event_preserves_real_errors_that_contain_boom_substring(now):
    """A real OANDA error like 'HTTPError: kaboom at host' must NOT be filtered."""
    state: dict = {}
    GoldBotRuntime._record_event(state, "runtime_error", "kaboom at host", now=now)
    assert len(state["events"]) == 1


def test_record_event_preserves_normal_event(now):
    state: dict = {}
    GoldBotRuntime._record_event(
        state,
        "trade_opened",
        "TREND_PULLBACK LONG opened at 2315.5 | size 0.50 | mode live",
        now=now,
    )
    assert len(state["events"]) == 1
    assert state["events"][0]["type"] == "trade_opened"


def _make_stub_runtime(
    *, environment: str = "practice", execution_mode: str = "live"
) -> GoldBotRuntime:
    r = GoldBotRuntime.__new__(GoldBotRuntime)
    settings = MagicMock()
    settings.oanda_environment = environment
    settings.execution_mode = execution_mode
    settings.account_type = "spread_bet"
    settings.instrument = "XAU_USD"
    settings.max_entry_spread = 0.80
    settings.usd_regime_filter_enabled = True
    settings.real_yield_filter_enabled = False
    settings.breakout_volume_mode = "tick"
    settings.vol_target_sizing_enabled = True
    settings.drawdown_kill_switch_enabled = True
    settings.cftc_filter_enabled = False
    settings.co_trade_gates_enabled = False
    settings.options_iv_gate_enabled = False
    settings.miners_overlay_enabled = False
    settings.factor_model_enabled = False
    settings.central_bank_flow_enabled = False
    settings.risk_parity_enabled = False
    r.settings = settings
    return r


def test_boot_manifest_includes_core_fields(caplog):
    r = _make_stub_runtime(environment="practice", execution_mode="live")
    with caplog.at_level("INFO", logger="goldbot.runtime"):
        r._log_boot_manifest()
    boot_lines = [rec for rec in caplog.records if rec.message.startswith("[BOOT]")]
    assert len(boot_lines) == 1
    message = boot_lines[0].getMessage()
    for expected in [
        "env=practice",
        "exec_mode=live",
        "account_type=spread_bet",
        "instrument=XAU_USD",
        "usd_regime=on",
        "miners_overlay=off",
        "breakout_volume_mode=tick",
    ]:
        assert expected in message, f"missing {expected!r} in [BOOT] line: {message}"


def test_boot_manifest_warns_when_signal_only_on_practice(caplog):
    r = _make_stub_runtime(environment="practice", execution_mode="signal_only")
    with caplog.at_level("INFO", logger="goldbot.runtime"):
        r._log_boot_manifest()
    warnings = [
        rec for rec in caplog.records
        if rec.levelname == "WARNING" and "signal_only" in rec.getMessage()
    ]
    assert len(warnings) == 1, "expected exactly one WARNING about signal_only on practice"


def test_boot_manifest_silent_when_live_mode_on_practice(caplog):
    r = _make_stub_runtime(environment="practice", execution_mode="live")
    with caplog.at_level("INFO", logger="goldbot.runtime"):
        r._log_boot_manifest()
    warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert warnings == []


def test_boot_manifest_silent_when_signal_only_on_live_env(caplog):
    """signal_only on a real live account is a valid dry-run — no WARNING."""
    r = _make_stub_runtime(environment="live", execution_mode="signal_only")
    with caplog.at_level("INFO", logger="goldbot.runtime"):
        r._log_boot_manifest()
    warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert warnings == []
