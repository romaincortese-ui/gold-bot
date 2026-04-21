from datetime import datetime, timedelta, timezone

from goldbot.kill_switch import EquityHistory, evaluate_kill_switch, latch_halt_state


NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _build_history(series: list[tuple[int, float]]) -> EquityHistory:
    """Build history from [(days_ago, equity), ...] in chronological order."""
    history = EquityHistory()
    for days_ago, equity in sorted(series, reverse=True):  # oldest first
        ts = NOW - timedelta(days=days_ago)
        history.append_today(ts, equity)
    return history


def _evaluate(history: EquityHistory, equity_now: float, latched=None):
    return evaluate_kill_switch(
        history=history,
        equity_now=equity_now,
        now=NOW,
        latched_halt=latched,
        soft_window_days=30,
        soft_threshold_pct=-0.06,
        soft_risk_per_trade=0.003,
        hard_window_days=90,
        hard_threshold_pct=-0.10,
    )


def test_clear_when_equity_flat() -> None:
    history = _build_history([(90, 10_000.0), (30, 10_050.0), (0, 10_050.0)])
    decision = _evaluate(history, 10_050.0)
    assert decision.halt is False
    assert decision.soft_cut is False
    assert decision.risk_per_trade_override is None


def test_soft_cut_triggers_at_minus_six_percent() -> None:
    history = _build_history([(90, 10_000.0), (30, 10_000.0), (0, 9_300.0)])
    decision = _evaluate(history, 9_300.0)
    assert decision.halt is False
    assert decision.soft_cut is True
    assert decision.risk_per_trade_override == 0.003


def test_hard_halt_triggers_at_minus_ten_percent() -> None:
    history = _build_history([(90, 10_000.0), (30, 9_500.0), (0, 8_900.0)])
    decision = _evaluate(history, 8_900.0)
    assert decision.halt is True
    assert decision.soft_cut is False


def test_latched_halt_persists_even_if_recovered() -> None:
    history = _build_history([(90, 10_000.0), (30, 10_500.0), (0, 10_800.0)])
    latched = {"halted_at": "2026-05-01T12:00:00+00:00"}
    decision = _evaluate(history, 10_800.0, latched=latched)
    assert decision.halt is True
    assert decision.halt_latched is True


def test_latch_cleared_allows_recovery() -> None:
    history = _build_history([(90, 10_000.0), (30, 10_500.0), (0, 10_800.0)])
    latched = {
        "halted_at": "2026-05-01T12:00:00+00:00",
        "halt_cleared_at": "2026-05-20T09:00:00+00:00",
    }
    decision = _evaluate(history, 10_800.0, latched=latched)
    assert decision.halt is False
    assert decision.soft_cut is False


def test_latch_halt_state_records_halted_at_once() -> None:
    history = _build_history([(90, 10_000.0), (30, 9_500.0), (0, 8_900.0)])
    decision = _evaluate(history, 8_900.0)
    payload = latch_halt_state(None, decision, now=NOW)
    assert payload["halted_at"]
    assert payload["reason"].startswith("hard_halt")

    # Second evaluation keeps the original halted_at timestamp.
    later = NOW + timedelta(days=2)
    decision2 = evaluate_kill_switch(
        history=history,
        equity_now=8_900.0,
        now=later,
        latched_halt=payload,
        soft_window_days=30,
        soft_threshold_pct=-0.06,
        soft_risk_per_trade=0.003,
        hard_window_days=90,
        hard_threshold_pct=-0.10,
    )
    payload2 = latch_halt_state(payload, decision2, now=later)
    assert payload2["halted_at"] == payload["halted_at"]


def test_insufficient_history_returns_clear() -> None:
    # Only 3 days of history available; no way to reach back 30 or 90 days.
    history = _build_history([(2, 10_000.0), (1, 9_900.0), (0, 9_500.0)])
    decision = _evaluate(history, 9_500.0)
    assert decision.halt is False
    assert decision.soft_cut is False
