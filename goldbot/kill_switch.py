"""Sprint 1 — Item 2.8: portfolio-level drawdown kill switch.

Two-stage equity-drawdown guard:

  * **Soft cut** (default: 30d return < -6%): per-trade risk is scaled to
    `drawdown_soft_risk_per_trade` (default 0.30%) until the 30-day return
    recovers above the threshold. Auto-reverts when recovery is observed.

  * **Hard halt** (default: 90d return < -10%): ALL new entries are blocked.
    Halt is persisted to state; the bot does NOT auto-resume. A human
    operator has to clear `state["kill_switch"]["halt_cleared_at"]` or
    delete the state key to re-arm. Boilerplate at any real desk.

This module is stateless: the runtime owns the equity history (one sample
per calendar day, UTC) and persists it to state.json. We only expose pure
functions that evaluate the state machine given that history.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


@dataclass(frozen=True)
class DrawdownDecision:
    """Outcome of evaluating the kill-switch against an equity curve."""
    halt: bool
    soft_cut: bool
    risk_per_trade_override: float | None  # None = use default max_risk_per_trade
    reason: str
    rolling_30d_return: float | None
    rolling_90d_return: float | None
    equity_now: float
    equity_30d_ago: float | None
    equity_90d_ago: float | None
    halt_latched: bool  # True if state already has a latched halt from a prior run


@dataclass
class EquityHistory:
    """Per-day equity samples (UTC calendar day)."""
    samples: list[dict[str, Any]] = field(default_factory=list)  # [{"date": "YYYY-MM-DD", "equity": float}]

    def append_today(self, now: datetime, equity: float) -> None:
        today_str = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
        if self.samples and self.samples[-1].get("date") == today_str:
            # overwrite same-day sample (latest intraday snapshot is most
            # representative — we want the day's closing NAV, not the open)
            self.samples[-1] = {"date": today_str, "equity": float(equity)}
        else:
            self.samples.append({"date": today_str, "equity": float(equity)})

    def trim(self, *, keep_days: int, now: datetime) -> None:
        cutoff = (now.astimezone(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        self.samples = [s for s in self.samples if s.get("date", "") >= cutoff]

    def equity_n_days_ago(self, n_days: int, *, now: datetime) -> float | None:
        """Return the equity sample closest to `n_days` ago (UTC calendar days).

        Picks the latest sample with date <= (today - n_days). Returns None if
        no such sample exists (insufficient history).
        """
        target = (now.astimezone(timezone.utc) - timedelta(days=n_days)).strftime("%Y-%m-%d")
        candidate = None
        for sample in self.samples:
            date_str = sample.get("date", "")
            if date_str <= target:
                candidate = sample
            else:
                break
        if candidate is None:
            return None
        return float(candidate.get("equity", 0.0) or 0.0)

    def to_list(self) -> list[dict[str, Any]]:
        return list(self.samples)


def evaluate_kill_switch(
    *,
    history: EquityHistory,
    equity_now: float,
    now: datetime,
    latched_halt: Mapping[str, Any] | None,
    soft_window_days: int,
    soft_threshold_pct: float,
    soft_risk_per_trade: float,
    hard_window_days: int,
    hard_threshold_pct: float,
) -> DrawdownDecision:
    """Decide whether to halt / scale risk based on rolling drawdown.

    `latched_halt` is the persisted `state["kill_switch"]` mapping from a
    prior cycle. If it contains `"halted_at"` with no `"halt_cleared_at"`,
    we stay halted regardless of current drawdown — operator must clear.
    """
    eq_now = float(equity_now) if equity_now else 0.0
    eq_soft = history.equity_n_days_ago(soft_window_days, now=now)
    eq_hard = history.equity_n_days_ago(hard_window_days, now=now)

    rolling_soft = (eq_now / eq_soft - 1.0) if (eq_soft and eq_soft > 0) else None
    rolling_hard = (eq_now / eq_hard - 1.0) if (eq_hard and eq_hard > 0) else None

    latched = bool(latched_halt) and bool(latched_halt.get("halted_at")) and not latched_halt.get("halt_cleared_at")

    if latched:
        return DrawdownDecision(
            halt=True,
            soft_cut=False,
            risk_per_trade_override=None,
            reason=f"latched_halt_from_{latched_halt.get('halted_at')}",
            rolling_30d_return=rolling_soft,
            rolling_90d_return=rolling_hard,
            equity_now=eq_now,
            equity_30d_ago=eq_soft,
            equity_90d_ago=eq_hard,
            halt_latched=True,
        )

    # Hard halt check
    if rolling_hard is not None and rolling_hard <= hard_threshold_pct:
        return DrawdownDecision(
            halt=True,
            soft_cut=False,
            risk_per_trade_override=None,
            reason=f"hard_halt_rolling{hard_window_days}d_return={rolling_hard:.2%}<={hard_threshold_pct:.2%}",
            rolling_30d_return=rolling_soft,
            rolling_90d_return=rolling_hard,
            equity_now=eq_now,
            equity_30d_ago=eq_soft,
            equity_90d_ago=eq_hard,
            halt_latched=False,
        )

    # Soft cut check
    if rolling_soft is not None and rolling_soft <= soft_threshold_pct:
        return DrawdownDecision(
            halt=False,
            soft_cut=True,
            risk_per_trade_override=float(soft_risk_per_trade),
            reason=f"soft_cut_rolling{soft_window_days}d_return={rolling_soft:.2%}<={soft_threshold_pct:.2%}",
            rolling_30d_return=rolling_soft,
            rolling_90d_return=rolling_hard,
            equity_now=eq_now,
            equity_30d_ago=eq_soft,
            equity_90d_ago=eq_hard,
            halt_latched=False,
        )

    # All clear
    return DrawdownDecision(
        halt=False,
        soft_cut=False,
        risk_per_trade_override=None,
        reason="clear",
        rolling_30d_return=rolling_soft,
        rolling_90d_return=rolling_hard,
        equity_now=eq_now,
        equity_30d_ago=eq_soft,
        equity_90d_ago=eq_hard,
        halt_latched=False,
    )


def latch_halt_state(
    existing: Mapping[str, Any] | None,
    decision: DrawdownDecision,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Return the updated `state["kill_switch"]` payload to persist.

    - On first hard-halt trigger, record `halted_at`.
    - Preserve `halt_cleared_at` if already set by an operator.
    - Always record the latest rolling-return snapshot for observability.
    """
    payload: dict[str, Any] = dict(existing or {})
    payload["last_evaluated_at"] = now.astimezone(timezone.utc).isoformat()
    payload["reason"] = decision.reason
    payload["rolling_30d_return"] = decision.rolling_30d_return
    payload["rolling_90d_return"] = decision.rolling_90d_return
    payload["soft_cut_active"] = bool(decision.soft_cut)

    if decision.halt:
        if not payload.get("halted_at") and not decision.halt_latched:
            payload["halted_at"] = now.astimezone(timezone.utc).isoformat()
        # Preserve operator's clear timestamp if any
    elif "halted_at" in payload and not payload.get("halt_cleared_at"):
        # No longer halted but no clearance recorded; keep halted_at so
        # evaluate_kill_switch's latched check still blocks new entries.
        pass
    return payload
