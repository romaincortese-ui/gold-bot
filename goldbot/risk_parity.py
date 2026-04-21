"""Cross-asset risk-parity sleeve allocator (Gold-bot Q2 §4.4).

Instead of a fixed 50/50 split between the gold sleeve and the sibling FX
sleeve, periodically rebalance so that each sleeve contributes equally to
portfolio variance:

    w_gold  ∝  1 / σ_gold
    w_fx    ∝  1 / σ_fx

with ``σ`` being the realised standard deviation of daily sleeve P&L over
the rebalance lookback window. The module is pure math on two daily-PnL
series — reading/writing the actual ``shared_budget_state.json`` allocation
is handled by the runtime caller.

The rebalance result is clamped to sane per-sleeve bounds (``min_weight``
to ``max_weight``, default 0.20..0.80) so that a quiet-vol sleeve never
dominates when the other sleeve has near-zero measured vol, and bounds
ensure neither sleeve goes fully dark.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import sqrt
from typing import Sequence


@dataclass(frozen=True, slots=True)
class SleeveVolSnapshot:
    sleeve_id: str
    realised_vol: float           # std dev of the last N daily P&L values
    observation_count: int


@dataclass(frozen=True, slots=True)
class RiskParityDecision:
    gold_weight: float
    fx_weight: float
    rebalanced: bool
    reason: str
    gold_vol: float | None
    fx_vol: float | None


def realised_daily_vol(pnl_series: Sequence[float]) -> float:
    """Population std dev of a daily-PnL series.

    Returns ``0.0`` for empty / single-point / constant series.
    """
    n = len(pnl_series)
    if n < 2:
        return 0.0
    mean = sum(pnl_series) / n
    variance = sum((x - mean) ** 2 for x in pnl_series) / n
    if variance <= 0:
        return 0.0
    return sqrt(variance)


def compute_risk_parity_weights(
    *,
    gold_pnl: Sequence[float],
    fx_pnl: Sequence[float],
    current_gold_weight: float,
    min_weight: float = 0.20,
    max_weight: float = 0.80,
    min_observations: int = 14,
    rebalance_threshold: float = 0.05,
) -> RiskParityDecision:
    """Return the recommended weights for (gold, fx).

    ``rebalance_threshold`` is the minimum absolute change from the current
    gold weight to trigger a rebalance; this prevents thrashing on tiny
    vol drifts.
    """
    gold_vol = realised_daily_vol(gold_pnl)
    fx_vol = realised_daily_vol(fx_pnl)
    gold_obs = len(gold_pnl)
    fx_obs = len(fx_pnl)

    if gold_obs < min_observations or fx_obs < min_observations:
        return RiskParityDecision(
            gold_weight=current_gold_weight,
            fx_weight=1.0 - current_gold_weight,
            rebalanced=False,
            reason="insufficient_observations",
            gold_vol=gold_vol if gold_obs >= 2 else None,
            fx_vol=fx_vol if fx_obs >= 2 else None,
        )

    if gold_vol <= 0 and fx_vol <= 0:
        return RiskParityDecision(
            gold_weight=current_gold_weight,
            fx_weight=1.0 - current_gold_weight,
            rebalanced=False,
            reason="zero_vol_both_sleeves",
            gold_vol=gold_vol,
            fx_vol=fx_vol,
        )
    if gold_vol <= 0:
        target_gold = max_weight
    elif fx_vol <= 0:
        target_gold = min_weight
    else:
        inv_gold = 1.0 / gold_vol
        inv_fx = 1.0 / fx_vol
        target_gold = inv_gold / (inv_gold + inv_fx)

    target_gold = max(min_weight, min(max_weight, target_gold))
    target_fx = 1.0 - target_gold

    if abs(target_gold - current_gold_weight) < rebalance_threshold:
        return RiskParityDecision(
            gold_weight=current_gold_weight,
            fx_weight=1.0 - current_gold_weight,
            rebalanced=False,
            reason="within_rebalance_threshold",
            gold_vol=gold_vol,
            fx_vol=fx_vol,
        )

    return RiskParityDecision(
        gold_weight=target_gold,
        fx_weight=target_fx,
        rebalanced=True,
        reason="rebalanced_to_equal_vol_contribution",
        gold_vol=gold_vol,
        fx_vol=fx_vol,
    )


def should_rebalance_now(
    *,
    last_rebalance_at: datetime | None,
    now: datetime,
    min_interval_days: int,
) -> bool:
    """True if enough time has elapsed since the last rebalance.

    ``None`` timestamps (never rebalanced) always return True.
    """
    if last_rebalance_at is None:
        return True
    now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    last_utc = (
        last_rebalance_at.astimezone(timezone.utc)
        if last_rebalance_at.tzinfo
        else last_rebalance_at.replace(tzinfo=timezone.utc)
    )
    return (now_utc - last_utc) >= timedelta(days=max(0, min_interval_days))
