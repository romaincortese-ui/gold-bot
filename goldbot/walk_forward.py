"""Walk-forward calibration stability filter (Sprint 3 §3.6).

Replaces the current rolling-180-day recalibration (a form of curve fit)
with a proper walk-forward pipeline:

* Optimise on ``in_sample_days`` days, hold out ``out_sample_days`` days for
  validation, step forward by ``step_days``.
* Only accept parameter sets whose out-of-sample PF > ``min_out_sample_pf``
  AND whose out-of-sample PF is not more than ``max_pf_degradation`` worse
  than in-sample (filters curve-fit artefacts).

This module is pure math on ``(date, pf, trade_count)`` tuples — the
actual optimisation loop lives in ``run_daily_calibration.py``. The split
generator and the stability judge can be unit-tested in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass(frozen=True, slots=True)
class WalkForwardSplit:
    in_sample_start: datetime
    in_sample_end: datetime
    out_sample_start: datetime
    out_sample_end: datetime


def generate_walk_forward_splits(
    start: datetime,
    end: datetime,
    *,
    in_sample_days: int,
    out_sample_days: int,
    step_days: int,
) -> list[WalkForwardSplit]:
    """Generate non-overlapping out-of-sample windows across ``[start, end)``.

    Each split has a training window of length ``in_sample_days`` ending at
    the split's ``out_sample_start``, then an out-of-sample window of length
    ``out_sample_days``. Splits advance by ``step_days``; the final split is
    included only if a full out-of-sample window fits before ``end``.
    """
    if in_sample_days <= 0 or out_sample_days <= 0 or step_days <= 0:
        raise ValueError("walk-forward day counts must all be > 0")
    start_utc = _ensure_utc(start)
    end_utc = _ensure_utc(end)
    if end_utc <= start_utc:
        return []

    splits: list[WalkForwardSplit] = []
    in_delta = timedelta(days=in_sample_days)
    out_delta = timedelta(days=out_sample_days)
    step_delta = timedelta(days=step_days)

    cursor = start_utc + in_delta
    while cursor + out_delta <= end_utc:
        splits.append(
            WalkForwardSplit(
                in_sample_start=cursor - in_delta,
                in_sample_end=cursor,
                out_sample_start=cursor,
                out_sample_end=cursor + out_delta,
            )
        )
        cursor += step_delta
    return splits


@dataclass(frozen=True, slots=True)
class StabilityResult:
    passed: bool
    reason: str
    in_sample_pf: float
    out_sample_pf: float
    degradation: float        # 0.5 means OOS is 50% worse than IS


def evaluate_stability(
    *,
    in_sample_pf: float,
    out_sample_pf: float,
    min_out_sample_pf: float,
    max_pf_degradation: float,
) -> StabilityResult:
    """Judge whether a parameter set is stable enough to ship.

    ``degradation`` is ``1 - out_sample_pf / in_sample_pf`` clamped into
    ``[-inf, 1]`` — positive values mean OOS is worse than IS, negative
    means OOS is actually better (rare, not a reason to reject).
    """
    if in_sample_pf <= 0:
        return StabilityResult(
            passed=False,
            reason="non_positive_in_sample_pf",
            in_sample_pf=in_sample_pf,
            out_sample_pf=out_sample_pf,
            degradation=float("inf"),
        )
    if out_sample_pf < min_out_sample_pf:
        degradation = 1.0 - (out_sample_pf / in_sample_pf)
        return StabilityResult(
            passed=False,
            reason="out_sample_pf_below_minimum",
            in_sample_pf=in_sample_pf,
            out_sample_pf=out_sample_pf,
            degradation=degradation,
        )
    degradation = 1.0 - (out_sample_pf / in_sample_pf)
    if degradation > max_pf_degradation:
        return StabilityResult(
            passed=False,
            reason="excessive_pf_degradation",
            in_sample_pf=in_sample_pf,
            out_sample_pf=out_sample_pf,
            degradation=degradation,
        )
    return StabilityResult(
        passed=True,
        reason="stable",
        in_sample_pf=in_sample_pf,
        out_sample_pf=out_sample_pf,
        degradation=degradation,
    )


def aggregate_out_sample_pf(results: Iterable[tuple[float, int]]) -> float:
    """Weight-average per-split OOS PFs by trade count.

    ``results`` is an iterable of ``(pf, trade_count)``. Returns ``0.0``
    when total trade count is zero (which also indicates the parameter set
    should be rejected upstream).
    """
    total_trades = 0
    weighted_sum = 0.0
    for pf, n in results:
        if n <= 0 or pf <= 0:
            continue
        total_trades += int(n)
        weighted_sum += float(pf) * int(n)
    if total_trades == 0:
        return 0.0
    return weighted_sum / total_trades


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
