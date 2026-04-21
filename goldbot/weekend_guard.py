"""Weekend gap handling for XAU_USD.

Gold trades close ~22:00 UTC Friday and reopen ~22:00 UTC Sunday. Sunday-open
gaps of 0.3-1.2% are routine, blowing past Friday-close stops at materially
worse fills. This module provides three primitives the runtime/backtest can
consume:

  * ``evaluate_weekend(now, settings)`` -> :class:`WeekendDecision`
      Tells the caller whether to flatten all positions, widen stops in
      anticipation of the weekend, and/or refuse new entries.

  * ``widened_stop_price(...)`` -> ``float``
      Computes a wider stop price covering a configurable max-gap-percent +
      ATR cushion, used Friday afternoon before the close.

The module is pure (no I/O) so it is trivial to unit-test.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class WeekendDecision:
    flatten: bool
    widen_stops: bool
    block_new_entries: bool
    reason: str


def evaluate_weekend(
    now: datetime,
    *,
    enabled: bool,
    flatten_weekday: int,
    flatten_hour_utc: int,
    stop_widen_enabled: bool,
    stop_widen_hour_utc: int,
    block_new_entries_hour_utc: int,
) -> WeekendDecision:
    """Decide which weekend protections apply at ``now`` (UTC).

    All hour comparisons are inclusive lower-bounds (>=). ``flatten`` implies
    ``block_new_entries`` and ``widen_stops`` since by the time we want to
    flatten, we definitely don't want to open new exposure or run with a
    tight stop.
    """
    if not enabled:
        return WeekendDecision(False, False, False, "weekend_guard_disabled")

    if now.weekday() != int(flatten_weekday):
        return WeekendDecision(False, False, False, "not_flatten_weekday")

    hour = int(now.hour)
    if hour >= int(flatten_hour_utc):
        return WeekendDecision(
            flatten=True,
            widen_stops=True,
            block_new_entries=True,
            reason="weekend_flatten_window",
        )
    if hour >= int(block_new_entries_hour_utc):
        return WeekendDecision(
            flatten=False,
            widen_stops=stop_widen_enabled and hour >= int(stop_widen_hour_utc),
            block_new_entries=True,
            reason="weekend_pre_close_block_entries",
        )
    if stop_widen_enabled and hour >= int(stop_widen_hour_utc):
        return WeekendDecision(
            flatten=False,
            widen_stops=True,
            block_new_entries=False,
            reason="weekend_stop_widen_window",
        )
    return WeekendDecision(False, False, False, "outside_weekend_windows")


def widened_stop_price(
    *,
    direction: str,
    entry_price: float,
    current_stop: float,
    atr: float,
    atr_mult: float,
    max_weekend_gap_pct: float,
) -> float:
    """Return a stop price wide enough to absorb a typical Sunday-open gap.

    The widened stop is the further of:

      * ``current_stop`` (never tightens an existing stop), and
      * a stop set ``max(atr * atr_mult, entry * max_weekend_gap_pct)`` away
        from the entry on the loss side.

    For LONG, "further" means lower (more loss tolerance). For SHORT, higher.
    """
    direction = (direction or "").upper()
    cushion = max(float(atr) * float(atr_mult), float(entry_price) * float(max_weekend_gap_pct))
    if direction == "LONG":
        candidate = float(entry_price) - cushion
        return min(float(current_stop), candidate)
    if direction == "SHORT":
        candidate = float(entry_price) + cushion
        return max(float(current_stop), candidate)
    return float(current_stop)


def decision_to_metadata(decision: WeekendDecision) -> dict[str, Any]:
    """Serialize a decision into a metadata dict suitable for trade logs."""
    return {
        "weekend_flatten": bool(decision.flatten),
        "weekend_widen_stops": bool(decision.widen_stops),
        "weekend_block_new_entries": bool(decision.block_new_entries),
        "weekend_reason": str(decision.reason),
    }
