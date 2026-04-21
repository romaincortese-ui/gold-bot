"""Execution tightening helpers (Sprint 3 §3.7).

Encapsulates three pure building blocks used around the live-order path:

* ``build_limit_entry_price`` — price a limit entry order at ``mid +/- spread * k``.
* ``reconcile_partial_fill`` — re-anchor TP/SL and risk when OANDA fills a
  subset of the requested units (possible above ~$250k notional). The old
  code assumed full fills; this helper lets the caller scale targets/risk
  against the actual filled quantity.
* ``build_execution_plan`` — assemble an ``ExecutionPlan`` that the
  market-data layer can honour (limit vs market, guaranteed-stop flag,
  timeout for cancel-and-requeue).

All functions are pure and dependency-free so the market-data client can
translate an ``ExecutionPlan`` into an OANDA order payload without further
policy logic.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    order_type: str                   # "MARKET" | "LIMIT"
    limit_price: float | None
    time_in_force: str                # "FOK" | "GTD" | "GTC"
    cancel_after_seconds: int | None  # for LIMIT+GTD only
    guaranteed_stop: bool


def build_limit_entry_price(
    *,
    direction: str,
    bid: float,
    ask: float,
    spread_multiplier: float,
) -> float:
    """Price a limit entry at ``mid +/- (spread/2) * multiplier``.

    Long entries post a bid above mid (paying up to a fraction of the
    spread); short entries post an offer below mid. ``multiplier`` of 1.0
    crosses the spread exactly at mid; 1.25 pays 25% into the spread;
    values > 2.0 effectively behave as market orders.
    """
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("bid/ask must be positive and ask >= bid")
    mid = (bid + ask) / 2.0
    half_spread = (ask - bid) / 2.0
    offset = half_spread * max(0.0, float(spread_multiplier))
    dir_upper = (direction or "").upper()
    if dir_upper == "LONG":
        return round(mid + offset, 5)
    if dir_upper == "SHORT":
        return round(mid - offset, 5)
    raise ValueError(f"unknown direction: {direction!r}")


@dataclass(frozen=True, slots=True)
class PartialFillAdjustment:
    filled_size: float
    fill_ratio: float             # filled_size / requested_size
    adjusted_risk_amount: float   # risk_per_unit * filled_size
    adjusted_tp_distance: float   # scaled 1:1 with fill_ratio


def reconcile_partial_fill(
    *,
    requested_size: float,
    filled_size: float,
    risk_per_unit: float,
    original_tp_distance: float,
) -> PartialFillAdjustment:
    """Re-anchor risk/TP after a partial fill.

    The TP distance is left unchanged (it's a price distance, independent
    of size) but the risk amount and notional exposure follow the actual
    fill. A fill ratio of 0 (no fill) returns an adjustment that the
    caller can use to cancel pending protective orders.
    """
    if requested_size <= 0:
        raise ValueError("requested_size must be > 0")
    filled = max(0.0, float(filled_size))
    if filled > requested_size:
        filled = float(requested_size)
    ratio = filled / float(requested_size)
    return PartialFillAdjustment(
        filled_size=filled,
        fill_ratio=ratio,
        adjusted_risk_amount=float(risk_per_unit) * filled,
        adjusted_tp_distance=float(original_tp_distance),  # price distance unchanged
    )


def build_execution_plan(
    *,
    use_limit_entry: bool,
    direction: str,
    bid: float,
    ask: float,
    limit_spread_multiplier: float,
    limit_timeout_seconds: int,
    guaranteed_stop: bool,
) -> ExecutionPlan:
    """Assemble an ``ExecutionPlan`` for the marketdata layer.

    ``use_limit_entry`` off yields a plain market FOK with optional
    guaranteed stop. With limit entries on, the caller should cancel the
    order after ``limit_timeout_seconds`` if still unfilled and either
    re-quote or skip the bar.
    """
    if use_limit_entry:
        price = build_limit_entry_price(
            direction=direction,
            bid=bid,
            ask=ask,
            spread_multiplier=limit_spread_multiplier,
        )
        return ExecutionPlan(
            order_type="LIMIT",
            limit_price=price,
            time_in_force="GTD",
            cancel_after_seconds=int(max(1, limit_timeout_seconds)),
            guaranteed_stop=bool(guaranteed_stop),
        )
    return ExecutionPlan(
        order_type="MARKET",
        limit_price=None,
        time_in_force="FOK",
        cancel_after_seconds=None,
        guaranteed_stop=bool(guaranteed_stop),
    )
