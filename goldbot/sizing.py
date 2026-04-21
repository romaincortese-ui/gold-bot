"""Sprint 1 — Item 2.4: volatility-target position sizing.

The legacy gold-bot sizes trades to a fixed 0.75% of the sleeve per trade:

    contract_size = risk_amount / stop_distance

That means on a 0.4% ATR day you take tiny P&L; on a 1.8% ATR day you take
roughly 4x the P&L for the same notional "risk %". It is the dominant source
of P&L noise on a single-instrument book.

Desk convention is to target a constant **basis-point NAV impact per 1 ATR
of stop**, so that the full stop-out costs the same percentage of equity on
a calm day as on an event day. Mechanically:

    risk_amount_vol = (nav * target_nav_bps / 10_000) * (stop_distance / atr)

Interpretation:
  - When `stop_distance == atr` (stop placed 1 ATR away), the trade risks
    exactly `target_nav_bps` of NAV.
  - When the stop has to sit further out (e.g. 2 ATR at a chart level), the
    dollar risk stays a constant fraction of NAV per unit-ATR — scaling
    linearly with stop distance preserves the invariant.

We then clamp the result against two protective ceilings:
  1. `max_trade_risk_amount` — the legacy static %-of-sleeve cap (never
     exceeded; this function only ever brings size *down* on high-vol days).
  2. `available_gold_risk` — the portfolio-wide open-risk budget.

We ALSO clamp against a floor (never below 25% of the legacy sizing) to
prevent the bot from effectively opting out in very quiet tape where ATR is
meaningfully smaller than stop distance would suggest.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SizingDecision:
    risk_amount: float
    source: str  # "vol_target", "legacy_cap", "available_budget", "disabled"
    atr: float
    stop_distance: float
    target_nav_bps: float
    nav: float


def compute_risk_amount(
    *,
    nav: float,
    atr: float,
    stop_distance: float,
    target_nav_bps: float,
    legacy_max_trade_risk: float,
    available_gold_risk: float,
    enabled: bool = True,
    risk_multiplier: float = 1.0,
    floor_fraction: float = 0.25,
) -> SizingDecision:
    """Return the dollar risk to use for this trade.

    Parameters
    ----------
    nav : float
        Account NAV in quote currency (the "equity" figure the broker reports
        including unrealized P&L).
    atr : float
        Current instrument ATR in $/oz (same units as stop_distance).
    stop_distance : float
        Distance from entry to protective stop, absolute value, in $/oz.
    target_nav_bps : float
        Desired NAV basis-points consumed per 1 ATR of stop (e.g. 25 = 25 bp).
    legacy_max_trade_risk : float
        The pre-existing %-of-sleeve cap in dollar terms. Always an upper
        bound — volatility-target sizing only *reduces* size.
    available_gold_risk : float
        Remaining open-risk budget across all gold trades.
    enabled : bool
        If False, return the legacy cap verbatim (preserves old behaviour).
    risk_multiplier : float
        Multiplicative adjustment from calibration / macro overlays. Applied
        to both the vol-target and legacy-cap ceilings.
    floor_fraction : float
        Minimum fraction of the legacy cap the vol-target output may produce.
        Prevents "effectively zero" sizing when ATR is very large relative to
        stop_distance.

    Returns
    -------
    SizingDecision
        Contains the final risk_amount (always >= 0) and a short attribution
        string describing which ceiling was binding.
    """
    legacy_risk = max(0.0, float(legacy_max_trade_risk) * float(risk_multiplier))
    budget_risk = max(0.0, float(available_gold_risk))

    if not enabled or atr <= 0 or stop_distance <= 0 or nav <= 0 or target_nav_bps <= 0:
        final = min(legacy_risk, budget_risk)
        return SizingDecision(
            risk_amount=final,
            source="disabled" if not enabled else "legacy_cap",
            atr=float(atr),
            stop_distance=float(stop_distance),
            target_nav_bps=float(target_nav_bps),
            nav=float(nav),
        )

    atr_f = float(atr)
    stop_f = float(stop_distance)
    # risk_amount such that 1 ATR move at the stop consumes target_nav_bps of NAV
    per_atr_dollar = float(nav) * float(target_nav_bps) / 10_000.0
    vol_risk = per_atr_dollar * (stop_f / atr_f) * float(risk_multiplier)

    floor = legacy_risk * float(floor_fraction) if legacy_risk > 0 else 0.0
    if vol_risk < floor:
        vol_risk = floor

    candidates = [
        (vol_risk, "vol_target"),
        (legacy_risk, "legacy_cap"),
        (budget_risk, "available_budget"),
    ]
    final_amount, source = min(candidates, key=lambda pair: pair[0])
    return SizingDecision(
        risk_amount=max(0.0, final_amount),
        source=source,
        atr=atr_f,
        stop_distance=stop_f,
        target_nav_bps=float(target_nav_bps),
        nav=float(nav),
    )
