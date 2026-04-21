"""Miners / ETF overlay for XAU_USD (Gold-bot Q2 §4.1).

The bot trades only XAU on OANDA, so "adding GDX/GLD as secondary
instruments" is implemented as a *signal-enrichment* overlay rather than a
co-traded book. The macro engine publishes the latest daily percentage
changes for GDX, NEM, and GLD in ``gold_macro_state.json#miners``. We
derive two signal components:

* **Miners lead** — a weighted average of GDX/NEM % change. Historically
  GDX has ~2× beta to gold; if miners lead gold meaningfully on the day,
  that is confirmation for a long-gold setup (boost score), and a warning
  sign for a short-gold setup.
* **ETF flow** — the intraday change in GLD shares outstanding as a direct
  inflow/outflow signal. Large positive flow (>= threshold) is dip-buying
  confirmation for longs; large negative flow favours shorts.

The overlay never vetoes a trade — it only adjusts `score` and, on a
strong miners-confirming long setup, scales `risk_multiplier` up to a
configurable cap.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from goldbot.config import Settings
from goldbot.models import Opportunity


@dataclass(frozen=True, slots=True)
class MinersSignal:
    as_of: datetime
    gdx_daily_change_pct: float | None
    nem_daily_change_pct: float | None
    gld_shares_outstanding_change_pct: float | None


def signal_to_payload(signal: MinersSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "gdx_daily_change_pct": signal.gdx_daily_change_pct,
        "nem_daily_change_pct": signal.nem_daily_change_pct,
        "gld_shares_outstanding_change_pct": signal.gld_shares_outstanding_change_pct,
    }


def load_miners_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_hours: int,
) -> MinersSignal | None:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("miners")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_hours >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(hours=max_age_hours):
        return None
    return MinersSignal(
        as_of=as_of,
        gdx_daily_change_pct=_coerce_optional_float(raw.get("gdx_daily_change_pct")),
        nem_daily_change_pct=_coerce_optional_float(raw.get("nem_daily_change_pct")),
        gld_shares_outstanding_change_pct=_coerce_optional_float(
            raw.get("gld_shares_outstanding_change_pct")
        ),
    )


def _miners_component(signal: MinersSignal) -> float | None:
    """Weighted % change across GDX (0.7) and NEM (0.3)."""
    parts: list[tuple[float, float]] = []
    if signal.gdx_daily_change_pct is not None:
        parts.append((float(signal.gdx_daily_change_pct), 0.7))
    if signal.nem_daily_change_pct is not None:
        parts.append((float(signal.nem_daily_change_pct), 0.3))
    if not parts:
        return None
    total_weight = sum(w for _, w in parts)
    if total_weight <= 0:
        return None
    return sum(v * w for v, w in parts) / total_weight


def apply_miners_overlay(
    settings: Settings,
    opportunity: Opportunity,
    signal: MinersSignal | None,
    *,
    gold_daily_change_pct: float | None = None,
) -> Opportunity:
    """Adjust an opportunity's score and optional risk multiplier.

    ``gold_daily_change_pct`` is optional — when supplied, the miners-vs-gold
    *divergence* is used as a stronger confirming signal than the raw miners
    change alone. When it is ``None`` the raw miners component still drives
    the score adjustment, which is still useful when the gold % change is
    unavailable from the runtime frame.
    """
    if not getattr(settings, "miners_overlay_enabled", False) or signal is None:
        return opportunity

    offset_magnitude = float(getattr(settings, "miners_score_offset", 6.0))
    if offset_magnitude <= 0:
        return opportunity

    miners = _miners_component(signal)
    etf_flow = signal.gld_shares_outstanding_change_pct
    direction = (opportunity.direction or "").upper()
    confirm_threshold_pct = float(getattr(settings, "miners_confirm_threshold_pct", 0.005))
    etf_flow_threshold_pct = float(getattr(settings, "miners_etf_flow_threshold_pct", 0.002))
    size_boost_mult = float(getattr(settings, "miners_long_confirm_size_mult", 1.2))

    metadata = dict(opportunity.metadata)
    delta = 0.0
    reason: str | None = None

    if miners is not None:
        divergence = miners
        if gold_daily_change_pct is not None:
            # Miners' *excess* move over gold — positive means miners leading
            # gold, negative means lagging / de-risking.
            divergence = miners - float(gold_daily_change_pct)
        metadata["miners_divergence_pct"] = round(divergence, 5)
        metadata["miners_component_pct"] = round(miners, 5)
        if direction == "LONG" and divergence >= confirm_threshold_pct:
            delta += offset_magnitude
            reason = "miners_confirm_long"
            if size_boost_mult > 1.0:
                current_mult = float(metadata.get("risk_multiplier", 1.0) or 1.0)
                metadata["risk_multiplier"] = current_mult * size_boost_mult
        elif direction == "LONG" and divergence <= -confirm_threshold_pct:
            delta -= offset_magnitude
            reason = "miners_divergence_fade_long"
        elif direction == "SHORT" and divergence <= -confirm_threshold_pct:
            delta += offset_magnitude
            reason = "miners_confirm_short"
        elif direction == "SHORT" and divergence >= confirm_threshold_pct:
            delta -= offset_magnitude
            reason = "miners_divergence_fade_short"

    if etf_flow is not None:
        metadata["gld_flow_pct"] = round(float(etf_flow), 5)
        etf_delta = 0.0
        if direction == "LONG" and etf_flow >= etf_flow_threshold_pct:
            etf_delta += offset_magnitude * 0.5
        elif direction == "SHORT" and etf_flow <= -etf_flow_threshold_pct:
            etf_delta += offset_magnitude * 0.5
        elif direction == "LONG" and etf_flow <= -etf_flow_threshold_pct:
            etf_delta -= offset_magnitude * 0.5
        elif direction == "SHORT" and etf_flow >= etf_flow_threshold_pct:
            etf_delta -= offset_magnitude * 0.5
        if etf_delta != 0.0:
            delta += etf_delta
            reason = (reason + "+etf_flow") if reason else "etf_flow"

    if delta == 0.0 and reason is None:
        opportunity.metadata = metadata
        return opportunity

    new_score = max(0.0, float(opportunity.score) + delta)
    metadata["miners_score_offset"] = float(delta)
    metadata["miners_overlay_reason"] = reason
    opportunity.score = new_score
    opportunity.metadata = metadata
    return opportunity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
