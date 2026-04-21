"""Three-factor gold model for Kelly-style sizing (Gold-bot Q2 §4.2).

The review memo specifies a small factor model for gold driven by:

1. **10Y US TIPS yield** — weekly change (basis points). Rising real yields
   are a headwind for gold; we flip the sign so that a *falling* yield
   scores positive.
2. **DXY** — weekly % change. Rising DXY is a headwind; sign-flipped.
3. **GLD ETF shares outstanding** — weekly % change. Positive flow is a
   direct bullish signal.

Each factor is standardised by its historical standard deviation (``std``)
so it lands roughly in ``[-2, +2]``. We combine with fixed weights
(``tips_weight``, ``dxy_weight``, ``gld_weight``; default 0.4/0.35/0.25
reflecting the TIPS factor's higher explanatory power on weekly gold
variance).

The aggregate ``factor_score`` is clamped to ``[-1, +1]``. Positive scores
above ``align_threshold`` on an opportunity whose direction matches boost
the ``risk_multiplier`` up to ``factor_align_size_mult``. Negative scores
(strong disagreement) reduce the multiplier. Scores within the dead-zone
leave sizing untouched.
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
class GoldFactorSignal:
    as_of: datetime
    tips_weekly_change_bps: float | None
    tips_change_std_bps: float | None
    dxy_weekly_change_pct: float | None
    dxy_change_std_pct: float | None
    gld_weekly_flow_pct: float | None
    gld_flow_std_pct: float | None


def signal_to_payload(signal: GoldFactorSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "tips_weekly_change_bps": signal.tips_weekly_change_bps,
        "tips_change_std_bps": signal.tips_change_std_bps,
        "dxy_weekly_change_pct": signal.dxy_weekly_change_pct,
        "dxy_change_std_pct": signal.dxy_change_std_pct,
        "gld_weekly_flow_pct": signal.gld_weekly_flow_pct,
        "gld_flow_std_pct": signal.gld_flow_std_pct,
    }


def load_factor_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_hours: int,
) -> GoldFactorSignal | None:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("gold_factor_model")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_hours >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(hours=max_age_hours):
        return None
    return GoldFactorSignal(
        as_of=as_of,
        tips_weekly_change_bps=_coerce_optional_float(raw.get("tips_weekly_change_bps")),
        tips_change_std_bps=_coerce_optional_float(raw.get("tips_change_std_bps")),
        dxy_weekly_change_pct=_coerce_optional_float(raw.get("dxy_weekly_change_pct")),
        dxy_change_std_pct=_coerce_optional_float(raw.get("dxy_change_std_pct")),
        gld_weekly_flow_pct=_coerce_optional_float(raw.get("gld_weekly_flow_pct")),
        gld_flow_std_pct=_coerce_optional_float(raw.get("gld_flow_std_pct")),
    )


@dataclass(frozen=True, slots=True)
class FactorScoreBreakdown:
    tips_z: float | None           # sign-flipped z-score of TIPS weekly change
    dxy_z: float | None            # sign-flipped z-score of DXY weekly %
    gld_z: float | None            # z-score of GLD flow %
    aggregate: float               # weighted + clamped [-1, +1]
    contributing_factor_count: int


def compute_factor_score(
    signal: GoldFactorSignal,
    *,
    tips_weight: float = 0.40,
    dxy_weight: float = 0.35,
    gld_weight: float = 0.25,
) -> FactorScoreBreakdown:
    """Compute a normalised gold factor score from a signal payload.

    Returns a breakdown with per-factor z-scores (sign-flipped for the two
    "headwind" factors so positive always means bullish gold) and an
    aggregate in ``[-1, +1]``. Missing factors are skipped and the weights
    are renormalised across the remaining contributors.
    """
    contributions: list[tuple[float, float]] = []  # (z, weight)

    tips_z = _signed_z(
        raw=signal.tips_weekly_change_bps,
        std=signal.tips_change_std_bps,
        sign_flip=True,
    )
    if tips_z is not None:
        contributions.append((tips_z, max(0.0, tips_weight)))

    dxy_z = _signed_z(
        raw=signal.dxy_weekly_change_pct,
        std=signal.dxy_change_std_pct,
        sign_flip=True,
    )
    if dxy_z is not None:
        contributions.append((dxy_z, max(0.0, dxy_weight)))

    gld_z = _signed_z(
        raw=signal.gld_weekly_flow_pct,
        std=signal.gld_flow_std_pct,
        sign_flip=False,
    )
    if gld_z is not None:
        contributions.append((gld_z, max(0.0, gld_weight)))

    if not contributions:
        return FactorScoreBreakdown(
            tips_z=tips_z,
            dxy_z=dxy_z,
            gld_z=gld_z,
            aggregate=0.0,
            contributing_factor_count=0,
        )

    total_weight = sum(w for _, w in contributions)
    if total_weight <= 0:
        return FactorScoreBreakdown(
            tips_z=tips_z,
            dxy_z=dxy_z,
            gld_z=gld_z,
            aggregate=0.0,
            contributing_factor_count=0,
        )
    weighted = sum(z * w for z, w in contributions) / total_weight
    # Map the weighted z (approx in [-2, +2]) to a bounded score [-1, +1].
    aggregate = max(-1.0, min(1.0, weighted / 2.0))
    return FactorScoreBreakdown(
        tips_z=tips_z,
        dxy_z=dxy_z,
        gld_z=gld_z,
        aggregate=aggregate,
        contributing_factor_count=len(contributions),
    )


def apply_factor_overlay(
    settings: Settings,
    opportunity: Opportunity,
    signal: GoldFactorSignal | None,
) -> Opportunity:
    """Apply the factor score as a sizing adjustment + score offset.

    * Aligned direction (score and trade direction agree) above
      ``align_threshold`` boosts ``risk_multiplier``.
    * Opposed direction below ``-align_threshold`` reduces ``risk_multiplier``
      down to ``min_size_mult`` (never below — we keep the trade on rather
      than veto-ing so the existing real-yield/regime gates remain the
      veto layer).
    """
    if not getattr(settings, "factor_model_enabled", False) or signal is None:
        return opportunity

    breakdown = compute_factor_score(
        signal,
        tips_weight=float(getattr(settings, "factor_tips_weight", 0.40)),
        dxy_weight=float(getattr(settings, "factor_dxy_weight", 0.35)),
        gld_weight=float(getattr(settings, "factor_gld_weight", 0.25)),
    )
    if breakdown.contributing_factor_count == 0:
        return opportunity

    direction = (opportunity.direction or "").upper()
    direction_sign = 1.0 if direction == "LONG" else (-1.0 if direction == "SHORT" else 0.0)
    if direction_sign == 0.0:
        return opportunity

    align = direction_sign * breakdown.aggregate

    align_threshold = float(getattr(settings, "factor_align_threshold", 0.4))
    max_mult = float(getattr(settings, "factor_align_size_mult", 1.5))
    min_mult = float(getattr(settings, "factor_oppose_size_mult", 0.5))
    score_offset_magnitude = float(getattr(settings, "factor_score_offset", 5.0))

    metadata = dict(opportunity.metadata)
    metadata["factor_score"] = round(breakdown.aggregate, 4)
    metadata["factor_tips_z"] = (
        round(breakdown.tips_z, 4) if breakdown.tips_z is not None else None
    )
    metadata["factor_dxy_z"] = (
        round(breakdown.dxy_z, 4) if breakdown.dxy_z is not None else None
    )
    metadata["factor_gld_z"] = (
        round(breakdown.gld_z, 4) if breakdown.gld_z is not None else None
    )

    score_delta = 0.0
    risk_mult_delta = 1.0
    reason = None
    if align >= align_threshold:
        reason = "factor_align_boost"
        score_delta = score_offset_magnitude * align
        risk_mult_delta = 1.0 + (max_mult - 1.0) * min(1.0, align)
    elif align <= -align_threshold:
        reason = "factor_oppose_reduce"
        score_delta = score_offset_magnitude * align  # negative
        # Linear interpolation to min_mult at align == -1.
        risk_mult_delta = 1.0 - (1.0 - min_mult) * min(1.0, -align)

    if reason is not None:
        metadata["factor_overlay_reason"] = reason
        metadata["factor_align"] = round(align, 4)
        current_mult = float(metadata.get("risk_multiplier", 1.0) or 1.0)
        metadata["risk_multiplier"] = current_mult * max(0.0, risk_mult_delta)
        opportunity.score = max(0.0, float(opportunity.score) + score_delta)
    opportunity.metadata = metadata
    return opportunity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _signed_z(*, raw: float | None, std: float | None, sign_flip: bool) -> float | None:
    if raw is None or std is None or std <= 0:
        return None
    z = float(raw) / float(std)
    if sign_flip:
        z = -z
    # Clip extreme outliers so a single headline doesn't dominate.
    return max(-3.0, min(3.0, z))


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
