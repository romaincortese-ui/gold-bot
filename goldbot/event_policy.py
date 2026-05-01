from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from goldbot.models import Opportunity


@dataclass(frozen=True, slots=True)
class GoldEventPolicyDecision:
    allowed: bool
    reason: str
    risk_multiplier: float = 1.0
    score_offset: float = 0.0
    gold_bias_score: float = 0.0
    event_count: int = 0
    extreme_event_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GoldEventCatalystDecision:
    direction: str | None
    reason: str
    gold_bias_score: float = 0.0
    event_count: int = 0
    extreme_event_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


def is_event_state_fresh(
    state: Mapping[str, Any] | None,
    now: datetime,
    *,
    max_age_seconds: int,
) -> bool:
    if not state:
        return False
    generated_at = parse_event_timestamp(
        state.get("generated_at") or state.get("as_of") or state.get("updated_at")
    )
    if generated_at is None:
        return False
    age_seconds = (now.astimezone(timezone.utc) - generated_at).total_seconds()
    return 0 <= age_seconds <= max_age_seconds


def apply_gold_event_policy(
    settings: Any,
    opportunity: Opportunity,
    state: Mapping[str, Any] | None,
    now: datetime,
) -> tuple[Opportunity | None, GoldEventPolicyDecision]:
    if not getattr(settings, "gold_event_policy_enabled", True):
        return opportunity, GoldEventPolicyDecision(allowed=True, reason="disabled")

    decision = evaluate_gold_event_policy(
        opportunity,
        state,
        now,
        stale_seconds=int(getattr(settings, "gold_event_stale_seconds", 7200)),
        high_impact_window_minutes=int(getattr(settings, "gold_event_high_impact_window_minutes", 180)),
        extreme_window_minutes=int(getattr(settings, "gold_event_extreme_window_minutes", 30)),
        adverse_risk_multiplier=float(getattr(settings, "gold_event_adverse_risk_multiplier", 0.5)),
        favourable_risk_multiplier=float(getattr(settings, "gold_event_favourable_risk_multiplier", 1.15)),
        score_offset_magnitude=float(getattr(settings, "gold_event_score_offset", 4.0)),
    )
    metadata = dict(opportunity.metadata or {})
    metadata.update(decision.metadata)
    metadata["gold_event_policy_reason"] = decision.reason
    metadata["gold_event_bias_score"] = round(decision.gold_bias_score, 4)
    metadata["gold_event_risk_mult"] = round(decision.risk_multiplier, 4)
    metadata["gold_event_count"] = decision.event_count
    metadata["gold_event_extreme_count"] = decision.extreme_event_count
    opportunity.metadata = metadata
    if not decision.allowed:
        return None, decision
    if abs(decision.score_offset) > 1e-9:
        opportunity.score += decision.score_offset
    if abs(decision.risk_multiplier - 1.0) > 1e-9:
        current_multiplier = float(opportunity.metadata.get("risk_multiplier", 1.0) or 1.0)
        opportunity.metadata["risk_multiplier"] = current_multiplier * decision.risk_multiplier
    return opportunity, decision


def evaluate_gold_event_catalyst(
    settings: Any,
    state: Mapping[str, Any] | None,
    now: datetime,
) -> GoldEventCatalystDecision:
    if not getattr(settings, "gold_event_policy_enabled", True):
        return GoldEventCatalystDecision(direction=None, reason="disabled")
    if not getattr(settings, "gold_event_catalyst_enabled", True):
        return GoldEventCatalystDecision(direction=None, reason="catalyst_disabled")
    stale_seconds = int(getattr(settings, "gold_event_stale_seconds", 7200))
    if not is_event_state_fresh(state, now, max_age_seconds=stale_seconds):
        return GoldEventCatalystDecision(direction=None, reason="no_fresh_event_state")

    high_impact_window_minutes = int(getattr(settings, "gold_event_high_impact_window_minutes", 180))
    extreme_window_minutes = int(getattr(settings, "gold_event_extreme_window_minutes", 30))
    components, events = _gold_event_bias_components(
        state or {},
        now,
        high_impact_window_minutes,
        extreme_window_minutes,
    )
    gold_bias = _weighted_average(components)
    event_count = len(events)
    extreme_count = sum(1 for event in events if event["extreme"])
    min_bias = float(getattr(settings, "gold_event_catalyst_min_bias", 0.35))
    metadata = {
        "gold_event_components": [name for name, score, _weight in components if abs(score) > 1e-9],
        "gold_event_catalyst_bias": round(gold_bias, 4),
        "gold_event_catalyst_events": event_count,
        "gold_event_catalyst_extreme_events": extreme_count,
    }
    if abs(gold_bias) < min_bias:
        return GoldEventCatalystDecision(
            direction=None,
            reason="event_bias_below_catalyst_threshold",
            gold_bias_score=gold_bias,
            event_count=event_count,
            extreme_event_count=extreme_count,
            metadata=metadata,
        )
    return GoldEventCatalystDecision(
        direction="LONG" if gold_bias > 0 else "SHORT",
        reason="event_catalyst_long" if gold_bias > 0 else "event_catalyst_short",
        gold_bias_score=gold_bias,
        event_count=event_count,
        extreme_event_count=extreme_count,
        metadata=metadata,
    )


def evaluate_gold_event_policy(
    opportunity: Opportunity,
    state: Mapping[str, Any] | None,
    now: datetime,
    *,
    stale_seconds: int = 7200,
    high_impact_window_minutes: int = 180,
    extreme_window_minutes: int = 30,
    adverse_risk_multiplier: float = 0.5,
    favourable_risk_multiplier: float = 1.15,
    score_offset_magnitude: float = 4.0,
) -> GoldEventPolicyDecision:
    if not is_event_state_fresh(state, now, max_age_seconds=stale_seconds):
        return GoldEventPolicyDecision(allowed=True, reason="no_fresh_event_state")

    direction = str(opportunity.direction or "").upper()
    direction_sign = 1.0 if direction == "LONG" else -1.0 if direction == "SHORT" else 0.0
    if direction_sign == 0.0:
        return GoldEventPolicyDecision(allowed=True, reason="unknown_direction")

    components, events = _gold_event_bias_components(
        state,
        now,
        high_impact_window_minutes,
        extreme_window_minutes,
    )

    gold_bias = _weighted_average(components)
    align = direction_sign * gold_bias
    event_count = len(events)
    extreme_count = sum(1 for event in events if event["extreme"])
    metadata = {
        "gold_event_components": [name for name, score, _weight in components if abs(score) > 1e-9],
    }

    adverse_risk_multiplier = _clamp(adverse_risk_multiplier, 0.05, 1.0)
    favourable_risk_multiplier = _clamp(favourable_risk_multiplier, 1.0, 1.5)

    if extreme_count and align <= -0.75:
        return GoldEventPolicyDecision(
            allowed=False,
            reason="extreme_event_adverse",
            gold_bias_score=gold_bias,
            event_count=event_count,
            extreme_event_count=extreme_count,
            metadata=metadata,
        )

    if align <= -0.35:
        return GoldEventPolicyDecision(
            allowed=True,
            reason="event_adverse_reduce",
            risk_multiplier=adverse_risk_multiplier,
            score_offset=-score_offset_magnitude * min(1.0, abs(align)),
            gold_bias_score=gold_bias,
            event_count=event_count,
            extreme_event_count=extreme_count,
            metadata=metadata,
        )

    if align >= 0.35:
        risk_multiplier = 1.0 + (favourable_risk_multiplier - 1.0) * min(1.0, align)
        return GoldEventPolicyDecision(
            allowed=True,
            reason="event_favourable_boost",
            risk_multiplier=risk_multiplier,
            score_offset=score_offset_magnitude * min(1.0, align),
            gold_bias_score=gold_bias,
            event_count=event_count,
            extreme_event_count=extreme_count,
            metadata=metadata,
        )

    if extreme_count and opportunity.strategy != "MACRO_BREAKOUT":
        return GoldEventPolicyDecision(
            allowed=True,
            reason="high_impact_event_window_reduce",
            risk_multiplier=max(adverse_risk_multiplier, 0.65),
            gold_bias_score=gold_bias,
            event_count=event_count,
            extreme_event_count=extreme_count,
            metadata=metadata,
        )

    return GoldEventPolicyDecision(
        allowed=True,
        reason="neutral",
        gold_bias_score=gold_bias,
        event_count=event_count,
        extreme_event_count=extreme_count,
        metadata=metadata,
    )


def _gold_event_bias_components(
    state: Mapping[str, Any],
    now: datetime,
    high_impact_window_minutes: int,
    extreme_window_minutes: int,
) -> tuple[list[tuple[str, float, float]], list[dict[str, Any]]]:
    events = _events_near_now(state, now, high_impact_window_minutes, extreme_window_minutes)
    components: list[tuple[str, float, float]] = []
    for event in events:
        title_score = _title_gold_bias(event["title"])
        if abs(title_score) > 1e-9:
            components.append(("event_title", title_score, 0.5))

    components.extend(_event_score_components(state, now, high_impact_window_minutes))
    real_yield_component = _real_yield_component(state, now)
    if real_yield_component is not None:
        components.append(real_yield_component)
    return components, events


def parse_event_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _events_near_now(
    state: Mapping[str, Any],
    now: datetime,
    high_impact_window_minutes: int,
    extreme_window_minutes: int,
) -> list[dict[str, Any]]:
    raw_events = state.get("events") or []
    if not isinstance(raw_events, list):
        return []
    now_utc = now.astimezone(timezone.utc)
    events: list[dict[str, Any]] = []
    for item in raw_events:
        if not isinstance(item, Mapping):
            continue
        title = str(item.get("title") or item.get("name") or "")
        currency = str(item.get("currency") or "USD").upper()
        impact = str(item.get("impact") or item.get("importance") or "").lower()
        occurs_at = parse_event_timestamp(item.get("occurs_at") or item.get("time") or item.get("as_of"))
        if occurs_at is None:
            continue
        if currency not in {"USD", "XAU", "GOLD", ""} and not _is_geopolitical_title(title):
            continue
        if impact and impact not in {"high", "medium"} and not _is_geopolitical_title(title):
            continue
        delta_minutes = abs((now_utc - occurs_at).total_seconds()) / 60.0
        if delta_minutes <= high_impact_window_minutes:
            events.append(
                {
                    "title": title,
                    "occurs_at": occurs_at,
                    "delta_minutes": delta_minutes,
                    "extreme": delta_minutes <= extreme_window_minutes,
                }
            )
    return events


def _event_score_components(
    state: Mapping[str, Any],
    now: datetime,
    high_impact_window_minutes: int,
) -> list[tuple[str, float, float]]:
    raw_scores = state.get("event_scores") or []
    if not isinstance(raw_scores, list):
        return []
    now_utc = now.astimezone(timezone.utc)
    components: list[tuple[str, float, float]] = []
    for item in raw_scores:
        if not isinstance(item, Mapping):
            continue
        as_of = parse_event_timestamp(item.get("as_of") or item.get("occurs_at"))
        if as_of is None:
            continue
        if abs((now_utc - as_of).total_seconds()) > high_impact_window_minutes * 60:
            continue
        composite = _optional_float(item.get("composite"))
        if composite is None:
            composite = _rough_composite_from_score_payload(item)
        if composite is None or composite <= 0:
            continue
        usd_direction = str(item.get("usd_direction") or "").upper()
        surprise_z = _optional_float(item.get("surprise_z"))
        if not usd_direction and surprise_z is not None:
            usd_direction = "UP" if surprise_z > 0 else "DOWN" if surprise_z < 0 else ""
        if usd_direction == "UP":
            components.append(("event_score", -_clamp(composite, 0.0, 1.0), 1.0))
        elif usd_direction == "DOWN":
            components.append(("event_score", _clamp(composite, 0.0, 1.0), 1.0))
    return components


def _real_yield_component(state: Mapping[str, Any], now: datetime) -> tuple[str, float, float] | None:
    raw = state.get("real_yields")
    if not isinstance(raw, Mapping):
        return None
    as_of = parse_event_timestamp(raw.get("as_of"))
    if as_of is not None and (now.astimezone(timezone.utc) - as_of) > timedelta(hours=36):
        return None
    change_bps = _optional_float(raw.get("real_yield_change_bps"))
    level = _optional_float(raw.get("real_yield_10y"))
    score = 0.0
    if change_bps is not None:
        if change_bps >= 7.5:
            score -= min(1.0, change_bps / 20.0)
        elif change_bps <= -7.5:
            score += min(1.0, abs(change_bps) / 20.0)
    if level is not None and change_bps is not None:
        if level >= 1.8 and change_bps > 0:
            score -= 0.35
        elif level <= 1.0 and change_bps < 0:
            score += 0.35
    if abs(score) < 1e-9:
        return None
    return "real_yields", _clamp(score, -1.0, 1.0), 0.8


def _rough_composite_from_score_payload(item: Mapping[str, Any]) -> float | None:
    values: list[float] = []
    surprise_z = _optional_float(item.get("surprise_z"))
    if surprise_z is not None:
        values.append(abs(surprise_z))
    rates_move_bps = _optional_float(item.get("rates_move_bps"))
    if rates_move_bps is not None:
        values.append(abs(rates_move_bps) / 10.0)
    dxy_move_pct = _optional_float(item.get("dxy_move_pct"))
    if dxy_move_pct is not None:
        values.append(abs(dxy_move_pct) / 0.30)
    if len(values) < 2:
        return None
    score = sum(1.0 - math.exp(-value) for value in values) / len(values)
    return _clamp(score, 0.0, 1.0)


def _title_gold_bias(title: str) -> float:
    lowered = title.lower()
    if _is_geopolitical_title(title):
        return 0.7
    if any(token in lowered for token in ("fomc", "fed", "rate decision", "powell")):
        return 0.0
    if any(token in lowered for token in ("hawkish", "rate hike", "higher for longer", "hot cpi", "inflation surprise")):
        return -0.6
    if any(token in lowered for token in ("dovish", "rate cut", "weak payroll", "soft cpi", "disinflation")):
        return 0.6
    return 0.0


def _is_geopolitical_title(title: str) -> bool:
    lowered = title.lower()
    return any(
        token in lowered
        for token in (
            "geopolitical",
            "war",
            "conflict",
            "missile",
            "sanction",
            "tariff",
            "safe haven",
            "crisis",
            "default",
        )
    )


def _weighted_average(components: list[tuple[str, float, float]]) -> float:
    weighted = [(score, max(0.0, weight)) for _name, score, weight in components if abs(score) > 1e-9]
    total_weight = sum(weight for _score, weight in weighted)
    if total_weight <= 0:
        return 0.0
    return _clamp(sum(score * weight for score, weight in weighted) / total_weight, -1.0, 1.0)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))