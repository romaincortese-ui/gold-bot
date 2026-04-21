"""CFTC Commitment of Traders (CoT) positioning filter for XAU_USD.

The CFTC's weekly Commitment of Traders report (CME Gold Futures, contract
GC) is the cleanest sentiment proxy for institutional flows. Managed-money
extreme net long historically marks short-term tops; extreme net short marks
bottoms. We do not block trades — we add a `score_offset` of `+/- 8` so the
selection logic prefers the *fading* side of crowded positioning.

The fetcher is intentionally a stub: reliable CoT ingestion needs the public
CFTC ZIP endpoint plus a parser, which is out of scope for Sprint 2. The
runtime path consumes a payload written by the macro engine, so a manual
CSV-to-JSON pipeline can populate `gold_macro_state.json#cftc` without code
changes here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from goldbot.config import Settings
from goldbot.models import Opportunity


@dataclass(frozen=True, slots=True)
class CFTCSignal:
    """Per-week summary of managed-money positioning in CME gold futures."""
    as_of: datetime
    managed_money_net_pct: float           # net long contracts as % of OI, signed
    managed_money_percentile_2y: float     # 0..1 percentile of net within 2y window
    commercial_net_short_change_wow: float # change in commercials' net short, week-over-week


def build_cftc_signal(history: Sequence[dict[str, Any]], as_of: datetime) -> CFTCSignal | None:
    """Build a signal from an ordered weekly history of CoT rows.

    Each row must contain ``date`` (ISO string), ``managed_money_net_pct``
    and ``commercial_net_short`` keys. Rows beyond ``as_of`` are ignored. The
    percentile is computed against the trailing 2-year window of net % values.
    """
    if not history:
        return None
    cutoff_utc = as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    parsed: list[tuple[datetime, float, float]] = []
    for row in history:
        try:
            ts = _parse_iso(row.get("date"))
            if ts is None or ts > cutoff_utc:
                continue
            net = float(row.get("managed_money_net_pct", 0.0))
            comm = float(row.get("commercial_net_short", 0.0))
            parsed.append((ts, net, comm))
        except (TypeError, ValueError):
            continue
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0])
    latest_ts, latest_net, latest_comm = parsed[-1]
    prev_comm = parsed[-2][2] if len(parsed) >= 2 else latest_comm

    window_start = latest_ts - timedelta(days=730)
    window_values = [net for ts, net, _ in parsed if ts >= window_start]
    if not window_values:
        window_values = [latest_net]
    percentile = _percentile_rank(window_values, latest_net)

    return CFTCSignal(
        as_of=latest_ts,
        managed_money_net_pct=latest_net,
        managed_money_percentile_2y=percentile,
        commercial_net_short_change_wow=latest_comm - prev_comm,
    )


def signal_to_payload(signal: CFTCSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "managed_money_net_pct": float(signal.managed_money_net_pct),
        "managed_money_percentile_2y": float(signal.managed_money_percentile_2y),
        "commercial_net_short_change_wow": float(signal.commercial_net_short_change_wow),
    }


def load_cftc_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_days: int,
) -> CFTCSignal | None:
    """Load a CFTC signal from the shared macro-state JSON file.

    Returns ``None`` if the file does not exist, is missing the ``cftc``
    payload, or the payload is older than ``max_age_days`` (CoT is weekly so
    >10d typically means a stale or skipped publication).
    """
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("cftc")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_days >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(days=max_age_days):
        return None
    try:
        return CFTCSignal(
            as_of=as_of,
            managed_money_net_pct=float(raw.get("managed_money_net_pct", 0.0)),
            managed_money_percentile_2y=float(raw.get("managed_money_percentile_2y", 0.5)),
            commercial_net_short_change_wow=float(raw.get("commercial_net_short_change_wow", 0.0)),
        )
    except (TypeError, ValueError):
        return None


def apply_cftc_overlay(settings: Settings, opportunity: Opportunity, signal: CFTCSignal | None) -> Opportunity:
    """Adjust an opportunity's score based on CFTC crowding extremes.

    Pure score nudge — never vetos a trade. Adds `cftc_extreme_score_offset`
    (default 8.0) negatively to LONGs when managed money is extremely long
    (top quantile), and positively to SHORTs in the same regime; symmetric
    for the bottom quantile.
    """
    if not getattr(settings, "cftc_filter_enabled", False) or signal is None:
        return opportunity

    threshold = float(getattr(settings, "cftc_extreme_percentile", 0.85))
    offset_magnitude = float(getattr(settings, "cftc_extreme_score_offset", 8.0))
    if offset_magnitude <= 0:
        return opportunity

    pctile = float(signal.managed_money_percentile_2y)
    direction = (opportunity.direction or "").upper()
    delta = 0.0
    reason = None
    if pctile >= threshold:
        # Crowded long — fade further longs, favour shorts.
        if direction == "LONG":
            delta = -offset_magnitude
            reason = "cftc_extreme_crowded_long"
        elif direction == "SHORT":
            delta = +offset_magnitude
            reason = "cftc_fade_crowded_long"
    elif pctile <= (1.0 - threshold):
        # Crowded short — fade further shorts, favour longs.
        if direction == "SHORT":
            delta = -offset_magnitude
            reason = "cftc_extreme_crowded_short"
        elif direction == "LONG":
            delta = +offset_magnitude
            reason = "cftc_fade_crowded_short"

    if delta == 0.0:
        return opportunity

    new_score = max(0.0, float(opportunity.score) + delta)
    metadata = dict(opportunity.metadata)
    metadata["cftc_score_offset"] = float(delta)
    metadata["cftc_percentile_2y"] = float(pctile)
    metadata["cftc_filter_reason"] = reason
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


def _percentile_rank(values: Sequence[float], target: float) -> float:
    if not values:
        return 0.5
    lower = sum(1 for v in values if v < target)
    equal = sum(1 for v in values if v == target)
    n = len(values)
    return (lower + 0.5 * equal) / n
