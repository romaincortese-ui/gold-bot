"""Cross-asset co-trade gates for XAU_USD (Sprint 3 §3.1).

Gold is a basket, not a symbol. Professional desks gate XAU trades by the
co-movement of three ambient risk signals:

* ES (S&P E-mini) — risk-on days (ES up > ``risk_off_threshold``) historically
  give a negative edge on new long-gold entries.
* USD/CNH — PBoC fixing stress (USD/CNH up > ``cnh_stress_threshold``) tends
  to bid gold, so do not fade it with fresh shorts.
* DXY — weak DXY + falling real yields is the classic long-gold regime; we
  size up accordingly.

The runtime consumes the latest daily changes via the shared macro state
file (``gold_macro_state.json#co_trade``); the fetcher is intentionally out
of scope and will be filled in by the macro-engine job or a sidecar.
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
class CoTradeSignal:
    """Same-session cross-asset snapshot for XAU context."""
    as_of: datetime
    es_daily_change_pct: float | None   # +0.015 = ES up 1.5%
    cnh_daily_change_pct: float | None  # +0.004 = USD/CNH up 0.4%
    dxy_daily_change_pct: float | None  # +0.003 = DXY up 0.3%


def signal_to_payload(signal: CoTradeSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "es_daily_change_pct": signal.es_daily_change_pct,
        "cnh_daily_change_pct": signal.cnh_daily_change_pct,
        "dxy_daily_change_pct": signal.dxy_daily_change_pct,
    }


def load_co_trade_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_hours: int,
) -> CoTradeSignal | None:
    """Load a co-trade snapshot from ``gold_macro_state.json#co_trade``."""
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("co_trade")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_hours >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(hours=max_age_hours):
        return None
    return CoTradeSignal(
        as_of=as_of,
        es_daily_change_pct=_coerce_optional_float(raw.get("es_daily_change_pct")),
        cnh_daily_change_pct=_coerce_optional_float(raw.get("cnh_daily_change_pct")),
        dxy_daily_change_pct=_coerce_optional_float(raw.get("dxy_daily_change_pct")),
    )


def apply_co_trade_gates(
    settings: Settings,
    opportunity: Opportunity,
    signal: CoTradeSignal | None,
) -> Opportunity | None:
    """Apply co-trade gates to an opportunity.

    * Vetoes long-XAU entries on strong risk-on ES days.
    * Vetoes short-XAU entries on CNH-stress days.
    * Scales risk_multiplier up on favourable-DXY long-XAU days.

    Never vetoes a short on a risk-on day (that is actually additive signal),
    and never vetoes a long on a DXY-strong day if other gates pass — level
    gating for DXY is handled in the existing USD regime filter.
    """
    if not getattr(settings, "co_trade_gates_enabled", False) or signal is None:
        return opportunity

    direction = (opportunity.direction or "").upper()
    metadata = dict(opportunity.metadata)

    es = signal.es_daily_change_pct
    cnh = signal.cnh_daily_change_pct
    dxy = signal.dxy_daily_change_pct

    risk_off_long_veto = float(getattr(settings, "co_trade_es_risk_on_long_veto_pct", 0.015))
    cnh_stress_threshold = float(getattr(settings, "co_trade_cnh_stress_short_veto_pct", 0.004))
    dxy_weak_threshold = float(getattr(settings, "co_trade_dxy_weak_favourable_pct", -0.003))
    favourable_size_mult = float(getattr(settings, "co_trade_favourable_size_mult", 1.25))

    # 1) Risk-on ES day → no new long-gold entries.
    if direction == "LONG" and es is not None and es >= risk_off_long_veto:
        metadata["co_trade_filter"] = "risk_on_equity_long_veto"
        metadata["co_trade_es_change_pct"] = es
        opportunity.metadata = metadata
        return None

    # 2) CNH stress → no new short-gold entries.
    if direction == "SHORT" and cnh is not None and cnh >= cnh_stress_threshold:
        metadata["co_trade_filter"] = "cnh_stress_short_veto"
        metadata["co_trade_cnh_change_pct"] = cnh
        opportunity.metadata = metadata
        return None

    # 3) Weak-DXY + long-gold → size up (handled through risk_multiplier).
    if direction == "LONG" and dxy is not None and dxy <= dxy_weak_threshold and favourable_size_mult > 1.0:
        current_mult = float(metadata.get("risk_multiplier", 1.0) or 1.0)
        metadata["risk_multiplier"] = current_mult * favourable_size_mult
        metadata["co_trade_filter"] = "dxy_weak_long_boost"
        metadata["co_trade_dxy_change_pct"] = dxy

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
