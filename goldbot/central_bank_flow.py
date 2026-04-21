"""Central-bank gold demand tracker (Gold-bot Q2 §4.3).

The World Gold Council publishes quarterly net central-bank gold purchases
(tonnes). Since 2022, net buying has averaged 250–400 tonnes per quarter,
well above the historical norm, and systematic short-gold strategies have
underperformed in quarters with net buying above ~300 tonnes.

This module provides a simple ``CentralBankFlowSignal`` plus a veto
function: when the latest quarterly net buying is at or above
``high_demand_tonnes_threshold``, veto new ``EXHAUSTION_REVERSAL`` shorts
(the strategy most directly in the path of a structural bid). All other
strategies and all long entries are unaffected.
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
class CentralBankFlowSignal:
    as_of: datetime
    quarter_label: str                     # e.g. "2026Q1"
    net_buying_tonnes: float               # signed; positive = net buying


def signal_to_payload(signal: CentralBankFlowSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "quarter_label": signal.quarter_label,
        "net_buying_tonnes": float(signal.net_buying_tonnes),
    }


def load_central_bank_flow_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_days: int,
) -> CentralBankFlowSignal | None:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("central_bank_flow")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_days >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(days=max_age_days):
        return None
    try:
        tonnes = float(raw.get("net_buying_tonnes", 0.0))
    except (TypeError, ValueError):
        return None
    return CentralBankFlowSignal(
        as_of=as_of,
        quarter_label=str(raw.get("quarter_label") or ""),
        net_buying_tonnes=tonnes,
    )


def apply_central_bank_short_veto(
    settings: Settings,
    opportunity: Opportunity,
    signal: CentralBankFlowSignal | None,
) -> Opportunity | None:
    """Veto EXHAUSTION_REVERSAL shorts in high-demand quarters.

    Returns ``None`` on a veto (consistent with the real-yield / co-trade
    overlays), otherwise returns the opportunity with metadata annotations.
    The filter only fires when:

    * the feature flag is on, and
    * a fresh signal is available, and
    * the opportunity is a SHORT, and
    * ``strategy`` is in the configured veto allowlist (default only
      ``EXHAUSTION_REVERSAL`` — other strategies are gated elsewhere).
    """
    if not getattr(settings, "central_bank_flow_enabled", False) or signal is None:
        return opportunity

    threshold = float(getattr(settings, "central_bank_high_demand_tonnes", 300.0))
    strategy_allowlist_raw = str(
        getattr(settings, "central_bank_short_veto_strategies", "EXHAUSTION_REVERSAL")
    )
    allowlist = {s.strip().upper() for s in strategy_allowlist_raw.split(",") if s.strip()}

    direction = (opportunity.direction or "").upper()
    strategy = (opportunity.strategy or "").upper()

    metadata = dict(opportunity.metadata)
    metadata["central_bank_net_tonnes"] = float(signal.net_buying_tonnes)
    metadata["central_bank_quarter"] = signal.quarter_label

    if (
        direction == "SHORT"
        and strategy in allowlist
        and signal.net_buying_tonnes >= threshold
    ):
        metadata["central_bank_filter"] = "high_demand_short_veto"
        opportunity.metadata = metadata
        return None

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
