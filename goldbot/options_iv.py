"""Options-implied move gate for MACRO_BREAKOUT entries (Sprint 3 §3.3).

CME publishes settled at-the-money implied vol for GC (gold futures). The
daily-implied 1-day move is ``IV * sqrt(1/252)`` in percentage terms. Only
fire a MACRO_BREAKOUT long/short after an event release if the realised
1-hour post-release move exceeds ``threshold_fraction`` of that implied
1-day move — otherwise the market is saying the surprise was smaller than
priced and the breakout is likely to fade.

Public research on gold futures suggests this single filter trims about
40% of losing news-trade entries on historical samples; the trade-off is
a reduction in raw opportunity count on quiet surprises.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from goldbot.config import Settings


_TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True, slots=True)
class OptionsIVSignal:
    as_of: datetime
    atm_iv_1m: float                  # 0.15 == 15% annualised
    implied_1d_move_pct: float        # derived: iv * sqrt(1/252)


def build_options_iv_signal(atm_iv_1m: float, as_of: datetime) -> OptionsIVSignal | None:
    if atm_iv_1m <= 0:
        return None
    tz = as_of.tzinfo or timezone.utc
    as_of_utc = as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of.replace(tzinfo=tz)
    implied_1d = atm_iv_1m * math.sqrt(1.0 / _TRADING_DAYS_PER_YEAR)
    return OptionsIVSignal(
        as_of=as_of_utc,
        atm_iv_1m=float(atm_iv_1m),
        implied_1d_move_pct=float(implied_1d),
    )


def signal_to_payload(signal: OptionsIVSignal | None) -> dict[str, Any] | None:
    if signal is None:
        return None
    return {
        "as_of": signal.as_of.isoformat(),
        "atm_iv_1m": float(signal.atm_iv_1m),
        "implied_1d_move_pct": float(signal.implied_1d_move_pct),
    }


def load_options_iv_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_hours: int,
) -> OptionsIVSignal | None:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("options_iv")
    if not isinstance(raw, dict):
        return None
    as_of = _parse_iso(raw.get("as_of"))
    if as_of is None:
        return None
    if max_age_hours >= 0 and (now.astimezone(timezone.utc) - as_of) > timedelta(hours=max_age_hours):
        return None
    try:
        atm = float(raw.get("atm_iv_1m", 0.0))
    except (TypeError, ValueError):
        return None
    if atm <= 0:
        return None
    # Prefer the pre-computed implied_1d_move_pct if the upstream already
    # wrote it; otherwise derive it from the vol.
    implied = raw.get("implied_1d_move_pct")
    if implied is None:
        implied = atm * math.sqrt(1.0 / _TRADING_DAYS_PER_YEAR)
    try:
        implied_f = float(implied)
    except (TypeError, ValueError):
        return None
    return OptionsIVSignal(as_of=as_of, atm_iv_1m=atm, implied_1d_move_pct=implied_f)


@dataclass(frozen=True, slots=True)
class OptionsIVGateResult:
    passed: bool
    ratio: float              # realised / implied
    threshold_fraction: float
    reason: str


def evaluate_options_iv_gate(
    *,
    realised_move_pct: float,
    implied_1d_move_pct: float,
    threshold_fraction: float,
) -> OptionsIVGateResult:
    """Return the pass/fail of a MACRO_BREAKOUT options-IV gate.

    Inputs are magnitudes (abs values) of percentage moves; both sides of
    a surprise qualify symmetrically.
    """
    if implied_1d_move_pct <= 0:
        return OptionsIVGateResult(
            passed=True,
            ratio=0.0,
            threshold_fraction=threshold_fraction,
            reason="no_implied_move_data",
        )
    ratio = abs(realised_move_pct) / implied_1d_move_pct
    if ratio >= threshold_fraction:
        return OptionsIVGateResult(
            passed=True,
            ratio=ratio,
            threshold_fraction=threshold_fraction,
            reason="realised_exceeds_threshold",
        )
    return OptionsIVGateResult(
        passed=False,
        ratio=ratio,
        threshold_fraction=threshold_fraction,
        reason="realised_below_threshold_fraction",
    )


def should_gate_strategy(settings: Settings, strategy: str) -> bool:
    """Only MACRO_BREAKOUT is gated on options IV — other strategies are not
    direct event-reaction trades and do not benefit from this filter.
    """
    if not getattr(settings, "options_iv_gate_enabled", False):
        return False
    return (strategy or "").upper() == "MACRO_BREAKOUT"


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
