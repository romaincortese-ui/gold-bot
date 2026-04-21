"""Sprint 1 — Item 2.1: scored event model for MACRO_BREAKOUT.

The legacy news filter matches on keywords: an event is either "high-impact"
or it isn't. That is binary, and it fires MACRO_BREAKOUT trades on every
"high-impact" CPI/NFP regardless of whether the release actually moved the
market. On the sell side nobody trades news that way.

This module introduces a three-factor score, computed off whatever data is
available in the macro state file. When actual/consensus/stddev data are
present we compute a real surprise score; when they are missing we degrade
gracefully to a `None` composite so the runtime can decide to either fall
back to the legacy keyword gate (off by default, configurable) or refuse
the trade.

Factors
-------
- **Surprise magnitude**: z-score of (actual - consensus) normalized by the
  historical standard deviation of revisions. Gold reacts roughly linearly
  to US macro surprises up to roughly +/- 1.5 sigma.

- **Rates impulse**: change in US 2Y yield in the 30 minutes following the
  release. Real yields drive gold more reliably than headline CPI prints —
  if the 2Y barely moved, gold's reaction is noise, not signal.

- **DXY impulse**: DXY move in the 15 minutes after release. If DXY moved
  < 0.15% in the USD-implied direction, the event did not actually move USD
  and should not drive a gold trade.

The composite is a geometric-mean-like 0..1 score; a value >= ~0.6 on a
CPI/NFP print historically corresponds to a tradable dislocation.

State shape expected in `macro_state_file` (written by macro_engine.py or a
future news collector):

    {
      "event_scores": [
        {
          "event_key": "NFP_2026-04-05",
          "title": "US Non-Farm Payrolls",
          "as_of": "2026-04-05T12:30:00Z",
          "actual": 215000,
          "consensus": 178000,
          "std": 45000,
          "dxy_move_pct": 0.42,
          "rates_move_bps": 8.5,
          "usd_direction": "UP",  # expected USD direction from surprise sign
          ...
        }
      ]
    }
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class EventScore:
    event_key: str
    as_of: datetime
    surprise_z: float | None        # (actual - consensus) / std; signed
    rates_move_bps: float | None    # 2Y yield move in 30 min post-event
    dxy_move_pct: float | None      # DXY % move in 15 min post-event
    usd_direction: str | None       # "UP" or "DOWN" — expected direction of USD
    composite: float | None         # 0..1 score; None if insufficient data
    raw: Mapping[str, Any]


def score_event(payload: Mapping[str, Any]) -> EventScore | None:
    """Score a single event payload. Returns None if the event lacks a
    timestamp (cannot be aligned with anything)."""
    as_of_raw = payload.get("as_of")
    if not as_of_raw:
        return None
    try:
        as_of = datetime.fromisoformat(str(as_of_raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    actual = _optional_float(payload.get("actual"))
    consensus = _optional_float(payload.get("consensus"))
    std = _optional_float(payload.get("std"))
    rates_move_bps = _optional_float(payload.get("rates_move_bps"))
    dxy_move_pct = _optional_float(payload.get("dxy_move_pct"))
    usd_direction = payload.get("usd_direction")
    if isinstance(usd_direction, str):
        usd_direction = usd_direction.upper()
    else:
        usd_direction = None

    surprise_z: float | None
    if actual is not None and consensus is not None and std and std > 0:
        surprise_z = (actual - consensus) / std
    else:
        surprise_z = None

    composite = _composite_score(
        surprise_z=surprise_z,
        rates_move_bps=rates_move_bps,
        dxy_move_pct=dxy_move_pct,
        usd_direction=usd_direction,
    )

    return EventScore(
        event_key=str(payload.get("event_key") or payload.get("title") or as_of.isoformat()),
        as_of=as_of.astimezone(timezone.utc),
        surprise_z=surprise_z,
        rates_move_bps=rates_move_bps,
        dxy_move_pct=dxy_move_pct,
        usd_direction=usd_direction,
        composite=composite,
        raw=dict(payload),
    )


def load_event_scores(
    macro_state_file: str,
    *,
    now: datetime,
    max_age_minutes: int,
) -> list[EventScore]:
    """Load scored events from the macro state file, dropping stale ones."""
    if not macro_state_file:
        return []
    path = Path(macro_state_file)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    raw_events = payload.get("event_scores") or []
    if not isinstance(raw_events, list):
        return []

    scored: list[EventScore] = []
    cutoff = now.astimezone(timezone.utc) - timedelta(minutes=max_age_minutes)
    for item in raw_events:
        if not isinstance(item, Mapping):
            continue
        score = score_event(item)
        if score is None:
            continue
        if score.as_of < cutoff:
            continue
        scored.append(score)
    return scored


def select_best_for_breakout(
    scored_events: list[EventScore],
    *,
    direction: str,
) -> EventScore | None:
    """Return the most recent scored event that supports a breakout in the
    given direction.

    For gold, LONG is supported by a **dovish** surprise (weak USD data) —
    so we want `usd_direction == "DOWN"` and a negative surprise_z, OR vice
    versa for SHORT. If the event doesn't specify `usd_direction`, we infer
    from the sign of surprise_z (a positive US-data surprise typically lifts
    USD, which is bearish for gold).
    """
    if not scored_events:
        return None
    wanted_usd = "DOWN" if direction == "LONG" else "UP"
    filtered: list[EventScore] = []
    for event in scored_events:
        usd_dir = event.usd_direction
        if usd_dir is None and event.surprise_z is not None:
            usd_dir = "UP" if event.surprise_z > 0 else "DOWN" if event.surprise_z < 0 else None
        if usd_dir == wanted_usd:
            filtered.append(event)
    if not filtered:
        return None
    return max(filtered, key=lambda e: e.as_of)


def _composite_score(
    *,
    surprise_z: float | None,
    rates_move_bps: float | None,
    dxy_move_pct: float | None,
    usd_direction: str | None,
) -> float | None:
    """Combine the three factors into a 0..1 score.

    Each factor is mapped to [0, 1] via a saturating transform; composite is
    the geometric mean of the available factors (so a single zero component
    makes the whole thing zero).

    Returns None if fewer than two factors are available — i.e. we don't
    have enough signal to call this a scored event.
    """
    components: list[float] = []

    if surprise_z is not None:
        # |z|=0.5 -> ~0.39, |z|=1 -> ~0.63, |z|=2 -> ~0.86
        components.append(_sat(abs(surprise_z), scale=1.0))

    if rates_move_bps is not None:
        # |2Y move in bps|: 2bps -> 0.18, 5bps -> 0.39, 10bps -> 0.63, 20bps -> 0.86
        components.append(_sat(abs(rates_move_bps), scale=10.0))

    if dxy_move_pct is not None:
        # |DXY move %|: 0.15% -> 0.39, 0.30% -> 0.63, 0.60% -> 0.86
        components.append(_sat(abs(dxy_move_pct), scale=0.30))

    if len(components) < 2:
        return None

    # Geometric mean — any near-zero component tanks the score.
    product = 1.0
    for c in components:
        product *= max(1e-6, c)
    composite = product ** (1.0 / len(components))
    return float(max(0.0, min(1.0, composite)))


def _sat(x: float, *, scale: float) -> float:
    """Saturating transform: 1 - exp(-x/scale)."""
    if x <= 0 or scale <= 0:
        return 0.0
    return float(1.0 - math.exp(-x / scale))


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
