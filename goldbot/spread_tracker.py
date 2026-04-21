"""Sprint 1 — Item 2.2: adaptive spread cap.

Gold spread on OANDA is not constant. On a normal London/NY day XAU/USD
quotes 0.18-0.35 pips; during NFP it blows out to 1.0-2.5 pips for 30-90
seconds; during Asian thin hours it sits at 0.45-0.60. A single static
`MAX_ENTRY_SPREAD = 0.80` vetoes good setups at noon London and simultaneously
*permits* fills during NFP widening.

The desk fix is to compare the current spread against a rolling median and
reject fills that exceed `max(floor, median * multiplier)`. This naturally:
  - tightens during calm periods (protects against thin-book fills),
  - widens during event-driven regimes (avoids vetoing every setup),
  - still enforces a hard static upper bound to catch feed errors.

This module provides a stateful tracker. The runtime feeds it every quote,
and `allowed_spread()` returns the live threshold.
"""

from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class SpreadSample:
    timestamp: datetime
    spread: float


class SpreadTracker:
    """Rolling-median spread tracker with graceful warm-up.

    Until `min_samples` observations are collected, `allowed_spread()` falls
    back to the static `static_cap` so the bot is never over-permissive
    during a fresh boot.
    """

    def __init__(
        self,
        *,
        window_minutes: int,
        multiplier: float,
        floor: float,
        min_samples: int,
        static_cap: float,
    ) -> None:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be > 0")
        if multiplier <= 1.0:
            raise ValueError("multiplier must be > 1.0")
        if floor <= 0 or static_cap <= 0:
            raise ValueError("floor and static_cap must be > 0")
        if min_samples < 1:
            raise ValueError("min_samples must be >= 1")
        self._window = timedelta(minutes=window_minutes)
        self._multiplier = float(multiplier)
        self._floor = float(floor)
        self._min_samples = int(min_samples)
        self._static_cap = float(static_cap)
        self._samples: deque[SpreadSample] = deque()

    def record(self, spread: float, *, now: datetime | None = None) -> None:
        """Append a spread observation, evicting stale samples."""
        if spread is None:
            return
        spread_f = float(spread)
        if spread_f < 0 or spread_f != spread_f:  # NaN guard
            return
        ts = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._samples.append(SpreadSample(ts, spread_f))
        self._evict(ts)

    def _evict(self, reference: datetime) -> None:
        cutoff = reference - self._window
        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    def sample_count(self) -> int:
        return len(self._samples)

    def median(self) -> float | None:
        if not self._samples:
            return None
        return statistics.median(sample.spread for sample in self._samples)

    def allowed_spread(self, *, now: datetime | None = None) -> float:
        """Return the current allowed maximum spread.

        Always clamped to the static cap (upper bound, prevents runaway on a
        bad feed) and the floor (lower bound, prevents over-tight rejection
        in a calm market where median can drop below a tradable threshold).
        """
        if now is not None:
            self._evict(now.astimezone(timezone.utc))
        if len(self._samples) < self._min_samples:
            return self._static_cap  # warm-up: defer to static cap
        median = self.median() or 0.0
        dynamic = max(self._floor, median * self._multiplier)
        return min(dynamic, self._static_cap)

    def is_acceptable(self, spread: float, *, now: datetime | None = None) -> bool:
        return float(spread) <= self.allowed_spread(now=now) + 1e-9
