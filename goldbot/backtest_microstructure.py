"""Realistic microstructure modelling for the gold backtest.

The default backtest uses a flat ``simulated_spread`` which over-estimates
realistic fills:

  * Spreads on XAU_USD vary ~3x intraday (Tokyo ~0.45, NY overlap ~0.18).
  * Inside the 2-minute window around tier-1 macro events spreads spike
    ~6-8x as liquidity providers pull quotes.
  * Stop-loss exits suffer adverse selection: realised slippage is ~1.5x
    the prevailing half-spread.
  * Sunday opens routinely gap 0.3-1.2% over Friday close; tight Friday
    stops would be filled at the gap-open price, not the stop price.
  * Holding XAU_USD long incurs a ~5% APR financing cost; shorts are
    roughly flat. This compounds materially over multi-day holds.

This module is pure (no I/O). The backtest engine consumes its primitives.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

# Hour-of-day relative spread multipliers for XAU_USD (UTC). Anchored so the
# week-long *median* hour is ~1.0; the named profile is "gold_m15" because it
# was calibrated against M15 OANDA tick data on the live demo book.
GOLD_HOURLY_SPREAD_MULTIPLIER: dict[int, float] = {
    0: 1.6,   # Tokyo open / liquidity vacuum
    1: 1.7,
    2: 1.7,
    3: 1.5,
    4: 1.4,
    5: 1.3,
    6: 1.1,
    7: 0.95,  # London open
    8: 0.85,
    9: 0.85,
    10: 0.9,
    11: 0.95,
    12: 0.75,  # NY pre-open / overlap start
    13: 0.7,   # NY overlap (tightest)
    14: 0.7,
    15: 0.75,
    16: 0.9,   # London close
    17: 1.0,
    18: 1.1,
    19: 1.2,   # NY late afternoon
    20: 1.3,
    21: 1.4,
    22: 1.5,   # post-NY close, no Asia yet
    23: 1.6,
}

WEEKEND_SPREAD_MULTIPLIER = 4.0  # if engine ever simulates weekend ticks


@dataclass(frozen=True, slots=True)
class SpreadModel:
    """Maps ``(now, base_spread, events)`` -> effective spread for one bar."""
    base_spread: float
    hourly_multiplier: Mapping[int, float] = field(default_factory=lambda: dict(GOLD_HOURLY_SPREAD_MULTIPLIER))
    news_window_minutes: int = 2
    news_multiplier: float = 6.0

    def effective_spread(self, now: datetime, events: Sequence[datetime] = ()) -> float:
        hour = int(now.hour)
        mult = float(self.hourly_multiplier.get(hour, 1.0))
        if self._inside_news_window(now, events):
            mult = max(mult, float(self.news_multiplier))
        return max(0.0, float(self.base_spread) * mult)

    def _inside_news_window(self, now: datetime, events: Iterable[datetime]) -> bool:
        if self.news_window_minutes <= 0:
            return False
        window = timedelta(minutes=int(self.news_window_minutes))
        for event_time in events:
            if event_time is None:
                continue
            if abs(now - event_time) <= window:
                return True
        return False


def exit_slippage_cost(*, half_spread: float, slippage_multiplier: float) -> float:
    """Adverse selection on stop-loss fills.

    The engine should subtract this *additional* per-unit cost when closing a
    trade with reason ``STOP_LOSS``, on top of the half-spread already paid
    on entry.
    """
    return max(0.0, float(half_spread) * float(slippage_multiplier))


def is_weekend_gap_boundary(prev_bar_time: datetime, current_bar_time: datetime) -> bool:
    """True if the bar pair straddles the Friday-close / Sunday-open gap.

    Triggers on any forward jump of >=24 hours that crosses Saturday.
    """
    if prev_bar_time is None or current_bar_time is None:
        return False
    delta = current_bar_time - prev_bar_time
    if delta < timedelta(hours=24):
        return False
    # Any 24h+ jump that includes a Saturday counts as a weekend boundary.
    cur = prev_bar_time
    while cur <= current_bar_time:
        if cur.weekday() == 5:  # Saturday
            return True
        cur += timedelta(days=1)
    return False


def weekend_gap_adjusted_stop(
    *,
    direction: str,
    stop_price: float,
    monday_open_price: float,
    weekend_was_crossed: bool,
) -> tuple[bool, float]:
    """If a stop would have been triggered on the Monday gap, compute the fill.

    Returns ``(stopped, fill_price)``. ``fill_price`` is the actual price
    where the stop is filled — for an adverse gap the fill is at the
    Monday-open price, not the stop price.
    """
    if not weekend_was_crossed:
        return False, float(stop_price)
    direction = (direction or "").upper()
    if direction == "LONG":
        if monday_open_price <= stop_price:
            return True, float(monday_open_price)
    elif direction == "SHORT":
        if monday_open_price >= stop_price:
            return True, float(monday_open_price)
    return False, float(stop_price)


def financing_charge(
    *,
    direction: str,
    notional: float,
    hours_held: float,
    long_apr: float,
    short_apr: float,
) -> float:
    """Per-position overnight financing accrual in account currency.

    Returns a positive number representing a *cost* to subtract from PnL.
    Hours are accrued as a fraction of 365 * 24 against the relevant APR.
    """
    direction = (direction or "").upper()
    apr = float(long_apr) if direction == "LONG" else float(short_apr)
    if apr <= 0 or notional <= 0 or hours_held <= 0:
        return 0.0
    return float(notional) * apr * (float(hours_held) / (365.0 * 24.0))


def hours_between(start: datetime, end: datetime) -> float:
    """UTC-aware hours-elapsed helper, never negative."""
    if start is None or end is None:
        return 0.0
    delta = end - start
    return max(0.0, delta.total_seconds() / 3600.0)


def parse_event_times(events: Iterable[object]) -> list[datetime]:
    """Best-effort extraction of ``occurs_at`` (datetime) from event objects."""
    out: list[datetime] = []
    for event in events:
        ts = getattr(event, "occurs_at", None)
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            out.append(ts)
    return out
