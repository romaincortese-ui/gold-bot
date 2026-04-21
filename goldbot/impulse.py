"""Sprint 1 — Item 2.5: impulse confirmation as a replacement for OANDA
tick volume on the breakout entry.

Rationale (from memo): OANDA tick volume is the number of quote updates,
not traded contracts. In gold this correlates with spread widening and
volatility rather than with participation — it is a fake feature.

Replacement proxy used here is the **body-to-ATR ratio** of the breakout
candle: a candle with a close far from its open (relative to ATR) is a
genuine directional impulse regardless of how many quotes printed. On gold
M15, a body >= 0.40 * ATR identifies breakout candles that have historically
continued in the direction of the break; tick-volume ratios above 1.10x have
no such predictive relationship.

We also expose `realized_vol_ratio` as an alternative (ratio of last N bars'
range standard deviation vs the prior M bars') for callers that prefer it.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ImpulseSignal:
    body_atr_ratio: float
    range_atr_ratio: float
    direction: str  # "UP" | "DOWN" | "FLAT"


def body_atr_ratio(df: pd.DataFrame, atr: float) -> ImpulseSignal:
    """Compute body-to-ATR ratio of the most recent candle.

    A candle with body >= 0.40 * ATR is treated as a genuine impulse. We
    also surface range-to-ATR as a secondary metric.
    """
    if df is None or len(df) == 0 or atr <= 0:
        return ImpulseSignal(0.0, 0.0, "FLAT")
    last = df.iloc[-1]
    open_px = float(last["open"])
    close_px = float(last["close"])
    high_px = float(last["high"])
    low_px = float(last["low"])
    body = abs(close_px - open_px)
    rng = max(1e-12, high_px - low_px)
    direction = "UP" if close_px > open_px else "DOWN" if close_px < open_px else "FLAT"
    return ImpulseSignal(
        body_atr_ratio=body / atr,
        range_atr_ratio=rng / atr,
        direction=direction,
    )


def realized_vol_ratio(df: pd.DataFrame, *, fast: int = 3, slow: int = 20) -> float:
    """Ratio of recent range-stddev to baseline range-stddev.

    A value > 1.0 indicates the recent window is more volatile than the
    baseline — a rough proxy for "something is happening now". Used as an
    alternative impulse signal when body/ATR is noisy (e.g. doji breakouts).
    """
    if df is None or len(df) < slow + fast:
        return 1.0
    ranges = (df["high"] - df["low"]).astype(float)
    fast_std = float(ranges.iloc[-fast:].std(ddof=0) or 0.0)
    slow_std = float(ranges.iloc[-(slow + fast):-fast].std(ddof=0) or 0.0)
    if slow_std <= 0:
        return 1.0
    return fast_std / slow_std


def confirms_breakout(
    signal: ImpulseSignal,
    *,
    required_direction: str,
    body_atr_min: float,
) -> bool:
    """Return True if the impulse confirms a break in the desired direction.

    `required_direction` is "UP" for a long breakout, "DOWN" for a short.
    """
    if required_direction not in {"UP", "DOWN"}:
        return False
    if signal.direction != required_direction:
        return False
    return signal.body_atr_ratio >= body_atr_min
