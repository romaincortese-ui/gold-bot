"""Volatility-regime classifier (Sprint 3 §3.2).

Gold's intraday character divides cleanly into three regimes:

* **quiet_carry** — low realised vol (ATR% < ``quiet_atr_pct_max``). Mean
  reversion works; trend-following loses the chop premium.
* **trend**     — medium realised vol and directional drift (ATR% between
  ``quiet_atr_pct_max`` and ``spike_atr_pct_min``). Classic trend-pullback
  edge; exhaustion reversals get run over.
* **spike**    — high realised vol (ATR% >= ``spike_atr_pct_min`` or an
  active news burst). Only breakout-with-volume survives; mean-reversion is
  burning premium in front of a moving market.

The classifier is a pure function so it can be unit-tested and reused from
the backtest engine for per-bar regime labelling.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from goldbot.config import Settings


@dataclass(frozen=True, slots=True)
class RegimeClassification:
    regime: str              # "quiet_carry" | "trend" | "spike" | "neutral"
    atr_pct: float            # ATR / close
    news_burst: bool
    reason: str


def classify_regime(
    *,
    atr_pct: float,
    quiet_atr_pct_max: float,
    trend_atr_pct_max: float,
    spike_atr_pct_min: float,
    news_burst: bool = False,
) -> RegimeClassification:
    """Classify the current 24-hour regime.

    ``atr_pct`` is ATR divided by close on the timeframe the caller feeds
    (typically H1 or H4 for the 24h classifier). ``news_burst`` forces a
    spike regime regardless of ATR — when a tier-1 release just printed the
    market is informationally spiky even if the candle hasn't expanded yet.
    """
    if atr_pct < 0:
        return RegimeClassification(
            regime="neutral",
            atr_pct=atr_pct,
            news_burst=news_burst,
            reason="atr_pct_negative",
        )

    if news_burst or atr_pct >= spike_atr_pct_min:
        return RegimeClassification(
            regime="spike",
            atr_pct=atr_pct,
            news_burst=news_burst,
            reason="news_burst" if news_burst else "atr_pct_above_spike_floor",
        )
    if atr_pct <= quiet_atr_pct_max:
        return RegimeClassification(
            regime="quiet_carry",
            atr_pct=atr_pct,
            news_burst=False,
            reason="atr_pct_below_quiet_ceiling",
        )
    if atr_pct <= trend_atr_pct_max:
        return RegimeClassification(
            regime="trend",
            atr_pct=atr_pct,
            news_burst=False,
            reason="atr_pct_in_trend_band",
        )
    # Between trend ceiling and spike floor — treat as trend (upper tail of
    # trend band) rather than declaring neutral, to avoid dropping obvious
    # directional setups that are close to but not quite at the spike floor.
    return RegimeClassification(
        regime="trend",
        atr_pct=atr_pct,
        news_burst=False,
        reason="atr_pct_above_trend_band_below_spike",
    )


_DEFAULT_REGIME_STRATEGY_MAP: dict[str, tuple[str, ...]] = {
    "quiet_carry": ("EXHAUSTION_REVERSAL",),
    "trend": ("TREND_PULLBACK", "MACRO_BREAKOUT"),
    "spike": ("MACRO_BREAKOUT",),
    "neutral": ("MACRO_BREAKOUT", "TREND_PULLBACK", "EXHAUSTION_REVERSAL"),
}


def strategy_allowed_in_regime(
    regime: str,
    strategy: str,
    *,
    quiet_strategies: Iterable[str] | None = None,
    trend_strategies: Iterable[str] | None = None,
    spike_strategies: Iterable[str] | None = None,
) -> bool:
    """Return True if ``strategy`` is allowed under ``regime``.

    Custom mappings can be passed in via kwargs; otherwise the default map
    above is used. ``neutral`` always allows every strategy so the regime
    filter fails open rather than blocking every trade when classification
    data is missing.
    """
    if regime == "neutral":
        return True
    allowed: tuple[str, ...]
    if regime == "quiet_carry":
        allowed = tuple(quiet_strategies) if quiet_strategies is not None else _DEFAULT_REGIME_STRATEGY_MAP["quiet_carry"]
    elif regime == "trend":
        allowed = tuple(trend_strategies) if trend_strategies is not None else _DEFAULT_REGIME_STRATEGY_MAP["trend"]
    elif regime == "spike":
        allowed = tuple(spike_strategies) if spike_strategies is not None else _DEFAULT_REGIME_STRATEGY_MAP["spike"]
    else:
        return True
    return strategy.upper() in {s.upper() for s in allowed}


def classify_from_settings(
    settings: Settings,
    *,
    atr_pct: float,
    news_burst: bool = False,
) -> RegimeClassification:
    """Convenience wrapper reading thresholds from ``Settings``."""
    return classify_regime(
        atr_pct=atr_pct,
        quiet_atr_pct_max=float(getattr(settings, "regime_quiet_atr_pct_max", 0.006)),
        trend_atr_pct_max=float(getattr(settings, "regime_trend_atr_pct_max", 0.016)),
        spike_atr_pct_min=float(getattr(settings, "regime_spike_atr_pct_min", 0.016)),
        news_burst=news_burst,
    )


def parse_strategy_csv(value: str) -> tuple[str, ...]:
    """Split a comma-separated strategy allowlist from settings."""
    if not value:
        return ()
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())
