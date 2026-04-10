import math
from datetime import datetime, timedelta, timezone

import pandas as pd

from goldbot.config import Settings
from goldbot.indicators import (
    calc_atr,
    calc_ema,
    calc_macd,
    calc_rsi,
    consolidation_box,
    detect_divergence,
    is_bearish_engulfing,
    is_bullish_engulfing,
    is_pin_bar,
    nearest_support_resistance,
)
from goldbot.models import CalendarEvent, Opportunity
from goldbot.volume_oracle import BreakoutVolumeSignal


USD_PROXY_COMPONENTS: tuple[tuple[str, float], ...] = (
    ("EUR_USD", -1.0),
    ("GBP_USD", -0.75),
    ("USD_JPY", 1.0),
)


def score_macro_breakout(
    settings: Settings,
    now: datetime,
    session_name: str,
    df_m15: pd.DataFrame,
    df_h1: pd.DataFrame,
    events: list[CalendarEvent],
    breakout_volume_signal: BreakoutVolumeSignal | None = None,
) -> Opportunity | None:
    if df_m15 is None or df_h1 is None or len(df_m15) < 40 or len(df_h1) < settings.breakout_box_hours + 8:
        return None
    if settings.breakout_overlap_only and session_name != "OVERLAP":
        return None
    recent_events = [
        event
        for event in events
        if now - timedelta(hours=settings.breakout_news_lookback_hours) <= event.occurs_at <= now - timedelta(minutes=settings.post_news_settle_minutes)
    ]
    if not recent_events:
        return None

    latest_event = recent_events[-1]
    pre_event_end = latest_event.occurs_at
    pre_event_start = pre_event_end - timedelta(hours=settings.breakout_box_hours)
    box_slice = df_h1[(df_h1["time"] >= pre_event_start) & (df_h1["time"] < pre_event_end)]
    if len(box_slice) < max(8, settings.breakout_box_hours // 2):
        return None

    box = consolidation_box(box_slice, len(box_slice), settings.atr_period)
    if box["width_atr_ratio"] <= 0 or box["width_atr_ratio"] > settings.breakout_min_box_atr_ratio:
        return None

    atr = calc_atr(df_m15, settings.atr_period)
    buffer_size = atr * settings.breakout_buffer_atr
    last_close = float(df_m15["close"].iloc[-1])
    recent_closes = df_m15["close"].tail(3)
    breakout_volume = float(df_m15["volume"].iloc[-1])
    avg_volume = float(df_m15["volume"].iloc[-21:-1].mean()) if len(df_m15) >= 21 else breakout_volume
    volume_ratio = breakout_volume / avg_volume if avg_volume > 0 else 1.0
    volume_confirmation = _confirm_breakout_volume(settings, volume_ratio, breakout_volume_signal)
    if volume_confirmation is None:
        return None

    if last_close > box["high"] + buffer_size and float(recent_closes.min()) > box["high"]:
        stop_price = box["low"] - buffer_size
        risk = last_close - stop_price
        score = 70 + min(20, ((last_close - box["high"]) / max(atr, 1e-9)) * 10)
        return Opportunity(
            strategy="MACRO_BREAKOUT",
            direction="LONG",
            score=score,
            entry_price=last_close,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale=f"Post-news break above {box['high']:.2f} after {latest_event.title}",
            metadata={
                "event": latest_event.title,
                "box_high": box["high"],
                "box_low": box["low"],
                "atr": atr,
                "volume_ratio": round(volume_ratio, 2),
                **volume_confirmation,
            },
            exit_plan=_build_exit_plan(settings, "LONG", last_close, risk, atr, timeframe="M15"),
        )
    if last_close < box["low"] - buffer_size and float(recent_closes.max()) < box["low"]:
        stop_price = box["high"] + buffer_size
        risk = stop_price - last_close
        score = 70 + min(20, ((box["low"] - last_close) / max(atr, 1e-9)) * 10)
        return Opportunity(
            strategy="MACRO_BREAKOUT",
            direction="SHORT",
            score=score,
            entry_price=last_close,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale=f"Post-news break below {box['low']:.2f} after {latest_event.title}",
            metadata={
                "event": latest_event.title,
                "box_high": box["high"],
                "box_low": box["low"],
                "atr": atr,
                "volume_ratio": round(volume_ratio, 2),
                **volume_confirmation,
            },
            exit_plan=_build_exit_plan(settings, "SHORT", last_close, risk, atr, timeframe="M15"),
        )
    return None


def _confirm_breakout_volume(
    settings: Settings,
    tick_volume_ratio: float,
    breakout_volume_signal: BreakoutVolumeSignal | None,
) -> dict[str, float | str | None] | None:
    external_ratio = breakout_volume_signal.volume_ratio if breakout_volume_signal is not None else None
    external_source = breakout_volume_signal.source if breakout_volume_signal is not None else None

    if settings.breakout_volume_mode == "tick":
        if tick_volume_ratio < settings.breakout_min_volume_ratio:
            return None
        return {
            "tick_volume_ratio": round(tick_volume_ratio, 2),
            "external_volume_ratio": None,
            "volume_confirmation": "tick",
            "external_volume_source": None,
        }

    if external_ratio is None or external_ratio < settings.breakout_external_min_volume_ratio:
        return None

    if settings.breakout_volume_mode == "hybrid" and tick_volume_ratio < settings.breakout_min_volume_ratio:
        return None

    return {
        "tick_volume_ratio": round(tick_volume_ratio, 2),
        "external_volume_ratio": round(external_ratio, 2),
        "volume_confirmation": settings.breakout_volume_mode,
        "external_volume_source": external_source,
    }


def score_exhaustion_reversal(settings: Settings, df_h4: pd.DataFrame, df_d1: pd.DataFrame) -> Opportunity | None:
    if df_h4 is None or df_d1 is None or len(df_h4) < 80 or len(df_d1) < 40:
        return None
    price = float(df_h4["close"].iloc[-1])
    atr = calc_atr(df_h4, settings.atr_period)
    rsi_h4 = calc_rsi(df_h4["close"])
    macd_h4 = calc_macd(df_h4)
    levels = nearest_support_resistance(df_d1, settings.exhaustion_sr_lookback)
    divergence = detect_divergence(df_h4, lookback=50)

    near_resistance = abs(price - levels["resistance"]) <= atr
    near_support = abs(price - levels["support"]) <= atr

    if divergence["bearish"] and near_resistance and rsi_h4 >= settings.exhaustion_rsi_overbought:
        stop_price = levels["resistance"] + atr * 0.5
        risk = stop_price - price
        score = 75 + min(10, max(0.0, rsi_h4 - settings.exhaustion_rsi_overbought))
        return Opportunity(
            strategy="EXHAUSTION_REVERSAL",
            direction="SHORT",
            score=score,
            entry_price=price,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale="Bearish divergence at higher-timeframe resistance",
            metadata={"rsi": rsi_h4, "resistance": levels["resistance"], "atr": atr},
            exit_plan=_build_exit_plan(settings, "SHORT", price, risk, atr, timeframe="H1"),
        )

    if divergence["bullish"] and near_support and rsi_h4 <= settings.exhaustion_rsi_oversold:
        stop_price = levels["support"] - atr * 0.5
        risk = price - stop_price
        score = 75 + min(10, max(0.0, settings.exhaustion_rsi_oversold - rsi_h4))
        return Opportunity(
            strategy="EXHAUSTION_REVERSAL",
            direction="LONG",
            score=score,
            entry_price=price,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale="Bullish divergence at higher-timeframe support",
            metadata={"rsi": rsi_h4, "support": levels["support"], "atr": atr},
            exit_plan=_build_exit_plan(settings, "LONG", price, risk, atr, timeframe="H1"),
        )
    return None


def score_trend_pullback(
    settings: Settings,
    df_h1: pd.DataFrame,
    df_h4: pd.DataFrame,
    usd_proxy_h4: dict[str, pd.DataFrame] | None = None,
) -> Opportunity | None:
    if df_h1 is None or df_h4 is None or len(df_h1) < 80 or len(df_h4) < settings.trend_ema_slow + 5:
        return None
    close_h4 = df_h4["close"]
    ema_fast = calc_ema(close_h4, settings.trend_ema_fast)
    ema_slow = calc_ema(close_h4, settings.trend_ema_slow)
    h1_confirm_ema = calc_ema(df_h1["close"], settings.trend_h1_confirm_ema_period)
    trigger_price = float(df_h1["close"].iloc[-1])
    atr_h4 = calc_atr(df_h4, settings.atr_period)
    if atr_h4 <= 0:
        return None

    bullish_trend = float(ema_fast.iloc[-1]) > float(ema_slow.iloc[-1])
    bearish_trend = float(ema_fast.iloc[-1]) < float(ema_slow.iloc[-1])
    if not bullish_trend and not bearish_trend:
        return None

    ema_fast_value = float(ema_fast.iloc[-1])
    ema_slow_value = float(ema_slow.iloc[-1])
    trend_strength = abs(ema_fast_value - ema_slow_value) / atr_h4
    if trend_strength < settings.trend_min_strength_atr:
        return None
    slope_bars = min(settings.trend_fast_slope_bars, len(ema_fast) - 1)
    if slope_bars <= 0:
        return None
    ema_fast_slope_atr = (ema_fast_value - float(ema_fast.iloc[-(slope_bars + 1)])) / atr_h4
    h1_confirm_value = float(h1_confirm_ema.iloc[-1])
    usd_regime_bias = compute_usd_regime_bias(settings, usd_proxy_h4)

    if bullish_trend and (is_bullish_engulfing(df_h1) or is_pin_bar(df_h1, "LONG")):
        if ema_fast_slope_atr < settings.trend_min_slope_atr or trigger_price < h1_confirm_value:
            return None
        if settings.usd_regime_filter_enabled and usd_regime_bias is not None and usd_regime_bias >= settings.usd_regime_min_bias_atr:
            return None
        support_probe = float(df_h1["low"].tail(3).min())
        pullback_gap = abs(support_probe - ema_fast_value)
        if pullback_gap > atr_h4 * settings.trend_pullback_atr_tolerance:
            return None
        stop_price = min(float(df_h1["low"].tail(5).min()), float(ema_fast.iloc[-1])) - atr_h4 * 0.6
        risk = trigger_price - stop_price
        score = 68 + min(18, trend_strength * 6)
        return Opportunity(
            strategy="TREND_PULLBACK",
            direction="LONG",
            score=score,
            entry_price=trigger_price,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale="H4 uptrend pullback into 50 EMA with bullish confirmation candle",
            metadata={
                "ema_fast": ema_fast_value,
                "ema_slow": ema_slow_value,
                "atr": atr_h4,
                "trend_strength_atr": round(trend_strength, 3),
                "ema_fast_slope_atr": round(ema_fast_slope_atr, 3),
                "usd_regime_bias_atr": round(usd_regime_bias, 3) if usd_regime_bias is not None else None,
            },
            exit_plan=_build_exit_plan(settings, "LONG", trigger_price, risk, atr_h4, timeframe="H1"),
        )

    if bearish_trend and (is_bearish_engulfing(df_h1) or is_pin_bar(df_h1, "SHORT")):
        if ema_fast_slope_atr > -settings.trend_min_slope_atr or trigger_price > h1_confirm_value:
            return None
        if settings.usd_regime_filter_enabled and usd_regime_bias is not None and usd_regime_bias <= -settings.usd_regime_min_bias_atr:
            return None
        resistance_probe = float(df_h1["high"].tail(3).max())
        pullback_gap = abs(resistance_probe - ema_fast_value)
        if pullback_gap > atr_h4 * settings.trend_pullback_atr_tolerance:
            return None
        stop_price = max(float(df_h1["high"].tail(5).max()), float(ema_fast.iloc[-1])) + atr_h4 * 0.6
        risk = stop_price - trigger_price
        score = 68 + min(18, trend_strength * 6)
        return Opportunity(
            strategy="TREND_PULLBACK",
            direction="SHORT",
            score=score,
            entry_price=trigger_price,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale="H4 downtrend pullback into 50 EMA with bearish confirmation candle",
            metadata={
                "ema_fast": ema_fast_value,
                "ema_slow": ema_slow_value,
                "atr": atr_h4,
                "trend_strength_atr": round(trend_strength, 3),
                "ema_fast_slope_atr": round(ema_fast_slope_atr, 3),
                "usd_regime_bias_atr": round(usd_regime_bias, 3) if usd_regime_bias is not None else None,
            },
            exit_plan=_build_exit_plan(settings, "SHORT", trigger_price, risk, atr_h4, timeframe="H1"),
        )
    return None


def compute_usd_regime_bias(settings: Settings, usd_proxy_h4: dict[str, pd.DataFrame] | None) -> float | None:
    if not settings.usd_regime_filter_enabled or not usd_proxy_h4:
        return None

    weighted_bias = 0.0
    total_weight = 0.0
    for instrument, orientation in USD_PROXY_COMPONENTS:
        frame = usd_proxy_h4.get(instrument)
        if frame is None or len(frame) < settings.usd_regime_slow_ema + 5:
            continue
        atr = calc_atr(frame, settings.atr_period)
        if atr <= 0:
            continue
        ema_fast = calc_ema(frame["close"], settings.usd_regime_fast_ema)
        ema_slow = calc_ema(frame["close"], settings.usd_regime_slow_ema)
        component = orientation * ((float(ema_fast.iloc[-1]) - float(ema_slow.iloc[-1])) / atr)
        weighted_bias += max(-3.0, min(3.0, component)) * abs(orientation)
        total_weight += abs(orientation)

    if total_weight <= 0:
        return None
    result = weighted_bias / total_weight
    if not math.isfinite(result):
        return 0.0
    return result


def _build_exit_plan(
    settings: Settings,
    direction: str,
    entry_price: float,
    risk: float,
    atr: float,
    *,
    timeframe: str,
) -> dict[str, float | int | str]:
    if direction == "LONG":
        partial_profit_price = entry_price + risk * settings.partial_profit_rr
        break_even_trigger_price = entry_price + risk * settings.break_even_rr
    else:
        partial_profit_price = entry_price - risk * settings.partial_profit_rr
        break_even_trigger_price = entry_price - risk * settings.break_even_rr
    return {
        "partial_take_profit_fraction": 0.5,
        "partial_take_profit_price": round(partial_profit_price, 3),
        "break_even_trigger_price": round(break_even_trigger_price, 3),
        "trail_timeframe": timeframe,
        "trail_ema_period": settings.trailing_ema_period,
        "trail_atr_mult": settings.trailing_atr_mult,
        "trailing_stop_distance": round(max(risk, atr * settings.trailing_atr_mult), 3),
    }


def select_best_opportunity(opportunities: list[Opportunity]) -> Opportunity | None:
    if not opportunities:
        return None
    return max(opportunities, key=lambda opportunity: opportunity.score)