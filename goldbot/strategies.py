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
    is_inside_bar,
    is_pin_bar,
    nearest_support_resistance,
)
from goldbot.models import CalendarEvent, Opportunity
from goldbot.news_scoring import EventScore, select_best_for_breakout
from goldbot.volume_oracle import BreakoutVolumeSignal
from goldbot.impulse import body_atr_ratio, confirms_breakout


USD_PROXY_COMPONENTS: tuple[tuple[str, float], ...] = (
    ("EUR_USD", -1.0),
    ("GBP_USD", -0.75),
    ("USD_JPY", 1.0),
)


def _reject(reasons: list[str] | None, strategy: str, reason: str) -> None:
    if reasons is not None:
        reasons.append(f"{strategy}:{reason}")


def score_macro_breakout(
    settings: Settings,
    now: datetime,
    session_name: str,
    df_m15: pd.DataFrame,
    df_h1: pd.DataFrame,
    events: list[CalendarEvent],
    breakout_volume_signal: BreakoutVolumeSignal | None = None,
    reasons: list[str] | None = None,
    scored_events: list[EventScore] | None = None,
) -> Opportunity | None:
    strategy = "MACRO_BREAKOUT"
    if df_m15 is None or df_h1 is None or len(df_m15) < 40 or len(df_h1) < settings.breakout_box_hours + 8:
        _reject(reasons, strategy, "insufficient_candles")
        return None
    if settings.breakout_overlap_only and session_name != "OVERLAP":
        _reject(reasons, strategy, f"session_not_overlap({session_name})")
        return None
    recent_events = [
        event
        for event in events
        if now - timedelta(hours=settings.breakout_news_lookback_hours) <= event.occurs_at <= now - timedelta(minutes=settings.post_news_settle_minutes)
    ]
    session_open_mode = False
    latest_event = None
    box_hours_used = settings.breakout_box_hours
    if recent_events:
        latest_event = recent_events[-1]
        pre_event_end = latest_event.occurs_at
        pre_event_start = pre_event_end - timedelta(hours=settings.breakout_box_hours)
        box_slice = df_h1[(df_h1["time"] >= pre_event_start) & (df_h1["time"] < pre_event_end)]
    elif getattr(settings, "breakout_allow_session_open", False):
        # Session-open consolidation breakout: don't require a scheduled news
        # event. Only fire on the H1 candle that just closed at the London or
        # NY session open (the hours listed in breakout_session_open_hours_utc).
        try:
            allowed_hours = {
                int(h) for h in (settings.breakout_session_open_hours_utc or "").split(",") if h.strip()
            }
        except ValueError:
            allowed_hours = set()
        if not allowed_hours or now.hour not in allowed_hours:
            _reject(reasons, strategy, f"no_eligible_news_events_and_not_session_open(h={now.hour})")
            return None
        session_open_mode = True
        box_hours_sess = int(
            getattr(settings, "breakout_session_open_box_hours", settings.breakout_box_hours)
        )
        box_hours_used = box_hours_sess
        # Anchor the box to the MOST RECENT session-open hour that has already
        # occurred (the London or NY open preceding `now`), not to `now`
        # itself. This way, as the session progresses we keep testing whether
        # current price has broken out of the pre-session consolidation.
        session_open_anchor_hours = sorted({int(x) for x in (7, 12)})
        anchor_hour = max(
            (h for h in session_open_anchor_hours if h <= now.hour),
            default=session_open_anchor_hours[0],
        )
        box_end = now.replace(hour=anchor_hour, minute=0, second=0, microsecond=0)
        box_start = box_end - timedelta(hours=box_hours_sess)
        box_slice = df_h1[(df_h1["time"] >= box_start) & (df_h1["time"] < box_end)]
    else:
        _reject(reasons, strategy, "no_eligible_news_events")
        return None

    if len(box_slice) < max(4 if session_open_mode else 8, box_hours_used // 2):
        _reject(reasons, strategy, "box_slice_too_small")
        return None

    # For session-open mode the box slice is shorter than the ATR window
    # (14), which makes calc_atr inside consolidation_box return 0 and every
    # candidate gets rejected. Compute high/low from the narrower pre-session
    # slice but take ATR from the broader H1 context so the ratio is valid.
    if session_open_mode:
        session_atr = calc_atr(df_h1, settings.atr_period)
        if session_atr <= 0:
            _reject(reasons, strategy, "session_atr_zero")
            return None
        box_high = float(box_slice["high"].max())
        box_low = float(box_slice["low"].min())
        box_width = box_high - box_low
        box = {
            "high": box_high,
            "low": box_low,
            "width": box_width,
            "atr": session_atr,
            "width_atr_ratio": box_width / session_atr if session_atr > 0 else 0.0,
        }
    else:
        box = consolidation_box(box_slice, len(box_slice), settings.atr_period)
    box_width_limit = (
        float(getattr(settings, "breakout_session_open_min_box_atr_ratio", settings.breakout_min_box_atr_ratio))
        if session_open_mode
        else settings.breakout_min_box_atr_ratio
    )
    box_width_ratio = float(box["width_atr_ratio"])
    if box_width_ratio <= 0:
        _reject(reasons, strategy, f"box_width_atr_ratio={box_width_ratio:.2f}>{box_width_limit}")
        return None
    range_expansion_mode = False
    if box_width_ratio > box_width_limit:
        expansion_enabled = bool(getattr(settings, "breakout_range_expansion_enabled", False))
        expansion_limit = float(getattr(settings, "breakout_range_expansion_max_box_atr_ratio", box_width_limit))
        if not (session_open_mode and expansion_enabled and box_width_ratio <= expansion_limit):
            _reject(reasons, strategy, f"box_width_atr_ratio={box_width_ratio:.2f}>{box_width_limit}")
            return None
        range_expansion_mode = True

    atr = calc_atr(df_m15, settings.atr_period)
    buffer_size = atr * settings.breakout_buffer_atr
    last_close = float(df_m15["close"].iloc[-1])
    # Session-open breakouts often only print 1-2 M15 closes outside the box
    # before the initial thrust — demanding 3 consecutive closes rejected 61
    # otherwise-valid setups in the validation window.
    closes_window = 2 if session_open_mode else 3
    recent_closes = df_m15["close"].tail(closes_window)
    breakout_volume = float(df_m15["volume"].iloc[-1])
    avg_volume = float(df_m15["volume"].iloc[-21:-1].mean()) if len(df_m15) >= 21 else breakout_volume
    volume_ratio = breakout_volume / avg_volume if avg_volume > 0 else 1.0
    tick_confirmation = _confirm_breakout_volume(settings, volume_ratio, breakout_volume_signal)

    # 2.5: impulse confirmation (body/ATR) replaces OANDA tick-volume as the
    # primary sanity check. OANDA tick volume is the number of quote updates,
    # not traded contracts — it correlates with spread widening rather than
    # participation and is a fake feature. A body >= 0.40 * ATR identifies a
    # genuine directional impulse regardless of how many quotes printed.
    impulse_enabled = bool(getattr(settings, "breakout_impulse_confirm_enabled", False))
    require_both = bool(getattr(settings, "breakout_impulse_require_tick_volume", False))
    impulse_body_min = float(getattr(settings, "breakout_impulse_body_atr_min", 0.40))
    impulse_body_max = float(getattr(settings, "breakout_impulse_body_atr_max", 2.50))
    impulse_signal = body_atr_ratio(df_m15, calc_atr(df_m15, settings.atr_period)) if impulse_enabled else None

    if impulse_enabled:
        if impulse_signal is not None and impulse_signal.body_atr_ratio > impulse_body_max:
            _reject(reasons, strategy, f"impulse_overextended(body_atr={impulse_signal.body_atr_ratio:.2f}>{impulse_body_max:.2f})")
            return None
        # Direction of the breakout candle must match the direction we'll
        # ultimately trade — the caller checks last_close vs box later, but
        # we gate here on the candle direction too. If body/ATR + tick
        # volume disagree we fail the weaker of the two tests.
        impulse_up_ok = confirms_breakout(impulse_signal, required_direction="UP", body_atr_min=impulse_body_min) if impulse_signal else False
        impulse_down_ok = confirms_breakout(impulse_signal, required_direction="DOWN", body_atr_min=impulse_body_min) if impulse_signal else False
        impulse_ok_any = impulse_up_ok or impulse_down_ok
        if require_both:
            # AND semantics: need BOTH impulse and tick volume
            if tick_confirmation is None or not impulse_ok_any:
                _reject(reasons, strategy, f"impulse_or_volume_insufficient(body_atr={impulse_signal.body_atr_ratio:.2f},vol_ratio={volume_ratio:.2f})")
                return None
            volume_confirmation = dict(tick_confirmation)
        else:
            # OR semantics: impulse alone is enough; tick volume alone is too
            if not impulse_ok_any:
                _reject(reasons, strategy, f"impulse_insufficient(body_atr={impulse_signal.body_atr_ratio:.2f},vol_ratio={volume_ratio:.2f})")
                return None
            volume_confirmation = dict(tick_confirmation) if tick_confirmation is not None else {
                "tick_volume_ratio": round(volume_ratio, 2),
                "external_volume_ratio": None,
                "volume_confirmation": "impulse_only",
                "external_volume_source": None,
            }
        volume_confirmation["body_atr_ratio"] = round(impulse_signal.body_atr_ratio, 2)
        volume_confirmation["impulse_direction"] = impulse_signal.direction
    else:
        if tick_confirmation is None:
            _reject(reasons, strategy, f"volume_ratio={volume_ratio:.2f}<{settings.breakout_min_volume_ratio}")
            return None
        volume_confirmation = dict(tick_confirmation)

    if range_expansion_mode:
        if not impulse_enabled or impulse_signal is None:
            _reject(reasons, "RANGE_EXPANSION_CONTINUATION", "impulse_required")
            return None
        expansion_body_min = float(getattr(settings, "breakout_range_expansion_body_atr_min", 0.80))
        if impulse_signal.body_atr_ratio < expansion_body_min:
            _reject(
                reasons,
                "RANGE_EXPANSION_CONTINUATION",
                f"body_atr={impulse_signal.body_atr_ratio:.2f}<{expansion_body_min:.2f}",
            )
            return None
        volume_confirmation["strategy_variant"] = "RANGE_EXPANSION_CONTINUATION"
        volume_confirmation["wide_box_atr_ratio"] = round(box_width_ratio, 2)

    event_label = latest_event.title if latest_event is not None else ("SESSION_OPEN" if session_open_mode else "")
    rationale_prefix = (
        f"Session-open break above {box['high']:.2f}"
        if session_open_mode
        else f"Post-news break above {box['high']:.2f} after {latest_event.title}"
    )

    # Session-open breakouts against the prevailing H4 trend are a common
    # fade; validation showed a counter-trend short losing $26 while aligned
    # longs won. Gate directionality on H4 EMA trend when there's no scheduled
    # news (news-driven moves can legitimately fade the prior trend).
    trend_direction_hint: str | None = None
    if session_open_mode and len(df_h1) >= settings.trend_ema_fast + 5:
        _ema_fast_h1 = calc_ema(df_h1["close"], settings.trend_ema_fast).iloc[-1]
        _ema_slow_h1 = calc_ema(df_h1["close"], max(settings.trend_ema_fast * 2, 100)).iloc[-1]
        if float(_ema_fast_h1) > float(_ema_slow_h1):
            trend_direction_hint = "LONG"
        elif float(_ema_fast_h1) < float(_ema_slow_h1):
            trend_direction_hint = "SHORT"

    if last_close > box["high"] + buffer_size and float(recent_closes.min()) > box["high"]:
        if session_open_mode and trend_direction_hint == "SHORT":
            _reject(reasons, strategy, "long_break_against_h1_trend")
            return None
        if impulse_enabled:
            required_body_min = float(getattr(settings, "breakout_range_expansion_body_atr_min", 0.80)) if range_expansion_mode else impulse_body_min
            if not confirms_breakout(impulse_signal, required_direction="UP", body_atr_min=required_body_min):
                _reject(reasons, "RANGE_EXPANSION_CONTINUATION" if range_expansion_mode else strategy, "long_impulse_missing")
                return None
        news_score_meta = _apply_news_surprise_gate(
            settings, "LONG", scored_events, session_open_mode, reasons
        )
        if news_score_meta is None:
            return None
        if range_expansion_mode:
            stop_price = last_close - max(
                atr * float(getattr(settings, "breakout_range_expansion_stop_atr", 1.35)),
                buffer_size,
            )
            candidate_strategy = "RANGE_EXPANSION_CONTINUATION"
            candidate_rationale = f"Range-expansion continuation above {box['high']:.2f}"
        else:
            stop_price = box["low"] - buffer_size
            candidate_strategy = "MACRO_BREAKOUT"
            candidate_rationale = rationale_prefix
        risk = last_close - stop_price
        score = 70 + min(20, ((last_close - box["high"]) / max(atr, 1e-9)) * 10)
        if session_open_mode:
            score -= 5  # modestly discount non-news breakouts
        if range_expansion_mode:
            score -= 3
        return Opportunity(
            strategy=candidate_strategy,
            direction="LONG",
            score=score,
            entry_price=last_close,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale=candidate_rationale,
            metadata={
                "event": event_label,
                "box_high": box["high"],
                "box_low": box["low"],
                "box_width_atr_ratio": round(box_width_ratio, 2),
                "atr": atr,
                "volume_ratio": round(volume_ratio, 2),
                **volume_confirmation,
                **news_score_meta,
            },
            exit_plan=_build_exit_plan(settings, "LONG", last_close, risk, atr, timeframe="M15"),
        )
    if last_close < box["low"] - buffer_size and float(recent_closes.max()) < box["low"]:
        if session_open_mode and trend_direction_hint == "LONG":
            _reject(reasons, strategy, "short_break_against_h1_trend")
            return None
        if impulse_enabled:
            required_body_min = float(getattr(settings, "breakout_range_expansion_body_atr_min", 0.80)) if range_expansion_mode else impulse_body_min
            if not confirms_breakout(impulse_signal, required_direction="DOWN", body_atr_min=required_body_min):
                _reject(reasons, "RANGE_EXPANSION_CONTINUATION" if range_expansion_mode else strategy, "short_impulse_missing")
                return None
        news_score_meta = _apply_news_surprise_gate(
            settings, "SHORT", scored_events, session_open_mode, reasons
        )
        if news_score_meta is None:
            return None
        if range_expansion_mode:
            stop_price = last_close + max(
                atr * float(getattr(settings, "breakout_range_expansion_stop_atr", 1.35)),
                buffer_size,
            )
            candidate_strategy = "RANGE_EXPANSION_CONTINUATION"
            rationale_short = f"Range-expansion continuation below {box['low']:.2f}"
        else:
            stop_price = box["high"] + buffer_size
            candidate_strategy = "MACRO_BREAKOUT"
            rationale_short = (
                f"Session-open break below {box['low']:.2f}"
                if session_open_mode
                else f"Post-news break below {box['low']:.2f} after {latest_event.title}"
            )
        risk = stop_price - last_close
        score = 70 + min(20, ((box["low"] - last_close) / max(atr, 1e-9)) * 10)
        if session_open_mode:
            score -= 5
        if range_expansion_mode:
            score -= 3
        return Opportunity(
            strategy=candidate_strategy,
            direction="SHORT",
            score=score,
            entry_price=last_close,
            stop_price=stop_price,
            take_profit_price=None,
            risk_per_unit=risk,
            rationale=rationale_short,
            metadata={
                "event": event_label,
                "box_high": box["high"],
                "box_low": box["low"],
                "box_width_atr_ratio": round(box_width_ratio, 2),
                "atr": atr,
                "volume_ratio": round(volume_ratio, 2),
                **volume_confirmation,
                **news_score_meta,
            },
            exit_plan=_build_exit_plan(settings, "SHORT", last_close, risk, atr, timeframe="M15"),
        )
    _reject(reasons, "MACRO_BREAKOUT", "no_breakout_direction")
    return None


def score_event_catalyst_breakout(
    settings: Settings,
    now: datetime,
    session_name: str,
    df_m15: pd.DataFrame,
    df_h1: pd.DataFrame,
    *,
    direction: str,
    catalyst_metadata: dict | None = None,
    breakout_volume_signal: BreakoutVolumeSignal | None = None,
    reasons: list[str] | None = None,
    scored_events: list[EventScore] | None = None,
) -> Opportunity | None:
    event_time = now - timedelta(minutes=max(1, int(settings.post_news_settle_minutes)))
    event = CalendarEvent("Gold event catalyst", "USD", "high", event_time, "gold_event_state")
    candidate = score_macro_breakout(
        settings,
        now,
        session_name,
        df_m15,
        df_h1,
        [event],
        breakout_volume_signal,
        reasons=reasons,
        scored_events=scored_events,
    )
    if candidate is None:
        _reject(reasons, "EVENT_CATALYST", "price_confirmation_missing")
        return None
    wanted_direction = direction.upper()
    if candidate.direction != wanted_direction:
        _reject(reasons, "EVENT_CATALYST", f"breakout_direction_{candidate.direction}_not_{wanted_direction}")
        return None
    metadata = dict(candidate.metadata or {})
    metadata.update(catalyst_metadata or {})
    metadata["gold_event_catalyst"] = True
    metadata["strategy_variant"] = "EVENT_CATALYST_BREAKOUT"
    candidate.metadata = metadata
    candidate.rationale = f"Event-catalyst {candidate.rationale}"
    return candidate


def _apply_news_surprise_gate(
    settings: Settings,
    direction: str,
    scored_events: list[EventScore] | None,
    session_open_mode: bool,
    reasons: list[str] | None,
) -> dict | None:
    """Return metadata dict on pass, or None on veto.

    The gate only applies when `news_surprise_filter_enabled = True` AND we
    have a scored event for the relevant direction. Session-open breakouts
    (no scheduled news) are exempt — they're a different strategy archetype.
    """
    if not getattr(settings, "news_surprise_filter_enabled", False):
        return {}
    if session_open_mode:
        return {}
    scored_events = scored_events or []
    if not scored_events:
        # Filter enabled but no scored data available -> fail-safe: reject.
        # Operator can either turn the filter off or ensure macro_engine
        # populates `event_scores` in the macro state file.
        _reject(reasons, "MACRO_BREAKOUT", "news_surprise_filter_enabled_but_no_scored_events")
        return None
    best = select_best_for_breakout(scored_events, direction=direction)
    if best is None:
        _reject(reasons, "MACRO_BREAKOUT", f"no_scored_event_supports_{direction}")
        return None
    min_composite = float(getattr(settings, "news_surprise_min_composite", 0.60))
    require_match = bool(getattr(settings, "news_surprise_require_direction_match", True))
    if best.composite is None:
        _reject(reasons, "MACRO_BREAKOUT", "scored_event_composite_none")
        return None
    if best.composite < min_composite:
        _reject(reasons, "MACRO_BREAKOUT", f"scored_event_composite={best.composite:.2f}<{min_composite:.2f}")
        return None
    if require_match:
        # Direction must match the sign of the surprise (i.e. dovish surprise
        # -> LONG gold; hawkish -> SHORT gold). select_best_for_breakout
        # already filters by usd_direction, so we just confirm here.
        if best.usd_direction not in {"UP", "DOWN"} and best.surprise_z is None:
            _reject(reasons, "MACRO_BREAKOUT", "scored_event_direction_unknown")
            return None
    return {
        "news_score_event_key": best.event_key,
        "news_score_composite": round(best.composite, 3),
        "news_score_surprise_z": round(best.surprise_z, 2) if best.surprise_z is not None else None,
        "news_score_rates_bps": round(best.rates_move_bps, 2) if best.rates_move_bps is not None else None,
        "news_score_dxy_pct": round(best.dxy_move_pct, 3) if best.dxy_move_pct is not None else None,
    }


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


def score_exhaustion_reversal(settings: Settings, df_h4: pd.DataFrame, df_d1: pd.DataFrame, reasons: list[str] | None = None) -> Opportunity | None:
    strategy = "EXHAUSTION_REVERSAL"
    if df_h4 is None or df_d1 is None or len(df_h4) < 80 or len(df_d1) < 40:
        _reject(reasons, strategy, "insufficient_candles")
        return None
    price = float(df_h4["close"].iloc[-1])
    atr = calc_atr(df_h4, settings.atr_period)
    rsi_h4 = calc_rsi(df_h4["close"])
    macd_h4 = calc_macd(df_h4)
    levels = nearest_support_resistance(df_d1, settings.exhaustion_sr_lookback)
    divergence = detect_divergence(df_h4, lookback=50)

    near_resistance = abs(price - levels["resistance"]) <= atr * getattr(settings, "exhaustion_near_sr_atr_mult", 1.0)
    near_support = abs(price - levels["support"]) <= atr * getattr(settings, "exhaustion_near_sr_atr_mult", 1.0)

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
    _reject(
        reasons,
        "EXHAUSTION_REVERSAL",
        f"no_setup(rsi={rsi_h4:.1f},div_bull={divergence['bullish']},div_bear={divergence['bearish']},near_sup={near_support},near_res={near_resistance})",
    )
    return None


def score_trend_pullback(
    settings: Settings,
    df_h1: pd.DataFrame,
    df_h4: pd.DataFrame,
    usd_proxy_h4: dict[str, pd.DataFrame] | None = None,
    reasons: list[str] | None = None,
) -> Opportunity | None:
    strategy = "TREND_PULLBACK"
    if df_h1 is None or df_h4 is None or len(df_h1) < 80 or len(df_h4) < settings.trend_ema_slow + 5:
        _reject(reasons, strategy, "insufficient_candles")
        return None
    close_h4 = df_h4["close"]
    ema_fast = calc_ema(close_h4, settings.trend_ema_fast)
    ema_slow = calc_ema(close_h4, settings.trend_ema_slow)
    h1_confirm_ema = calc_ema(df_h1["close"], settings.trend_h1_confirm_ema_period)
    trigger_price = float(df_h1["close"].iloc[-1])
    atr_h4 = calc_atr(df_h4, settings.atr_period)
    if atr_h4 <= 0:
        _reject(reasons, strategy, "zero_atr")
        return None

    bullish_trend = float(ema_fast.iloc[-1]) > float(ema_slow.iloc[-1])
    bearish_trend = float(ema_fast.iloc[-1]) < float(ema_slow.iloc[-1])
    if not bullish_trend and not bearish_trend:
        _reject(reasons, strategy, "no_trend_direction")
        return None

    ema_fast_value = float(ema_fast.iloc[-1])
    ema_slow_value = float(ema_slow.iloc[-1])
    trend_strength = abs(ema_fast_value - ema_slow_value) / atr_h4
    if trend_strength < settings.trend_min_strength_atr:
        _reject(reasons, strategy, f"trend_strength={trend_strength:.2f}<{settings.trend_min_strength_atr}")
        return None
    slope_bars = min(settings.trend_fast_slope_bars, len(ema_fast) - 1)
    if slope_bars <= 0:
        _reject(reasons, strategy, "slope_bars_zero")
        return None
    ema_fast_slope_atr = (ema_fast_value - float(ema_fast.iloc[-(slope_bars + 1)])) / atr_h4
    h1_confirm_value = float(h1_confirm_ema.iloc[-1])
    usd_regime_bias = compute_usd_regime_bias(settings, usd_proxy_h4)

    # EMA reclaim check (P5): a candle that dipped into the H1 EMA and then
    # closed back above it by a modest ATR-buffer, with RSI filter, is a
    # standard gold pullback trigger used by prop desks. Symmetric for short.
    h1_ema_fast = calc_ema(df_h1["close"], settings.trend_h1_confirm_ema_period)
    last_bar = df_h1.iloc[-1]
    prev_bar = df_h1.iloc[-2] if len(df_h1) >= 2 else last_bar
    h1_rsi = calc_rsi(df_h1["close"])
    atr_h1 = calc_atr(df_h1, settings.atr_period)
    h1_ema_last = float(h1_ema_fast.iloc[-1])
    h1_ema_prev = float(h1_ema_fast.iloc[-2]) if len(h1_ema_fast) >= 2 else h1_ema_last

    ema_reclaim_bull = False
    ema_reclaim_bear = False
    if getattr(settings, "trend_allow_ema_reclaim", False) and atr_h1 > 0:
        break_atr = float(getattr(settings, "trend_ema_reclaim_break_atr", 0.10))
        touch_atr = float(getattr(settings, "trend_ema_reclaim_touch_atr", 0.50))
        rsi_min = float(getattr(settings, "trend_ema_reclaim_rsi_min", 45.0))
        # bull reclaim: current H1 low (or prior close) touched/pierced EMA within
        # touch_atr, and current close finished above EMA by >= break_atr * ATR.
        touched_from_above = (
            float(last_bar["low"]) <= h1_ema_last + atr_h1 * 0.05
            or float(prev_bar["close"]) <= h1_ema_prev + atr_h1 * touch_atr
        )
        closed_above = float(last_bar["close"]) >= h1_ema_last + atr_h1 * break_atr
        if touched_from_above and closed_above and h1_rsi >= rsi_min:
            ema_reclaim_bull = True
        touched_from_below = (
            float(last_bar["high"]) >= h1_ema_last - atr_h1 * 0.05
            or float(prev_bar["close"]) >= h1_ema_prev - atr_h1 * touch_atr
        )
        closed_below = float(last_bar["close"]) <= h1_ema_last - atr_h1 * break_atr
        if touched_from_below and closed_below and h1_rsi <= (100.0 - rsi_min):
            ema_reclaim_bear = True

    bull_confirm = is_bullish_engulfing(df_h1) or is_pin_bar(df_h1, "LONG") or (
        settings.trend_allow_inside_bar_confirmation
        and is_inside_bar(df_h1)
        and trigger_price > float(df_h1["close"].iloc[-2])
    ) or ema_reclaim_bull
    bear_confirm = is_bearish_engulfing(df_h1) or is_pin_bar(df_h1, "SHORT") or (
        settings.trend_allow_inside_bar_confirmation
        and is_inside_bar(df_h1)
        and trigger_price < float(df_h1["close"].iloc[-2])
    ) or ema_reclaim_bear

    if bullish_trend and bull_confirm:
        if ema_fast_slope_atr < settings.trend_min_slope_atr:
            _reject(reasons, strategy, f"ema_fast_slope_atr={ema_fast_slope_atr:.3f}<{settings.trend_min_slope_atr}")
            return None
        if trigger_price < h1_confirm_value:
            _reject(reasons, strategy, "h1_close_below_ema")
            return None
        usd_risk_mult = 1.0
        if settings.usd_regime_filter_enabled and usd_regime_bias is not None:
            if usd_regime_bias >= settings.usd_regime_hard_veto_atr:
                _reject(reasons, strategy, f"usd_regime_bias={usd_regime_bias:.2f}>=hard_veto")
                return None
            if usd_regime_bias >= settings.usd_regime_min_bias_atr:
                usd_risk_mult = settings.usd_regime_adverse_risk_multiplier
        support_probe = float(df_h1["low"].tail(3).min())
        pullback_gap = abs(support_probe - ema_fast_value)
        deep_pullback = False
        if pullback_gap > atr_h4 * settings.trend_pullback_atr_tolerance:
            deep_enabled = bool(getattr(settings, "trend_deep_pullback_enabled", False))
            deep_tolerance = float(getattr(settings, "trend_deep_pullback_atr_tolerance", settings.trend_pullback_atr_tolerance))
            deep_strength_min = float(getattr(settings, "trend_deep_pullback_min_strength_atr", settings.trend_min_strength_atr))
            if not (deep_enabled and ema_reclaim_bull and pullback_gap <= atr_h4 * deep_tolerance and trend_strength >= deep_strength_min):
                _reject(reasons, strategy, f"pullback_gap={pullback_gap/atr_h4:.2f}ATR>{settings.trend_pullback_atr_tolerance}")
                return None
            deep_pullback = True
        stop_cushion = 0.8 if deep_pullback else 0.6
        stop_price = min(float(df_h1["low"].tail(5).min()), float(ema_fast.iloc[-1])) - atr_h4 * stop_cushion
        risk = trigger_price - stop_price
        score = 68 + min(18, trend_strength * 6)
        if deep_pullback:
            score -= 4
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
                "risk_multiplier": usd_risk_mult,
                "strategy_variant": "DEEP_PULLBACK_RECLAIM" if deep_pullback else "STANDARD_PULLBACK",
                "pullback_gap_atr": round(pullback_gap / atr_h4, 3),
            },
            exit_plan=_build_exit_plan(settings, "LONG", trigger_price, risk, atr_h4, timeframe="H1"),
        )

    if bearish_trend and bear_confirm:
        if ema_fast_slope_atr > -settings.trend_min_slope_atr:
            _reject(reasons, strategy, f"ema_fast_slope_atr={ema_fast_slope_atr:.3f}>-{settings.trend_min_slope_atr}")
            return None
        if trigger_price > h1_confirm_value:
            _reject(reasons, strategy, "h1_close_above_ema")
            return None
        usd_risk_mult = 1.0
        if settings.usd_regime_filter_enabled and usd_regime_bias is not None:
            if usd_regime_bias <= -settings.usd_regime_hard_veto_atr:
                _reject(reasons, strategy, f"usd_regime_bias={usd_regime_bias:.2f}<=-hard_veto")
                return None
            if usd_regime_bias <= -settings.usd_regime_min_bias_atr:
                usd_risk_mult = settings.usd_regime_adverse_risk_multiplier
        resistance_probe = float(df_h1["high"].tail(3).max())
        pullback_gap = abs(resistance_probe - ema_fast_value)
        deep_pullback = False
        if pullback_gap > atr_h4 * settings.trend_pullback_atr_tolerance:
            deep_enabled = bool(getattr(settings, "trend_deep_pullback_enabled", False))
            deep_tolerance = float(getattr(settings, "trend_deep_pullback_atr_tolerance", settings.trend_pullback_atr_tolerance))
            deep_strength_min = float(getattr(settings, "trend_deep_pullback_min_strength_atr", settings.trend_min_strength_atr))
            if not (deep_enabled and ema_reclaim_bear and pullback_gap <= atr_h4 * deep_tolerance and trend_strength >= deep_strength_min):
                _reject(reasons, strategy, f"pullback_gap={pullback_gap/atr_h4:.2f}ATR>{settings.trend_pullback_atr_tolerance}")
                return None
            deep_pullback = True
        stop_cushion = 0.8 if deep_pullback else 0.6
        stop_price = max(float(df_h1["high"].tail(5).max()), float(ema_fast.iloc[-1])) + atr_h4 * stop_cushion
        risk = stop_price - trigger_price
        score = 68 + min(18, trend_strength * 6)
        if deep_pullback:
            score -= 4
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
                "risk_multiplier": usd_risk_mult,
                "strategy_variant": "DEEP_PULLBACK_RECLAIM" if deep_pullback else "STANDARD_PULLBACK",
                "pullback_gap_atr": round(pullback_gap / atr_h4, 3),
            },
            exit_plan=_build_exit_plan(settings, "SHORT", trigger_price, risk, atr_h4, timeframe="H1"),
        )
    # Tell the next log reader which side we were set up for but couldn't
    # confirm -- it makes "why didn't the bot fire?" triage symmetric between
    # longs and shorts, which was the explicit point of this review.
    if bullish_trend:
        _reject(reasons, strategy, "no_bull_confirmation_candle")
    elif bearish_trend:
        _reject(reasons, strategy, "no_bear_confirmation_candle")
    else:
        _reject(reasons, strategy, "no_confirmation_candle")
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