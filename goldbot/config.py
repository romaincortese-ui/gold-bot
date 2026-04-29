import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:
    find_dotenv = None  # type: ignore[assignment]
    load_dotenv = None  # type: ignore[assignment]


if load_dotenv is not None and find_dotenv is not None:
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)


def env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean for {name}: {value}")


def env_csv(name: str, default: str) -> list[str]:
    raw = env_str(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    instrument: str
    oanda_api_key: str
    oanda_account_id: str
    oanda_environment: str
    account_type: str
    execution_mode: str
    paper_balance: float
    gold_budget_allocation: float
    fx_budget_allocation: float
    max_risk_per_trade: float
    max_total_gold_risk: float
    max_open_gold_trades: int
    leverage: float
    poll_interval_seconds: int
    london_open_utc: int
    london_close_utc: int
    ny_open_utc: int
    ny_close_utc: int
    overlap_start_utc: int
    overlap_end_utc: int
    breakout_news_lookback_hours: int
    breakout_news_lookahead_hours: int
    pre_news_pause_minutes: int
    post_news_settle_minutes: int
    max_entry_spread: float
    breakout_box_hours: int
    breakout_buffer_atr: float
    breakout_min_box_atr_ratio: float
    breakout_min_volume_ratio: float
    breakout_overlap_only: bool
    exhaustion_rr: float
    exhaustion_rsi_overbought: float
    exhaustion_rsi_oversold: float
    exhaustion_sr_lookback: int
    trend_ema_fast: int
    trend_ema_slow: int
    trend_h1_confirm_ema_period: int
    trend_min_strength_atr: float
    trend_fast_slope_bars: int
    trend_min_slope_atr: float
    trend_pullback_atr_tolerance: float
    trend_stopout_cooldown_hours: int
    usd_regime_filter_enabled: bool
    usd_regime_fast_ema: int
    usd_regime_slow_ema: int
    usd_regime_min_bias_atr: float
    partial_profit_rr: float
    break_even_rr: float
    trailing_atr_mult: float
    trailing_ema_period: int
    atr_period: int
    state_file: str
    shared_budget_file: str
    macro_state_file: str
    news_cache_file: str
    news_urls: list[str]
    breakout_volume_mode: str = "tick"
    breakout_external_volume_file: str = ""
    breakout_external_volume_max_age_minutes: int = 30
    breakout_external_min_volume_ratio: float = 1.05
    macro_breakout_spread_settle_seconds: int = 45
    macro_breakout_spread_stability_checks: int = 3
    macro_breakout_spread_stability_tolerance: float = 0.15
    real_yield_filter_enabled: bool = False
    real_yield_state_max_age_hours: int = 24
    real_yield_lookback_days: int = 5
    real_yield_reduce_risk_bps: float = 7.5
    real_yield_veto_bps: float = 15.0
    real_yield_adverse_risk_multiplier: float = 0.5
    # --- P3: USD regime soft/hard veto ---
    usd_regime_hard_veto_atr: float = 0.70
    usd_regime_adverse_risk_multiplier: float = 0.5
    # --- P4: limited ASIA scanning ---
    scan_asia_active: bool = True
    asia_active_start_utc: int = 1
    asia_active_end_utc: int = 6
    # --- P1: accept H1 inside-bar as a pullback confirmation candle ---
    trend_allow_inside_bar_confirmation: bool = True
    # --- P5 (post-review Apr 2026): gold pros enter pullbacks on a simple
    # "close back above 50 EMA with RSI > 40" pattern. The textbook engulfing/
    # pin-bar filter is too strict (189 misses in 15-day window). Allow EMA
    # reclaim as an alternative confirmation signal.
    trend_allow_ema_reclaim: bool = True
    trend_ema_reclaim_rsi_min: float = 45.0
    trend_ema_reclaim_break_atr: float = 0.10
    trend_ema_reclaim_touch_atr: float = 0.50
    # --- P5: MACRO_BREAKOUT was blocking 92% of eligible hours because it
    # requires a scheduled news event within the lookback. Pros trade
    # consolidation breakouts at session opens without news as well. Allow a
    # "session open" breakout mode that builds the box from the last N hours
    # ending at the London/NY/overlap open.
    breakout_allow_session_open: bool = True
    breakout_session_open_hours_utc: str = "7,8,9,12,13,14,15"
    breakout_session_open_box_hours: int = 8
    breakout_session_open_min_box_atr_ratio: float = 2.75
    # --- P5: widen the near-support/near-resistance tolerance for exhaustion
    # reversals (default was exactly 1*ATR which never triggered in the
    # 15-day window). Pros treat 1.2-1.5 ATR as "near" in high-vol gold.
    exhaustion_near_sr_atr_mult: float = 1.3

    # --- Sprint 1 (Apr 2026): institutional-grade upgrades ---
    # 2.2 Adaptive spread cap. Instead of rejecting every fill at a static
    # pip-count, take median spread over a rolling window and reject fills
    # that exceed `max(floor, median * multiplier)`. Static cap acts as an
    # absolute upper bound to catch pathological feed errors.
    adaptive_spread_enabled: bool = True
    adaptive_spread_window_minutes: int = 30
    adaptive_spread_multiplier: float = 1.8
    adaptive_spread_floor: float = 0.25  # below this, use floor (thin markets)
    adaptive_spread_min_samples: int = 6  # warm-up: fall back to static cap until we have this many samples

    # 2.3 Real-yields absolute-level sign gate. On top of the change-bps
    # scaling/veto, refuse LONG gold when 10Y real yield is above a hostile
    # level AND rising, refuse SHORT gold when yield is below a friendly
    # level AND falling. Drives most weekly gold variance historically.
    real_yield_level_gate_enabled: bool = True
    real_yield_long_veto_level_pct: float = 1.80   # 10Y TIPS %. Rising + above => no longs.
    real_yield_short_veto_level_pct: float = 1.00  # 10Y TIPS %. Falling + below => no shorts.
    real_yield_slope_lookback_days: int = 5
    real_yield_slope_threshold_bps: float = 0.0    # bps over lookback. sign matters only.

    # 2.4 Volatility-target sizing. Size so that 1 ATR at the stop distance
    # equals `vol_target_nav_bps` basis points of account NAV. Always clamped
    # to the pre-existing `max_risk_per_trade` cap so this never *increases*
    # risk above the manual ceiling — it only brings down size on high-vol
    # days when a static %-of-equity would be over-sized.
    vol_target_sizing_enabled: bool = True
    vol_target_nav_bps: float = 25.0  # 25 bp of NAV per 1 ATR of stop distance

    # 2.5 Impulse confirmation. OANDA tick volume is the number of quotes,
    # not traded volume, and correlates with spread widening rather than
    # participation. Replace/augment with a body-to-ATR ratio on the breakout
    # candle which is a cleaner proxy for a genuine directional impulse.
    breakout_impulse_confirm_enabled: bool = True
    breakout_impulse_body_atr_min: float = 0.40
    breakout_impulse_require_tick_volume: bool = False  # if True, require BOTH; else EITHER passes

    # 2.1 Event scoring. Keyword matches are binary; real desks score each
    # high-impact release on surprise magnitude and cross-asset confirmation.
    # When scoring data is available via the macro state file, only take
    # MACRO_BREAKOUT when the composite score clears a threshold. Falls back
    # to the legacy keyword gate (no-op) when scoring data is missing.
    news_surprise_filter_enabled: bool = False
    news_surprise_min_composite: float = 0.60  # 0..1 composite score
    news_surprise_require_direction_match: bool = True  # breakout side must match sign of surprise
    news_score_state_max_age_minutes: int = 120

    # 2.8 Portfolio drawdown kill switch. Rolling-NAV guard: scales risk down
    # on a -6% 30-day drawdown, halts new entries on a -10% 90-day drawdown
    # and leaves the halt in place until an operator clears it manually. No
    # auto-resume — boilerplate at any real desk.
    drawdown_kill_switch_enabled: bool = True
    drawdown_soft_window_days: int = 30
    drawdown_soft_threshold_pct: float = -0.06
    drawdown_soft_risk_per_trade: float = 0.003  # 0.30% of sleeve when soft cut active
    drawdown_hard_window_days: int = 90
    drawdown_hard_threshold_pct: float = -0.10
    drawdown_equity_history_max_days: int = 180  # trim history after this

    # --- Sprint 2 (Apr 2026): weekend / calibration / CFTC hardening ---
    # 2.6 Weekend gap handling. Gold has frequent Sunday-open gaps of
    # 0.3-1.2%. Flatten all open trades before Friday close and widen stops
    # through the last trading hour so a Friday stop-run cannot trigger a
    # Monday-gap fill at a far worse price.
    weekend_gap_handling_enabled: bool = True
    weekend_flatten_hour_utc: int = 20           # Friday hour-of-day at which to flatten
    weekend_flatten_weekday: int = 4             # 0=Mon, 4=Fri
    weekend_stop_widen_enabled: bool = True
    weekend_stop_widen_hour_utc: int = 19        # Friday hour to start widening
    weekend_stop_widen_atr_mult: float = 2.0     # extra ATR multiples added to the stop
    weekend_block_new_entries_hour_utc: int = 19 # Friday hour to stop opening new trades

    # 2.7 Calibration hardening. The original 2-trade activation is way
    # below statistical significance. Raise the minimum sample and apply a
    # James-Stein-style shrinkage so adjustments asymptote to neutral until
    # we have a meaningful sample. Hard-block also needs more evidence.
    calibration_min_trades_for_adjustment: int = 40
    calibration_min_trades_for_block: int = 80
    calibration_shrinkage_denominator: int = 200  # shrink = min(1, n/denom)
    calibration_block_pf_threshold: float = 0.7
    calibration_block_expectancy_threshold: float = -5.0
    calibration_block_win_rate_threshold: float = 0.35

    # 3.4 Realistic backtest microstructure. The default simulated_spread=0.25
    # is a flat pip figure; in reality gold spreads double through Tokyo and
    # 6x through the NFP minute. The weekend gap is not simulated. Overnight
    # financing on longs is ~5% APR, on shorts ~0%. These feed into the
    # backtest engine only; live execution is unaffected.
    backtest_spread_model_enabled: bool = True
    backtest_spread_profile: str = "gold_m15"    # name of the hour-of-day profile
    backtest_spread_news_window_minutes: int = 2
    backtest_spread_news_multiplier: float = 6.0
    backtest_exit_slippage_multiplier: float = 1.5  # adverse selection on stops
    backtest_weekend_gap_enabled: bool = True
    backtest_weekend_flatten_hour_utc: int = 20
    backtest_financing_enabled: bool = True
    backtest_financing_long_apr: float = 0.05    # 5% APR cost on long XAU
    backtest_financing_short_apr: float = 0.0    # flat on shorts

    # 3.5 CFTC positioning filter. Weekly Commitment of Traders report for
    # gold futures (CME GC). Managed-money extreme net long -> fade further
    # long entries; extreme net short -> fade further short entries. Only
    # applies a score offset (~+/- 8 points), never a hard block.
    cftc_filter_enabled: bool = False
    cftc_state_max_age_days: int = 10            # stale if > 10d (report is weekly)
    cftc_extreme_percentile: float = 0.85        # 85th pctile triggers fade
    cftc_extreme_score_offset: float = 8.0       # +/- score offset applied to crowded side

    # --- Sprint 3 (Apr 2026): cross-asset co-trade / regime / options IV /
    # walk-forward / execution tightening. All disabled-by-default until
    # their upstream data pipelines are live in macro_engine.
    co_trade_gates_enabled: bool = False
    co_trade_state_max_age_hours: int = 24
    co_trade_es_risk_on_long_veto_pct: float = 0.015     # ES +1.5% day → no long-gold
    co_trade_cnh_stress_short_veto_pct: float = 0.004    # USD/CNH +0.4% → no short-gold
    co_trade_dxy_weak_favourable_pct: float = -0.003     # DXY -0.3% → size up long
    co_trade_favourable_size_mult: float = 1.25

    regime_filter_enabled: bool = False
    regime_quiet_atr_pct_max: float = 0.006              # ATR% ≤ 0.6% → quiet_carry
    regime_trend_atr_pct_max: float = 0.014              # 0.6% < ATR% ≤ 1.4% → trend
    regime_spike_atr_pct_min: float = 0.016              # ATR% ≥ 1.6% → spike
    regime_quiet_strategies: str = "EXHAUSTION_REVERSAL"
    regime_trend_strategies: str = "TREND_PULLBACK,MACRO_BREAKOUT"
    regime_spike_strategies: str = "MACRO_BREAKOUT"

    options_iv_gate_enabled: bool = False
    options_iv_state_max_age_hours: int = 24
    options_iv_realised_fraction_threshold: float = 0.60  # 1h realised / 1d implied

    walk_forward_enabled: bool = False
    walk_forward_in_sample_days: int = 275
    walk_forward_out_sample_days: int = 90
    walk_forward_step_days: int = 90
    walk_forward_min_out_sample_pf: float = 1.15
    walk_forward_max_pf_degradation: float = 0.50

    execution_use_limit_entry: bool = False
    execution_limit_spread_multiplier: float = 1.25
    execution_limit_timeout_seconds: int = 3
    execution_guaranteed_stop_enabled: bool = False

    # --- Q2 (Apr 2026): Tier-3 strategic items — miners overlay, 3-factor
    # sizing model, central-bank demand tracker, cross-asset risk parity.
    # All default-off until their macro-engine data pipelines are live.
    miners_overlay_enabled: bool = False
    miners_state_max_age_hours: int = 24
    miners_confirm_threshold_pct: float = 0.005    # 0.5% miners-vs-gold divergence
    miners_etf_flow_threshold_pct: float = 0.002   # 0.2% GLD shares-outstanding change
    miners_score_offset: float = 6.0
    miners_long_confirm_size_mult: float = 1.2

    factor_model_enabled: bool = False
    factor_model_state_max_age_hours: int = 168    # weekly data — 7d window
    factor_tips_weight: float = 0.40
    factor_dxy_weight: float = 0.35
    factor_gld_weight: float = 0.25
    factor_align_threshold: float = 0.40
    factor_align_size_mult: float = 1.5
    factor_oppose_size_mult: float = 0.5
    factor_score_offset: float = 5.0

    central_bank_flow_enabled: bool = False
    central_bank_state_max_age_days: int = 100     # quarterly report
    central_bank_high_demand_tonnes: float = 300.0
    central_bank_short_veto_strategies: str = "EXHAUSTION_REVERSAL"

    risk_parity_enabled: bool = False
    risk_parity_rebalance_interval_days: int = 7
    risk_parity_lookback_days: int = 20
    risk_parity_min_weight: float = 0.20
    risk_parity_max_weight: float = 0.80
    risk_parity_rebalance_threshold: float = 0.05


def load_settings() -> Settings:
    settings = Settings(
        instrument=env_str("GOLD_INSTRUMENT", "XAU_USD").upper(),
        oanda_api_key=env_str("OANDA_API_KEY", ""),
        oanda_account_id=env_str("OANDA_ACCOUNT_ID", ""),
        oanda_environment=env_str("OANDA_ENVIRONMENT", "practice").lower(),
        account_type=env_str("ACCOUNT_TYPE", "spread_bet").lower(),
        execution_mode=env_str("EXECUTION_MODE", "signal_only").lower(),
        paper_balance=env_float("PAPER_BALANCE", 10_000.0),
        gold_budget_allocation=env_float("GOLD_BUDGET_ALLOCATION", 1.00),
        fx_budget_allocation=env_float("FX_BUDGET_ALLOCATION", 1.00),
        max_risk_per_trade=env_float("MAX_RISK_PER_TRADE", 0.0075),
        max_total_gold_risk=env_float("MAX_TOTAL_GOLD_RISK", 0.03),
        max_open_gold_trades=env_int("MAX_OPEN_GOLD_TRADES", 1),
        leverage=env_float("LEVERAGE", 20.0),
        poll_interval_seconds=env_int("POLL_INTERVAL_SECONDS", 120),
        london_open_utc=env_int("LONDON_OPEN_UTC", 7),
        london_close_utc=env_int("LONDON_CLOSE_UTC", 16),
        ny_open_utc=env_int("NY_OPEN_UTC", 12),
        ny_close_utc=env_int("NY_CLOSE_UTC", 21),
        overlap_start_utc=env_int("OVERLAP_START_UTC", 12),
        overlap_end_utc=env_int("OVERLAP_END_UTC", 16),
        breakout_news_lookback_hours=env_int("BREAKOUT_NEWS_LOOKBACK_HOURS", 24),
        breakout_news_lookahead_hours=env_int("BREAKOUT_NEWS_LOOKAHEAD_HOURS", 24),
        pre_news_pause_minutes=env_int("PRE_NEWS_PAUSE_MINUTES", 30),
        post_news_settle_minutes=env_int("POST_NEWS_SETTLE_MINUTES", 20),
        max_entry_spread=env_float("MAX_ENTRY_SPREAD", 0.80),
        breakout_box_hours=env_int("BREAKOUT_BOX_HOURS", 18),
        breakout_buffer_atr=env_float("BREAKOUT_BUFFER_ATR", 0.20),
        breakout_min_box_atr_ratio=env_float("BREAKOUT_MIN_BOX_ATR_RATIO", 1.75),
        breakout_min_volume_ratio=env_float("BREAKOUT_MIN_VOLUME_RATIO", 1.10),
        breakout_overlap_only=env_bool("BREAKOUT_OVERLAP_ONLY", False),
        exhaustion_rr=env_float("EXHAUSTION_RR", 2.5),
        exhaustion_rsi_overbought=env_float("EXHAUSTION_RSI_OVERBOUGHT", 68.0),
        exhaustion_rsi_oversold=env_float("EXHAUSTION_RSI_OVERSOLD", 32.0),
        exhaustion_sr_lookback=env_int("EXHAUSTION_SR_LOOKBACK", 60),
        trend_ema_fast=env_int("TREND_EMA_FAST", 50),
        trend_ema_slow=env_int("TREND_EMA_SLOW", 200),
        trend_h1_confirm_ema_period=env_int("TREND_H1_CONFIRM_EMA_PERIOD", 50),
        trend_min_strength_atr=env_float("TREND_MIN_STRENGTH_ATR", 1.0),
        trend_fast_slope_bars=env_int("TREND_FAST_SLOPE_BARS", 3),
        trend_min_slope_atr=env_float("TREND_MIN_SLOPE_ATR", 0.06),
        trend_pullback_atr_tolerance=env_float("TREND_PULLBACK_ATR_TOLERANCE", 0.85),
        trend_stopout_cooldown_hours=env_int("TREND_STOPOUT_COOLDOWN_HOURS", 48),
        usd_regime_filter_enabled=env_bool("USD_REGIME_FILTER_ENABLED", True),
        usd_regime_fast_ema=env_int("USD_REGIME_FAST_EMA", 20),
        usd_regime_slow_ema=env_int("USD_REGIME_SLOW_EMA", 50),
        usd_regime_min_bias_atr=env_float("USD_REGIME_MIN_BIAS_ATR", 0.35),
        partial_profit_rr=env_float("PARTIAL_PROFIT_RR", 0.9),
        break_even_rr=env_float("BREAK_EVEN_RR", 1.3),
        trailing_atr_mult=env_float("TRAILING_ATR_MULT", 2.8),
        trailing_ema_period=env_int("TRAILING_EMA_PERIOD", 20),
        atr_period=env_int("ATR_PERIOD", 14),
        state_file=env_str("GOLD_STATE_FILE", "state.json"),
        shared_budget_file=env_str("GOLD_SHARED_BUDGET_FILE", "shared_budget_state.json"),
        macro_state_file=env_str("GOLD_MACRO_STATE_FILE", "gold_macro_state.json"),
        news_cache_file=env_str("GOLD_NEWS_CACHE_FILE", "gold_news_cache.json"),
        news_urls=env_csv(
            "GOLD_NEWS_URLS",
            "https://nfs.faireconomy.media/ff_calendar_thisweek.xml,https://www.forexfactory.com/ffcal_week_this.xml",
        ),
        breakout_volume_mode=env_str("BREAKOUT_VOLUME_MODE", "tick").lower(),
        breakout_external_volume_file=env_str("BREAKOUT_EXTERNAL_VOLUME_FILE", ""),
        breakout_external_volume_max_age_minutes=env_int("BREAKOUT_EXTERNAL_VOLUME_MAX_AGE_MINUTES", 30),
        breakout_external_min_volume_ratio=env_float("BREAKOUT_EXTERNAL_MIN_VOLUME_RATIO", 1.05),
        macro_breakout_spread_settle_seconds=env_int("MACRO_BREAKOUT_SPREAD_SETTLE_SECONDS", 45),
        macro_breakout_spread_stability_checks=env_int("MACRO_BREAKOUT_SPREAD_STABILITY_CHECKS", 3),
        macro_breakout_spread_stability_tolerance=env_float("MACRO_BREAKOUT_SPREAD_STABILITY_TOLERANCE", 0.15),
        real_yield_filter_enabled=env_bool("REAL_YIELD_FILTER_ENABLED", False),
        real_yield_state_max_age_hours=env_int("REAL_YIELD_STATE_MAX_AGE_HOURS", 24),
        real_yield_lookback_days=env_int("REAL_YIELD_LOOKBACK_DAYS", 5),
        real_yield_reduce_risk_bps=env_float("REAL_YIELD_REDUCE_RISK_BPS", 7.5),
        real_yield_veto_bps=env_float("REAL_YIELD_VETO_BPS", 15.0),
        real_yield_adverse_risk_multiplier=env_float("REAL_YIELD_ADVERSE_RISK_MULTIPLIER", 0.5),
        usd_regime_hard_veto_atr=env_float("USD_REGIME_HARD_VETO_ATR", 0.70),
        usd_regime_adverse_risk_multiplier=env_float("USD_REGIME_ADVERSE_RISK_MULTIPLIER", 0.5),
        scan_asia_active=env_bool("SCAN_ASIA_ACTIVE", True),
        asia_active_start_utc=env_int("ASIA_ACTIVE_START_UTC", 1),
        asia_active_end_utc=env_int("ASIA_ACTIVE_END_UTC", 6),
        trend_allow_inside_bar_confirmation=env_bool("TREND_ALLOW_INSIDE_BAR_CONFIRMATION", True),
        trend_allow_ema_reclaim=env_bool("TREND_ALLOW_EMA_RECLAIM", True),
        trend_ema_reclaim_rsi_min=env_float("TREND_EMA_RECLAIM_RSI_MIN", 45.0),
        trend_ema_reclaim_break_atr=env_float("TREND_EMA_RECLAIM_BREAK_ATR", 0.10),
        trend_ema_reclaim_touch_atr=env_float("TREND_EMA_RECLAIM_TOUCH_ATR", 0.50),
        breakout_allow_session_open=env_bool("BREAKOUT_ALLOW_SESSION_OPEN", True),
        breakout_session_open_hours_utc=env_str("BREAKOUT_SESSION_OPEN_HOURS_UTC", "7,8,9,12,13,14,15"),
        breakout_session_open_box_hours=env_int("BREAKOUT_SESSION_OPEN_BOX_HOURS", 8),
        breakout_session_open_min_box_atr_ratio=env_float("BREAKOUT_SESSION_OPEN_MIN_BOX_ATR_RATIO", 2.75),
        exhaustion_near_sr_atr_mult=env_float("EXHAUSTION_NEAR_SR_ATR_MULT", 1.3),
        # --- Sprint 1 knobs ---
        adaptive_spread_enabled=env_bool("ADAPTIVE_SPREAD_ENABLED", True),
        adaptive_spread_window_minutes=env_int("ADAPTIVE_SPREAD_WINDOW_MINUTES", 30),
        adaptive_spread_multiplier=env_float("ADAPTIVE_SPREAD_MULTIPLIER", 1.8),
        adaptive_spread_floor=env_float("ADAPTIVE_SPREAD_FLOOR", 0.25),
        adaptive_spread_min_samples=env_int("ADAPTIVE_SPREAD_MIN_SAMPLES", 6),
        real_yield_level_gate_enabled=env_bool("REAL_YIELD_LEVEL_GATE_ENABLED", True),
        real_yield_long_veto_level_pct=env_float("REAL_YIELD_LONG_VETO_LEVEL_PCT", 1.80),
        real_yield_short_veto_level_pct=env_float("REAL_YIELD_SHORT_VETO_LEVEL_PCT", 1.00),
        real_yield_slope_lookback_days=env_int("REAL_YIELD_SLOPE_LOOKBACK_DAYS", 5),
        real_yield_slope_threshold_bps=env_float("REAL_YIELD_SLOPE_THRESHOLD_BPS", 0.0),
        vol_target_sizing_enabled=env_bool("VOL_TARGET_SIZING_ENABLED", True),
        vol_target_nav_bps=env_float("VOL_TARGET_NAV_BPS", 25.0),
        breakout_impulse_confirm_enabled=env_bool("BREAKOUT_IMPULSE_CONFIRM_ENABLED", True),
        breakout_impulse_body_atr_min=env_float("BREAKOUT_IMPULSE_BODY_ATR_MIN", 0.40),
        breakout_impulse_require_tick_volume=env_bool("BREAKOUT_IMPULSE_REQUIRE_TICK_VOLUME", False),
        news_surprise_filter_enabled=env_bool("NEWS_SURPRISE_FILTER_ENABLED", False),
        news_surprise_min_composite=env_float("NEWS_SURPRISE_MIN_COMPOSITE", 0.60),
        news_surprise_require_direction_match=env_bool("NEWS_SURPRISE_REQUIRE_DIRECTION_MATCH", True),
        news_score_state_max_age_minutes=env_int("NEWS_SCORE_STATE_MAX_AGE_MINUTES", 120),
        drawdown_kill_switch_enabled=env_bool("DRAWDOWN_KILL_SWITCH_ENABLED", True),
        drawdown_soft_window_days=env_int("DRAWDOWN_SOFT_WINDOW_DAYS", 30),
        drawdown_soft_threshold_pct=env_float("DRAWDOWN_SOFT_THRESHOLD_PCT", -0.06),
        drawdown_soft_risk_per_trade=env_float("DRAWDOWN_SOFT_RISK_PER_TRADE", 0.003),
        drawdown_hard_window_days=env_int("DRAWDOWN_HARD_WINDOW_DAYS", 90),
        drawdown_hard_threshold_pct=env_float("DRAWDOWN_HARD_THRESHOLD_PCT", -0.10),
        drawdown_equity_history_max_days=env_int("DRAWDOWN_EQUITY_HISTORY_MAX_DAYS", 180),
        # Sprint 2: weekend / calibration / backtest microstructure / CFTC
        weekend_gap_handling_enabled=env_bool("WEEKEND_GAP_HANDLING_ENABLED", True),
        weekend_flatten_hour_utc=env_int("WEEKEND_FLATTEN_HOUR_UTC", 20),
        weekend_flatten_weekday=env_int("WEEKEND_FLATTEN_WEEKDAY", 4),
        weekend_stop_widen_enabled=env_bool("WEEKEND_STOP_WIDEN_ENABLED", True),
        weekend_stop_widen_hour_utc=env_int("WEEKEND_STOP_WIDEN_HOUR_UTC", 19),
        weekend_stop_widen_atr_mult=env_float("WEEKEND_STOP_WIDEN_ATR_MULT", 2.0),
        weekend_block_new_entries_hour_utc=env_int("WEEKEND_BLOCK_NEW_ENTRIES_HOUR_UTC", 19),
        calibration_min_trades_for_adjustment=env_int("CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT", 40),
        calibration_min_trades_for_block=env_int("CALIBRATION_MIN_TRADES_FOR_BLOCK", 80),
        calibration_shrinkage_denominator=env_int("CALIBRATION_SHRINKAGE_DENOMINATOR", 200),
        calibration_block_pf_threshold=env_float("CALIBRATION_BLOCK_PF_THRESHOLD", 0.7),
        calibration_block_expectancy_threshold=env_float("CALIBRATION_BLOCK_EXPECTANCY_THRESHOLD", -5.0),
        calibration_block_win_rate_threshold=env_float("CALIBRATION_BLOCK_WIN_RATE_THRESHOLD", 0.35),
        backtest_spread_model_enabled=env_bool("BACKTEST_SPREAD_MODEL_ENABLED", True),
        backtest_spread_profile=env_str("BACKTEST_SPREAD_PROFILE", "gold_m15"),
        backtest_spread_news_window_minutes=env_int("BACKTEST_SPREAD_NEWS_WINDOW_MINUTES", 2),
        backtest_spread_news_multiplier=env_float("BACKTEST_SPREAD_NEWS_MULTIPLIER", 6.0),
        backtest_exit_slippage_multiplier=env_float("BACKTEST_EXIT_SLIPPAGE_MULTIPLIER", 1.5),
        backtest_weekend_gap_enabled=env_bool("BACKTEST_WEEKEND_GAP_ENABLED", True),
        backtest_weekend_flatten_hour_utc=env_int("BACKTEST_WEEKEND_FLATTEN_HOUR_UTC", 20),
        backtest_financing_enabled=env_bool("BACKTEST_FINANCING_ENABLED", True),
        backtest_financing_long_apr=env_float("BACKTEST_FINANCING_LONG_APR", 0.05),
        backtest_financing_short_apr=env_float("BACKTEST_FINANCING_SHORT_APR", 0.0),
        cftc_filter_enabled=env_bool("CFTC_FILTER_ENABLED", False),
        cftc_state_max_age_days=env_int("CFTC_STATE_MAX_AGE_DAYS", 10),
        cftc_extreme_percentile=env_float("CFTC_EXTREME_PERCENTILE", 0.85),
        cftc_extreme_score_offset=env_float("CFTC_EXTREME_SCORE_OFFSET", 8.0),
        # Sprint 3 knobs
        co_trade_gates_enabled=env_bool("CO_TRADE_GATES_ENABLED", False),
        co_trade_state_max_age_hours=env_int("CO_TRADE_STATE_MAX_AGE_HOURS", 24),
        co_trade_es_risk_on_long_veto_pct=env_float("CO_TRADE_ES_RISK_ON_LONG_VETO_PCT", 0.015),
        co_trade_cnh_stress_short_veto_pct=env_float("CO_TRADE_CNH_STRESS_SHORT_VETO_PCT", 0.004),
        co_trade_dxy_weak_favourable_pct=env_float("CO_TRADE_DXY_WEAK_FAVOURABLE_PCT", -0.003),
        co_trade_favourable_size_mult=env_float("CO_TRADE_FAVOURABLE_SIZE_MULT", 1.25),
        regime_filter_enabled=env_bool("REGIME_FILTER_ENABLED", False),
        regime_quiet_atr_pct_max=env_float("REGIME_QUIET_ATR_PCT_MAX", 0.006),
        regime_trend_atr_pct_max=env_float("REGIME_TREND_ATR_PCT_MAX", 0.014),
        regime_spike_atr_pct_min=env_float("REGIME_SPIKE_ATR_PCT_MIN", 0.016),
        regime_quiet_strategies=env_str("REGIME_QUIET_STRATEGIES", "EXHAUSTION_REVERSAL"),
        regime_trend_strategies=env_str("REGIME_TREND_STRATEGIES", "TREND_PULLBACK,MACRO_BREAKOUT"),
        regime_spike_strategies=env_str("REGIME_SPIKE_STRATEGIES", "MACRO_BREAKOUT"),
        options_iv_gate_enabled=env_bool("OPTIONS_IV_GATE_ENABLED", False),
        options_iv_state_max_age_hours=env_int("OPTIONS_IV_STATE_MAX_AGE_HOURS", 24),
        options_iv_realised_fraction_threshold=env_float("OPTIONS_IV_REALISED_FRACTION_THRESHOLD", 0.60),
        walk_forward_enabled=env_bool("WALK_FORWARD_ENABLED", False),
        walk_forward_in_sample_days=env_int("WALK_FORWARD_IN_SAMPLE_DAYS", 275),
        walk_forward_out_sample_days=env_int("WALK_FORWARD_OUT_SAMPLE_DAYS", 90),
        walk_forward_step_days=env_int("WALK_FORWARD_STEP_DAYS", 90),
        walk_forward_min_out_sample_pf=env_float("WALK_FORWARD_MIN_OUT_SAMPLE_PF", 1.15),
        walk_forward_max_pf_degradation=env_float("WALK_FORWARD_MAX_PF_DEGRADATION", 0.50),
        execution_use_limit_entry=env_bool("EXECUTION_USE_LIMIT_ENTRY", False),
        execution_limit_spread_multiplier=env_float("EXECUTION_LIMIT_SPREAD_MULTIPLIER", 1.25),
        execution_limit_timeout_seconds=env_int("EXECUTION_LIMIT_TIMEOUT_SECONDS", 3),
        execution_guaranteed_stop_enabled=env_bool("EXECUTION_GUARANTEED_STOP_ENABLED", False),
        # Q2 knobs
        miners_overlay_enabled=env_bool("MINERS_OVERLAY_ENABLED", False),
        miners_state_max_age_hours=env_int("MINERS_STATE_MAX_AGE_HOURS", 24),
        miners_confirm_threshold_pct=env_float("MINERS_CONFIRM_THRESHOLD_PCT", 0.005),
        miners_etf_flow_threshold_pct=env_float("MINERS_ETF_FLOW_THRESHOLD_PCT", 0.002),
        miners_score_offset=env_float("MINERS_SCORE_OFFSET", 6.0),
        miners_long_confirm_size_mult=env_float("MINERS_LONG_CONFIRM_SIZE_MULT", 1.2),
        factor_model_enabled=env_bool("FACTOR_MODEL_ENABLED", False),
        factor_model_state_max_age_hours=env_int("FACTOR_MODEL_STATE_MAX_AGE_HOURS", 168),
        factor_tips_weight=env_float("FACTOR_TIPS_WEIGHT", 0.40),
        factor_dxy_weight=env_float("FACTOR_DXY_WEIGHT", 0.35),
        factor_gld_weight=env_float("FACTOR_GLD_WEIGHT", 0.25),
        factor_align_threshold=env_float("FACTOR_ALIGN_THRESHOLD", 0.40),
        factor_align_size_mult=env_float("FACTOR_ALIGN_SIZE_MULT", 1.5),
        factor_oppose_size_mult=env_float("FACTOR_OPPOSE_SIZE_MULT", 0.5),
        factor_score_offset=env_float("FACTOR_SCORE_OFFSET", 5.0),
        central_bank_flow_enabled=env_bool("CENTRAL_BANK_FLOW_ENABLED", False),
        central_bank_state_max_age_days=env_int("CENTRAL_BANK_STATE_MAX_AGE_DAYS", 100),
        central_bank_high_demand_tonnes=env_float("CENTRAL_BANK_HIGH_DEMAND_TONNES", 300.0),
        central_bank_short_veto_strategies=env_str(
            "CENTRAL_BANK_SHORT_VETO_STRATEGIES", "EXHAUSTION_REVERSAL"
        ),
        risk_parity_enabled=env_bool("RISK_PARITY_ENABLED", False),
        risk_parity_rebalance_interval_days=env_int("RISK_PARITY_REBALANCE_INTERVAL_DAYS", 7),
        risk_parity_lookback_days=env_int("RISK_PARITY_LOOKBACK_DAYS", 20),
        risk_parity_min_weight=env_float("RISK_PARITY_MIN_WEIGHT", 0.20),
        risk_parity_max_weight=env_float("RISK_PARITY_MAX_WEIGHT", 0.80),
        risk_parity_rebalance_threshold=env_float("RISK_PARITY_REBALANCE_THRESHOLD", 0.05),
    )
    _validate_settings(settings)
    return settings


def resolve_path(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _validate_settings(settings: Settings) -> None:
    if settings.instrument != "XAU_USD":
        raise ValueError("Gold-bot only supports XAU_USD")
    if abs((settings.gold_budget_allocation + settings.fx_budget_allocation) - 1.0) > 0.001:
        raise ValueError("GOLD_BUDGET_ALLOCATION and FX_BUDGET_ALLOCATION must sum to 1.0")
    if settings.execution_mode not in {"signal_only", "paper", "live"}:
        raise ValueError("EXECUTION_MODE must be signal_only, paper, or live")
    if settings.max_total_gold_risk < settings.max_risk_per_trade:
        raise ValueError("MAX_TOTAL_GOLD_RISK must be >= MAX_RISK_PER_TRADE")
    if settings.trend_ema_fast >= settings.trend_ema_slow:
        raise ValueError("TREND_EMA_FAST must be smaller than TREND_EMA_SLOW")
    if settings.trend_h1_confirm_ema_period <= 0:
        raise ValueError("TREND_H1_CONFIRM_EMA_PERIOD must be > 0")
    if settings.trend_min_strength_atr <= 0 or settings.trend_min_slope_atr < 0:
        raise ValueError("Trend-strength filters must be non-negative and strength must be > 0")
    if settings.trend_fast_slope_bars <= 0 or settings.trend_stopout_cooldown_hours < 0:
        raise ValueError("Trend slope bars must be > 0 and cooldown must be >= 0")
    if settings.usd_regime_fast_ema <= 0 or settings.usd_regime_slow_ema <= settings.usd_regime_fast_ema:
        raise ValueError("USD regime EMA periods must be > 0 and slow EMA must be larger than fast EMA")
    if settings.usd_regime_min_bias_atr < 0:
        raise ValueError("USD regime bias threshold must be >= 0")
    if settings.breakout_box_hours < 12 or settings.breakout_box_hours > 24:
        raise ValueError("BREAKOUT_BOX_HOURS should be between 12 and 24")
    if settings.max_entry_spread <= 0:
        raise ValueError("MAX_ENTRY_SPREAD must be > 0")
    if settings.breakout_min_volume_ratio < 1.0:
        raise ValueError("BREAKOUT_MIN_VOLUME_RATIO should be >= 1.0")
    if settings.breakout_volume_mode not in {"tick", "external", "hybrid"}:
        raise ValueError("BREAKOUT_VOLUME_MODE must be tick, external, or hybrid")
    if settings.breakout_external_volume_max_age_minutes < 0:
        raise ValueError("BREAKOUT_EXTERNAL_VOLUME_MAX_AGE_MINUTES must be >= 0")
    if settings.breakout_external_min_volume_ratio < 1.0:
        raise ValueError("BREAKOUT_EXTERNAL_MIN_VOLUME_RATIO should be >= 1.0")
    if settings.macro_breakout_spread_settle_seconds < 0:
        raise ValueError("MACRO_BREAKOUT_SPREAD_SETTLE_SECONDS must be >= 0")
    if settings.macro_breakout_spread_stability_checks <= 0:
        raise ValueError("MACRO_BREAKOUT_SPREAD_STABILITY_CHECKS must be > 0")
    if settings.macro_breakout_spread_stability_tolerance < 0:
        raise ValueError("MACRO_BREAKOUT_SPREAD_STABILITY_TOLERANCE must be >= 0")
    if settings.real_yield_state_max_age_hours < 0:
        raise ValueError("REAL_YIELD_STATE_MAX_AGE_HOURS must be >= 0")
    if settings.real_yield_lookback_days <= 0:
        raise ValueError("REAL_YIELD_LOOKBACK_DAYS must be > 0")
    if settings.real_yield_reduce_risk_bps < 0 or settings.real_yield_veto_bps < 0:
        raise ValueError("Real-yield thresholds must be >= 0")
    if settings.real_yield_veto_bps < settings.real_yield_reduce_risk_bps:
        raise ValueError("REAL_YIELD_VETO_BPS must be >= REAL_YIELD_REDUCE_RISK_BPS")
    if settings.real_yield_adverse_risk_multiplier <= 0 or settings.real_yield_adverse_risk_multiplier > 1:
        raise ValueError("REAL_YIELD_ADVERSE_RISK_MULTIPLIER must be > 0 and <= 1")
    if settings.usd_regime_hard_veto_atr < settings.usd_regime_min_bias_atr:
        raise ValueError("USD_REGIME_HARD_VETO_ATR must be >= USD_REGIME_MIN_BIAS_ATR")
    if settings.usd_regime_adverse_risk_multiplier <= 0 or settings.usd_regime_adverse_risk_multiplier > 1:
        raise ValueError("USD_REGIME_ADVERSE_RISK_MULTIPLIER must be > 0 and <= 1")
    if settings.asia_active_start_utc < 0 or settings.asia_active_end_utc > 24:
        raise ValueError("ASIA_ACTIVE_START_UTC / ASIA_ACTIVE_END_UTC must be within 0..24")
    if settings.asia_active_end_utc <= settings.asia_active_start_utc:
        raise ValueError("ASIA_ACTIVE_END_UTC must be > ASIA_ACTIVE_START_UTC")
    if settings.partial_profit_rr <= 0 or settings.break_even_rr <= 0 or settings.trailing_atr_mult <= 0:
        raise ValueError("Exit-plan multipliers must be > 0")
    # Sprint 1 validations
    if settings.adaptive_spread_window_minutes <= 0:
        raise ValueError("ADAPTIVE_SPREAD_WINDOW_MINUTES must be > 0")
    if settings.adaptive_spread_multiplier <= 1.0:
        raise ValueError("ADAPTIVE_SPREAD_MULTIPLIER must be > 1.0")
    if settings.adaptive_spread_floor <= 0:
        raise ValueError("ADAPTIVE_SPREAD_FLOOR must be > 0")
    if settings.adaptive_spread_min_samples < 1:
        raise ValueError("ADAPTIVE_SPREAD_MIN_SAMPLES must be >= 1")
    if settings.real_yield_long_veto_level_pct <= settings.real_yield_short_veto_level_pct:
        raise ValueError("REAL_YIELD_LONG_VETO_LEVEL_PCT must be > REAL_YIELD_SHORT_VETO_LEVEL_PCT")
    if settings.real_yield_slope_lookback_days <= 0:
        raise ValueError("REAL_YIELD_SLOPE_LOOKBACK_DAYS must be > 0")
    if settings.vol_target_nav_bps <= 0:
        raise ValueError("VOL_TARGET_NAV_BPS must be > 0")
    if settings.breakout_impulse_body_atr_min <= 0:
        raise ValueError("BREAKOUT_IMPULSE_BODY_ATR_MIN must be > 0")
    if not (0.0 <= settings.news_surprise_min_composite <= 1.0):
        raise ValueError("NEWS_SURPRISE_MIN_COMPOSITE must be in [0, 1]")
    if settings.drawdown_soft_window_days <= 0 or settings.drawdown_hard_window_days <= 0:
        raise ValueError("Drawdown windows must be > 0")
    if settings.drawdown_soft_threshold_pct >= 0 or settings.drawdown_hard_threshold_pct >= 0:
        raise ValueError("Drawdown thresholds must be negative (e.g. -0.06 for -6%)")
    if settings.drawdown_hard_threshold_pct > settings.drawdown_soft_threshold_pct:
        raise ValueError("Hard drawdown threshold must be <= soft drawdown threshold (more negative)")
    if not (0 < settings.drawdown_soft_risk_per_trade <= settings.max_risk_per_trade):
        raise ValueError("DRAWDOWN_SOFT_RISK_PER_TRADE must be > 0 and <= MAX_RISK_PER_TRADE")
    # Sprint 2 validations
    if not (0 <= settings.weekend_flatten_hour_utc <= 23):
        raise ValueError("WEEKEND_FLATTEN_HOUR_UTC must be in [0, 23]")
    if not (0 <= settings.weekend_stop_widen_hour_utc <= 23):
        raise ValueError("WEEKEND_STOP_WIDEN_HOUR_UTC must be in [0, 23]")
    if not (0 <= settings.weekend_block_new_entries_hour_utc <= 23):
        raise ValueError("WEEKEND_BLOCK_NEW_ENTRIES_HOUR_UTC must be in [0, 23]")
    if not (0 <= settings.weekend_flatten_weekday <= 6):
        raise ValueError("WEEKEND_FLATTEN_WEEKDAY must be in [0, 6] (Mon=0..Sun=6)")
    if settings.weekend_stop_widen_atr_mult < 0:
        raise ValueError("WEEKEND_STOP_WIDEN_ATR_MULT must be >= 0")
    if settings.weekend_block_new_entries_hour_utc > settings.weekend_flatten_hour_utc:
        raise ValueError(
            "WEEKEND_BLOCK_NEW_ENTRIES_HOUR_UTC must be <= WEEKEND_FLATTEN_HOUR_UTC"
        )
    if settings.calibration_min_trades_for_adjustment < 1:
        raise ValueError("CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT must be >= 1")
    if settings.calibration_min_trades_for_block < settings.calibration_min_trades_for_adjustment:
        raise ValueError(
            "CALIBRATION_MIN_TRADES_FOR_BLOCK must be >= CALIBRATION_MIN_TRADES_FOR_ADJUSTMENT"
        )
    if settings.calibration_shrinkage_denominator < 1:
        raise ValueError("CALIBRATION_SHRINKAGE_DENOMINATOR must be >= 1")
    if settings.calibration_block_pf_threshold <= 0:
        raise ValueError("CALIBRATION_BLOCK_PF_THRESHOLD must be > 0")
    if not (0 <= settings.calibration_block_win_rate_threshold <= 1):
        raise ValueError("CALIBRATION_BLOCK_WIN_RATE_THRESHOLD must be in [0, 1]")
    if settings.backtest_spread_news_window_minutes < 0:
        raise ValueError("BACKTEST_SPREAD_NEWS_WINDOW_MINUTES must be >= 0")
    if settings.backtest_spread_news_multiplier < 1.0:
        raise ValueError("BACKTEST_SPREAD_NEWS_MULTIPLIER must be >= 1.0")
    if settings.backtest_exit_slippage_multiplier < 1.0:
        raise ValueError("BACKTEST_EXIT_SLIPPAGE_MULTIPLIER must be >= 1.0")
    if settings.backtest_financing_long_apr < 0 or settings.backtest_financing_short_apr < 0:
        raise ValueError("BACKTEST_FINANCING_*_APR must be >= 0")
    if not (0 <= settings.backtest_weekend_flatten_hour_utc <= 23):
        raise ValueError("BACKTEST_WEEKEND_FLATTEN_HOUR_UTC must be in [0, 23]")
    if not (0.5 <= settings.cftc_extreme_percentile <= 1.0):
        raise ValueError("CFTC_EXTREME_PERCENTILE must be in [0.5, 1.0]")
    if settings.cftc_extreme_score_offset < 0:
        raise ValueError("CFTC_EXTREME_SCORE_OFFSET must be >= 0")
    if settings.cftc_state_max_age_days < 1:
        raise ValueError("CFTC_STATE_MAX_AGE_DAYS must be >= 1")
    # Sprint 3 validations
    if settings.co_trade_state_max_age_hours < 0:
        raise ValueError("CO_TRADE_STATE_MAX_AGE_HOURS must be >= 0")
    if settings.co_trade_es_risk_on_long_veto_pct <= 0:
        raise ValueError("CO_TRADE_ES_RISK_ON_LONG_VETO_PCT must be > 0")
    if settings.co_trade_cnh_stress_short_veto_pct <= 0:
        raise ValueError("CO_TRADE_CNH_STRESS_SHORT_VETO_PCT must be > 0")
    if settings.co_trade_dxy_weak_favourable_pct >= 0:
        raise ValueError("CO_TRADE_DXY_WEAK_FAVOURABLE_PCT must be < 0 (a DXY decline)")
    if settings.co_trade_favourable_size_mult < 1.0:
        raise ValueError("CO_TRADE_FAVOURABLE_SIZE_MULT must be >= 1.0")
    if settings.regime_quiet_atr_pct_max <= 0:
        raise ValueError("REGIME_QUIET_ATR_PCT_MAX must be > 0")
    if settings.regime_trend_atr_pct_max < settings.regime_quiet_atr_pct_max:
        raise ValueError("REGIME_TREND_ATR_PCT_MAX must be >= REGIME_QUIET_ATR_PCT_MAX")
    if settings.regime_spike_atr_pct_min < settings.regime_trend_atr_pct_max:
        raise ValueError("REGIME_SPIKE_ATR_PCT_MIN must be >= REGIME_TREND_ATR_PCT_MAX")
    if settings.options_iv_state_max_age_hours < 0:
        raise ValueError("OPTIONS_IV_STATE_MAX_AGE_HOURS must be >= 0")
    if not (0.0 < settings.options_iv_realised_fraction_threshold <= 5.0):
        raise ValueError("OPTIONS_IV_REALISED_FRACTION_THRESHOLD must be in (0, 5]")
    if settings.walk_forward_in_sample_days <= 0 or settings.walk_forward_out_sample_days <= 0:
        raise ValueError("Walk-forward window sizes must be > 0")
    if settings.walk_forward_step_days <= 0:
        raise ValueError("WALK_FORWARD_STEP_DAYS must be > 0")
    if settings.walk_forward_min_out_sample_pf <= 0:
        raise ValueError("WALK_FORWARD_MIN_OUT_SAMPLE_PF must be > 0")
    if not (0.0 <= settings.walk_forward_max_pf_degradation <= 1.0):
        raise ValueError("WALK_FORWARD_MAX_PF_DEGRADATION must be in [0, 1]")
    if settings.execution_limit_spread_multiplier < 0:
        raise ValueError("EXECUTION_LIMIT_SPREAD_MULTIPLIER must be >= 0")
    if settings.execution_limit_timeout_seconds < 1:
        raise ValueError("EXECUTION_LIMIT_TIMEOUT_SECONDS must be >= 1")
    # Q2 validations
    if settings.miners_state_max_age_hours < 0:
        raise ValueError("MINERS_STATE_MAX_AGE_HOURS must be >= 0")
    if settings.miners_confirm_threshold_pct <= 0:
        raise ValueError("MINERS_CONFIRM_THRESHOLD_PCT must be > 0")
    if settings.miners_etf_flow_threshold_pct <= 0:
        raise ValueError("MINERS_ETF_FLOW_THRESHOLD_PCT must be > 0")
    if settings.miners_score_offset < 0:
        raise ValueError("MINERS_SCORE_OFFSET must be >= 0")
    if settings.miners_long_confirm_size_mult < 1.0:
        raise ValueError("MINERS_LONG_CONFIRM_SIZE_MULT must be >= 1.0")
    if settings.factor_model_state_max_age_hours < 0:
        raise ValueError("FACTOR_MODEL_STATE_MAX_AGE_HOURS must be >= 0")
    if settings.factor_tips_weight < 0 or settings.factor_dxy_weight < 0 or settings.factor_gld_weight < 0:
        raise ValueError("Factor-model weights must all be >= 0")
    if (settings.factor_tips_weight + settings.factor_dxy_weight + settings.factor_gld_weight) <= 0:
        raise ValueError("At least one factor-model weight must be > 0")
    if not (0.0 < settings.factor_align_threshold <= 1.0):
        raise ValueError("FACTOR_ALIGN_THRESHOLD must be in (0, 1]")
    if settings.factor_align_size_mult < 1.0:
        raise ValueError("FACTOR_ALIGN_SIZE_MULT must be >= 1.0")
    if not (0.0 < settings.factor_oppose_size_mult <= 1.0):
        raise ValueError("FACTOR_OPPOSE_SIZE_MULT must be in (0, 1]")
    if settings.factor_score_offset < 0:
        raise ValueError("FACTOR_SCORE_OFFSET must be >= 0")
    if settings.central_bank_state_max_age_days < 1:
        raise ValueError("CENTRAL_BANK_STATE_MAX_AGE_DAYS must be >= 1")
    if settings.central_bank_high_demand_tonnes <= 0:
        raise ValueError("CENTRAL_BANK_HIGH_DEMAND_TONNES must be > 0")
    if settings.risk_parity_rebalance_interval_days < 1:
        raise ValueError("RISK_PARITY_REBALANCE_INTERVAL_DAYS must be >= 1")
    if settings.risk_parity_lookback_days < 2:
        raise ValueError("RISK_PARITY_LOOKBACK_DAYS must be >= 2")
    if not (0.0 < settings.risk_parity_min_weight < settings.risk_parity_max_weight <= 1.0):
        raise ValueError(
            "Risk-parity weights must satisfy 0 < min_weight < max_weight <= 1"
        )
    if not (0.0 <= settings.risk_parity_rebalance_threshold <= 0.5):
        raise ValueError("RISK_PARITY_REBALANCE_THRESHOLD must be in [0, 0.5]")