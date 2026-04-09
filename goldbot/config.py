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


def load_settings() -> Settings:
    settings = Settings(
        instrument=env_str("GOLD_INSTRUMENT", "XAU_USD").upper(),
        oanda_api_key=env_str("OANDA_API_KEY", ""),
        oanda_account_id=env_str("OANDA_ACCOUNT_ID", ""),
        oanda_environment=env_str("OANDA_ENVIRONMENT", "practice").lower(),
        account_type=env_str("ACCOUNT_TYPE", "spread_bet").lower(),
        execution_mode=env_str("EXECUTION_MODE", "signal_only").lower(),
        paper_balance=env_float("PAPER_BALANCE", 10_000.0),
        gold_budget_allocation=env_float("GOLD_BUDGET_ALLOCATION", 0.50),
        fx_budget_allocation=env_float("FX_BUDGET_ALLOCATION", 0.50),
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
        breakout_news_lookback_hours=env_int("BREAKOUT_NEWS_LOOKBACK_HOURS", 8),
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
        trend_min_slope_atr=env_float("TREND_MIN_SLOPE_ATR", 0.10),
        trend_pullback_atr_tolerance=env_float("TREND_PULLBACK_ATR_TOLERANCE", 0.65),
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
    if settings.partial_profit_rr <= 0 or settings.break_even_rr <= 0 or settings.trailing_atr_mult <= 0:
        raise ValueError("Exit-plan multipliers must be > 0")