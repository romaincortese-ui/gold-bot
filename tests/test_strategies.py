from datetime import datetime, timedelta, timezone

import pandas as pd

from goldbot.config import Settings
from goldbot.models import CalendarEvent
from goldbot.strategies import score_macro_breakout, score_trend_pullback
from goldbot.volume_oracle import BreakoutVolumeSignal


def build_settings() -> Settings:
    return Settings(
        instrument="XAU_USD",
        oanda_api_key="",
        oanda_account_id="",
        oanda_environment="practice",
        account_type="spread_bet",
        execution_mode="signal_only",
        paper_balance=10_000,
        gold_budget_allocation=0.5,
        fx_budget_allocation=0.5,
        max_risk_per_trade=0.01,
        max_total_gold_risk=0.03,
        max_open_gold_trades=1,
        leverage=20,
        poll_interval_seconds=60,
        london_open_utc=7,
        london_close_utc=16,
        ny_open_utc=12,
        ny_close_utc=21,
        overlap_start_utc=12,
        overlap_end_utc=16,
        breakout_news_lookback_hours=8,
        breakout_news_lookahead_hours=24,
        pre_news_pause_minutes=30,
        post_news_settle_minutes=20,
        max_entry_spread=0.8,
        breakout_box_hours=18,
        breakout_buffer_atr=0.2,
        breakout_min_box_atr_ratio=1.75,
        breakout_min_volume_ratio=1.1,
        breakout_overlap_only=True,
        exhaustion_rr=2.5,
        exhaustion_rsi_overbought=72,
        exhaustion_rsi_oversold=28,
        exhaustion_sr_lookback=60,
        trend_ema_fast=50,
        trend_ema_slow=200,
        trend_h1_confirm_ema_period=50,
        trend_min_strength_atr=1.25,
        trend_fast_slope_bars=3,
        trend_min_slope_atr=0.10,
        trend_pullback_atr_tolerance=0.65,
        trend_stopout_cooldown_hours=48,
        usd_regime_filter_enabled=True,
        usd_regime_fast_ema=20,
        usd_regime_slow_ema=50,
        usd_regime_min_bias_atr=0.35,
        partial_profit_rr=1.25,
        break_even_rr=1.25,
        trailing_atr_mult=2.8,
        trailing_ema_period=20,
        atr_period=14,
        state_file="state.json",
        shared_budget_file="shared.json",
        macro_state_file="macro.json",
        news_cache_file="cache.json",
        news_urls=["https://example.com"],
    )


def test_score_macro_breakout_finds_post_news_long_break() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    event_time = now - timedelta(hours=1)
    h1_times = pd.date_range(end=now, periods=40, freq="h", tz="UTC")
    h1_rows = []
    for index, timestamp in enumerate(h1_times):
        base = 3010.0 if index < 38 else 3012.0 + index
        high = 3012.0 if timestamp <= event_time else base + 3.0
        low = 3008.0 if timestamp <= event_time else base - 2.0
        h1_rows.append({"time": timestamp, "open": base, "high": high, "low": low, "close": base + 0.4, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    m15_times = pd.date_range(end=now, periods=60, freq="15min", tz="UTC")
    m15_rows = []
    for index, timestamp in enumerate(m15_times):
        close = 3010.0 + (index * 0.05)
        if index >= 56:
            close = 3014.5 + (index - 56) * 1.2
        volume = 150 if index == len(m15_times) - 1 else 100
        m15_rows.append({"time": timestamp, "open": close - 0.3, "high": close + 0.5, "low": close - 0.6, "close": close, "volume": volume})
    df_m15 = pd.DataFrame(m15_rows)

    event = CalendarEvent("US CPI", "USD", "high", event_time, "test")
    opportunity = score_macro_breakout(settings, now, "OVERLAP", df_m15, df_h1, [event])

    assert opportunity is not None
    assert opportunity.strategy == "MACRO_BREAKOUT"
    assert opportunity.direction == "LONG"
    assert opportunity.metadata["volume_ratio"] >= settings.breakout_min_volume_ratio
    assert opportunity.take_profit_price is None


def test_score_trend_pullback_identifies_bullish_setup() -> None:
    settings = build_settings()
    settings = Settings(**{**settings.__dict__, "trend_min_strength_atr": 0.5, "trend_min_slope_atr": 0.02, "trend_h1_confirm_ema_period": 20, "trend_pullback_atr_tolerance": 1.5})
    h4_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=260, freq="4h", tz="UTC")
    h4_rows = []
    for index, timestamp in enumerate(h4_times):
        close = 2800 + index * 1.5
        if index > 250:
            close -= (260 - index) * 2.0
        h4_rows.append({"time": timestamp, "open": close - 1.0, "high": close + 2.0, "low": close - 2.0, "close": close, "volume": 100})
    df_h4 = pd.DataFrame(h4_rows)

    h1_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=120, freq="h", tz="UTC")
    h1_rows = []
    for index, timestamp in enumerate(h1_times):
        close = 3170 - abs(60 - index) * 0.3
        open_price = close + 0.4 if index == 118 else close - 0.2
        final_close = close + 3.4 if index == 119 else close
        high = max(open_price, final_close) + 0.6
        low = min(open_price, final_close) - 0.8
        h1_rows.append({"time": timestamp, "open": open_price, "high": high, "low": low, "close": final_close, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    opportunity = score_trend_pullback(settings, df_h1, df_h4, build_usd_proxy_frames("weak"))

    assert opportunity is not None
    assert opportunity.strategy == "TREND_PULLBACK"
    assert opportunity.direction == "LONG"
    assert opportunity.exit_plan["partial_take_profit_fraction"] == 0.5


def test_score_macro_breakout_rejects_low_volume_break() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    event_time = now - timedelta(hours=1)
    h1_times = pd.date_range(end=now, periods=40, freq="h", tz="UTC")
    h1_rows = []
    for timestamp in h1_times:
        h1_rows.append({"time": timestamp, "open": 3010.0, "high": 3012.0, "low": 3008.0, "close": 3010.4, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    m15_times = pd.date_range(end=now, periods=60, freq="15min", tz="UTC")
    m15_rows = []
    for index, timestamp in enumerate(m15_times):
        close = 3010.0 + (index * 0.05)
        if index >= 56:
            close = 3014.5 + (index - 56) * 1.2
        volume = 85 if index == len(m15_times) - 1 else 100
        m15_rows.append({"time": timestamp, "open": close - 0.3, "high": close + 0.5, "low": close - 0.6, "close": close, "volume": volume})
    df_m15 = pd.DataFrame(m15_rows)

    opportunity = score_macro_breakout(settings, now, "OVERLAP", df_m15, df_h1, [CalendarEvent("US CPI", "USD", "high", event_time, "test")])

    assert opportunity is None


def test_score_macro_breakout_requires_overlap_when_enabled() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    event_time = now - timedelta(hours=1)
    h1_times = pd.date_range(end=now, periods=40, freq="h", tz="UTC")
    h1_rows = []
    for timestamp in h1_times:
        h1_rows.append({"time": timestamp, "open": 3010.0, "high": 3012.0, "low": 3008.0, "close": 3010.4, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    m15_times = pd.date_range(end=now, periods=60, freq="15min", tz="UTC")
    m15_rows = []
    for index, timestamp in enumerate(m15_times):
        close = 3010.0 + (index * 0.05)
        if index >= 56:
            close = 3014.5 + (index - 56) * 1.2
        m15_rows.append({"time": timestamp, "open": close - 0.3, "high": close + 0.5, "low": close - 0.6, "close": close, "volume": 150})
    df_m15 = pd.DataFrame(m15_rows)

    opportunity = score_macro_breakout(settings, now, "LONDON", df_m15, df_h1, [CalendarEvent("US CPI", "USD", "high", event_time, "test")])

    assert opportunity is None


def test_score_macro_breakout_accepts_external_volume_confirmation() -> None:
    settings = build_settings()
    settings = Settings(
        **{
            **settings.__dict__,
            "breakout_volume_mode": "hybrid",
            "breakout_external_min_volume_ratio": 1.2,
        }
    )
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    event_time = now - timedelta(hours=1)
    h1_times = pd.date_range(end=now, periods=40, freq="h", tz="UTC")
    df_h1 = pd.DataFrame(
        [{"time": timestamp, "open": 3010.0, "high": 3012.0, "low": 3008.0, "close": 3010.4, "volume": 100} for timestamp in h1_times]
    )
    m15_times = pd.date_range(end=now, periods=60, freq="15min", tz="UTC")
    m15_rows = []
    for index, timestamp in enumerate(m15_times):
        close = 3010.0 + (index * 0.05)
        if index >= 56:
            close = 3014.5 + (index - 56) * 1.2
        volume = 150 if index == len(m15_times) - 1 else 100
        m15_rows.append({"time": timestamp, "open": close - 0.3, "high": close + 0.5, "low": close - 0.6, "close": close, "volume": volume})
    df_m15 = pd.DataFrame(m15_rows)

    opportunity = score_macro_breakout(
        settings,
        now,
        "OVERLAP",
        df_m15,
        df_h1,
        [CalendarEvent("US CPI", "USD", "high", event_time, "test")],
        BreakoutVolumeSignal(source="cme", as_of=now, volume_ratio=1.35),
    )

    assert opportunity is not None
    assert opportunity.metadata["external_volume_ratio"] == 1.35
    assert opportunity.metadata["volume_confirmation"] == "hybrid"


def test_score_trend_pullback_rejects_weak_h4_regime() -> None:
    settings = build_settings()
    settings = Settings(**{**settings.__dict__, "trend_min_strength_atr": 3.0})

    h4_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=260, freq="4h", tz="UTC")
    h4_rows = []
    for index, timestamp in enumerate(h4_times):
        close = 3000 + index * 0.2
        h4_rows.append({"time": timestamp, "open": close - 0.2, "high": close + 0.4, "low": close - 0.4, "close": close, "volume": 100})
    df_h4 = pd.DataFrame(h4_rows)

    h1_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=120, freq="h", tz="UTC")
    h1_rows = []
    for index, timestamp in enumerate(h1_times):
        close = 3050 + index * 0.05
        open_price = close + 0.4 if index == 118 else close - 0.2
        final_close = close + 1.4 if index == 119 else close
        high = max(open_price, final_close) + 0.6
        low = min(open_price, final_close) - 0.8
        h1_rows.append({"time": timestamp, "open": open_price, "high": high, "low": low, "close": final_close, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    assert score_trend_pullback(settings, df_h1, df_h4, build_usd_proxy_frames("weak")) is None


def test_score_trend_pullback_rejects_long_when_usd_is_strong() -> None:
    settings = build_settings()
    settings = Settings(**{**settings.__dict__, "trend_min_strength_atr": 0.5, "trend_min_slope_atr": 0.02, "trend_h1_confirm_ema_period": 20, "trend_pullback_atr_tolerance": 1.5})

    h4_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=260, freq="4h", tz="UTC")
    h4_rows = []
    for index, timestamp in enumerate(h4_times):
        close = 2800 + index * 1.5
        if index > 250:
            close -= (260 - index) * 2.0
        h4_rows.append({"time": timestamp, "open": close - 1.0, "high": close + 2.0, "low": close - 2.0, "close": close, "volume": 100})
    df_h4 = pd.DataFrame(h4_rows)

    h1_times = pd.date_range(end=datetime(2026, 4, 6, tzinfo=timezone.utc), periods=120, freq="h", tz="UTC")
    h1_rows = []
    for index, timestamp in enumerate(h1_times):
        close = 3170 - abs(60 - index) * 0.3
        open_price = close + 0.4 if index == 118 else close - 0.2
        final_close = close + 3.4 if index == 119 else close
        high = max(open_price, final_close) + 0.6
        low = min(open_price, final_close) - 0.8
        h1_rows.append({"time": timestamp, "open": open_price, "high": high, "low": low, "close": final_close, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    assert score_trend_pullback(settings, df_h1, df_h4, build_usd_proxy_frames("strong")) is None


def build_usd_proxy_frames(mode: str) -> dict[str, pd.DataFrame]:
    end = datetime(2026, 4, 6, tzinfo=timezone.utc)
    times = pd.date_range(end=end, periods=140, freq="4h", tz="UTC")
    specs = {
        "EUR_USD": (1.18, -0.0012 if mode == "strong" else 0.0012),
        "GBP_USD": (1.34, -0.0010 if mode == "strong" else 0.0010),
        "USD_JPY": (148.0, 0.18 if mode == "strong" else -0.18),
    }
    frames: dict[str, pd.DataFrame] = {}
    for instrument, (base, step) in specs.items():
        rows = []
        for index, timestamp in enumerate(times):
            close = base + index * step
            rows.append({"time": timestamp, "open": close - abs(step), "high": close + abs(step) * 2, "low": close - abs(step) * 2, "close": close, "volume": 100})
        frames[instrument] = pd.DataFrame(rows)
    return frames