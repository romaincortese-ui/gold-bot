from goldbot.budget import SharedBudgetManager
from goldbot.config import Settings


def build_settings(tmp_path) -> Settings:
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
        state_file=str(tmp_path / "state.json"),
        shared_budget_file=str(tmp_path / "shared.json"),
        macro_state_file=str(tmp_path / "macro.json"),
        news_cache_file=str(tmp_path / "cache.json"),
        news_urls=["https://example.com"],
    )


def test_budget_manager_tracks_reserved_gold_risk(tmp_path) -> None:
    manager = SharedBudgetManager(build_settings(tmp_path))
    snapshot = manager.build_snapshot(10_000)
    assert snapshot.gold_sleeve_balance == 5_000
    assert snapshot.available_gold_risk == 150

    manager.reserve_gold_risk("trade-1", 50, "TREND_PULLBACK")
    updated = manager.build_snapshot(10_000)
    assert updated.reserved_gold_risk == 50
    assert updated.available_gold_risk == 100

    manager.release_gold_risk("trade-1")
    final = manager.build_snapshot(10_000)
    assert final.reserved_gold_risk == 0