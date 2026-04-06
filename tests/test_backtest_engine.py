from datetime import datetime, timezone

import pandas as pd

from goldbot.backtest_config import GoldBacktestConfig
from goldbot.backtest_engine import GoldBacktestEngine
from goldbot.config import Settings
from goldbot.models import Opportunity


class StubProvider:
    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.frames = frames

    def load_frames(self, config: GoldBacktestConfig, instrument: str) -> dict[str, pd.DataFrame]:
        return self.frames

    def load_aux_h4_frames(self, config: GoldBacktestConfig, instruments: list[str]) -> dict[str, pd.DataFrame]:
        base_frame = self.frames["H4"].tail(140).copy()
        aux: dict[str, pd.DataFrame] = {}
        for instrument in instruments:
            frame = base_frame.copy()
            if instrument in {"EUR_USD", "GBP_USD"}:
                frame["close"] = frame["close"] / 3000.0
                frame["open"] = frame["open"] / 3000.0
                frame["high"] = frame["high"] / 3000.0
                frame["low"] = frame["low"] / 3000.0
            else:
                frame["close"] = 140.0 + (frame.index * 0.05)
                frame["open"] = frame["close"] - 0.03
                frame["high"] = frame["close"] + 0.06
                frame["low"] = frame["close"] - 0.06
            aux[instrument] = frame.reset_index(drop=True)
        return aux

    def load_events(self, event_file: str):
        return []


class StubEngine(GoldBacktestEngine):
    def _score_at_time(self, timestamp, frames, usd_proxy_frames, events, session_name):
        if session_name == "ASIA":
            return None
        return Opportunity(
            strategy="TREND_PULLBACK",
            direction="LONG",
            score=80.0,
            entry_price=3180.0,
            stop_price=3176.0,
            take_profit_price=None,
            risk_per_unit=4.0,
            rationale="synthetic trend pullback",
            metadata={},
            exit_plan={
                "partial_take_profit_fraction": 0.5,
                "partial_take_profit_price": 3184.0,
                "break_even_trigger_price": 3184.0,
                "trail_timeframe": "H1",
                "trail_ema_period": 20,
                "trail_atr_mult": 2.2,
                "trailing_stop_distance": 4.0,
            },
        )


def build_settings() -> Settings:
    return Settings(
        instrument="XAU_USD",
        oanda_api_key="",
        oanda_account_id="",
        oanda_environment="practice",
        account_type="spread_bet",
        execution_mode="paper",
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
        news_urls=[],
    )


def build_frames() -> dict[str, pd.DataFrame]:
    end = datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc)

    h4_times = pd.date_range(end=end, periods=260, freq="4h", tz="UTC")
    h4_rows = []
    for index, timestamp in enumerate(h4_times):
        close = 2800 + index * 1.5
        if index > 250:
            close -= (260 - index) * 2.0
        h4_rows.append({"time": timestamp, "open": close - 1.0, "high": close + 2.0, "low": close - 2.0, "close": close, "volume": 100})
    df_h4 = pd.DataFrame(h4_rows)

    h1_times = pd.date_range(end=end, periods=260, freq="h", tz="UTC")
    h1_rows = []
    for index, timestamp in enumerate(h1_times):
        close = 3170 - abs(60 - (index % 120)) * 0.3
        open_price = close + 0.4 if index == len(h1_times) - 2 else close - 0.2
        final_close = close + 1.4 if index == len(h1_times) - 1 else close
        high = max(open_price, final_close) + 0.8
        low = min(open_price, final_close) - 0.8
        h1_rows.append({"time": timestamp, "open": open_price, "high": high, "low": low, "close": final_close, "volume": 100})
    df_h1 = pd.DataFrame(h1_rows)

    m15_times = pd.date_range(end=end, periods=520, freq="15min", tz="UTC")
    m15_rows = []
    for index, timestamp in enumerate(m15_times):
        close = 3168 + index * 0.08
        m15_rows.append({"time": timestamp, "open": close - 0.2, "high": close + 0.5, "low": close - 0.4, "close": close, "volume": 120})
    df_m15 = pd.DataFrame(m15_rows)

    d1_times = pd.date_range(end=end, periods=180, freq="D", tz="UTC")
    d1_rows = []
    for index, timestamp in enumerate(d1_times):
        close = 2750 + index * 2.0
        d1_rows.append({"time": timestamp, "open": close - 3.0, "high": close + 5.0, "low": close - 5.0, "close": close, "volume": 200})
    df_d1 = pd.DataFrame(d1_rows)

    return {"M15": df_m15, "H1": df_h1, "H4": df_h4, "D": df_d1}


def test_backtest_engine_runs_and_returns_equity_curve() -> None:
    settings = build_settings()
    config = GoldBacktestConfig(
        start=datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc),
        initial_balance=10_000.0,
        simulated_spread=0.0,
    )
    engine = StubEngine(settings, config, StubProvider(build_frames()))

    equity_curve, trades = engine.run()

    assert equity_curve
    assert trades
    assert trades[0]["strategy"] == "TREND_PULLBACK"
    assert "pnl" in trades[0]


def test_backtest_engine_stopout_cooldown_blocks_immediate_reentry() -> None:
    settings = build_settings()
    settings = Settings(**{**settings.__dict__, "trend_stopout_cooldown_hours": 999})
    config = GoldBacktestConfig(
        start=datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc),
        initial_balance=10_000.0,
        simulated_spread=0.0,
    )

    class CooldownEngine(StubEngine):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._forced_stop = False

        def _advance_trade(self, trade, bar, frames, closed_trades):
            if not self._forced_stop:
                self._forced_stop = True
                closed = self._close_trade(trade, exit_price=float(trade["stop_price"]), exit_time=bar["time"], reason="STOP_LOSS")
                closed_trades.append(closed)
                return closed
            return None

    engine = CooldownEngine(settings, config, StubProvider(build_frames()))

    equity_curve, trades = engine.run()

    assert equity_curve
    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "STOP_LOSS"