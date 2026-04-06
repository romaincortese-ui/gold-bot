from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from goldbot.backtest_config import GoldBacktestConfig
from goldbot.backtest_data import GoldHistoricalDataProvider
from goldbot.config import Settings
from goldbot.indicators import calc_atr, calc_ema
from goldbot.marketdata import OandaClient
from goldbot.models import CalendarEvent, Opportunity
from goldbot.strategies import (
    score_exhaustion_reversal,
    score_macro_breakout,
    score_trend_pullback,
    select_best_opportunity,
)


class GoldBacktestEngine:
    USD_PROXY_INSTRUMENTS = ["EUR_USD", "GBP_USD", "USD_JPY"]

    def __init__(self, settings: Settings, config: GoldBacktestConfig, provider: GoldHistoricalDataProvider) -> None:
        self.settings = settings
        self.config = config
        self.provider = provider
        self.client = OandaClient(settings)

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        frames = self.provider.load_frames(self.config, self.settings.instrument)
        usd_proxy_frames = self.provider.load_aux_h4_frames(self.config, self.USD_PROXY_INSTRUMENTS) if self.settings.usd_regime_filter_enabled else {}
        events = self.provider.load_events(self.config.event_file)
        h1_times = [timestamp for timestamp in frames["H1"]["time"] if self.config.start <= timestamp <= self.config.end]
        if not h1_times:
            raise RuntimeError("No H1 candles available inside the requested backtest window")

        balance = float(self.config.initial_balance)
        equity_curve: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        open_trade: dict[str, Any] | None = None
        cooldowns: dict[tuple[str, str], datetime] = {}
        last_checkpoint = frames["M15"]["time"].iloc[0]

        for timestamp in h1_times:
            m15_window = frames["M15"][(frames["M15"]["time"] > last_checkpoint) & (frames["M15"]["time"] <= timestamp)]
            for _, bar in m15_window.iterrows():
                if open_trade is not None:
                    closed = self._advance_trade(open_trade, bar, frames, trades)
                    if closed is not None:
                        balance += float(closed["pnl"])
                        if str(closed.get("exit_reason")) == "STOP_LOSS":
                            self._register_stopout_cooldown(cooldowns, open_trade, bar["time"])
                        open_trade = None
                mark_price = float(bar["close"])
                equity_curve.append({"time": bar["time"].isoformat(), "equity": round(balance + self._unrealized_pnl(open_trade, mark_price), 4)})
            last_checkpoint = timestamp

            if open_trade is not None:
                continue

            session_name = self._session_name(timestamp)
            if session_name in {"ASIA", "OFF_HOURS"}:
                continue

            opportunity = self._score_at_time(timestamp, frames, usd_proxy_frames, events, session_name)
            if opportunity is None:
                continue
            if self._is_cooldown_active(cooldowns, opportunity, timestamp):
                continue

            risk_amount = balance * self.settings.gold_budget_allocation * self.settings.max_risk_per_trade
            size = self.client.calculate_xau_size(risk_amount, opportunity.risk_per_unit, account_currency="USD")
            if size <= 0:
                continue

            entry_price = self._entry_price(opportunity)
            open_trade = {
                "instrument": self.settings.instrument,
                "strategy": opportunity.strategy,
                "direction": opportunity.direction,
                "entry_signal": opportunity.strategy,
                "entry_time": timestamp.isoformat(),
                "entry_price": entry_price,
                "stop_price": float(opportunity.stop_price),
                "initial_stop_price": float(opportunity.stop_price),
                "initial_risk_per_unit": float(opportunity.risk_per_unit),
                "size": float(size),
                "remaining_size": float(size),
                "risk_amount": float(risk_amount),
                "exit_plan": dict(opportunity.exit_plan),
                "partial_taken": False,
                "break_even_moved": False,
                "metadata": dict(opportunity.metadata),
            }

        if open_trade is not None:
            last_price = float(frames["M15"]["close"].iloc[-1])
            final_trade = self._close_trade(open_trade, exit_price=last_price, exit_time=self.config.end, reason="END_OF_TEST")
            trades.append(final_trade)
            balance += float(final_trade["pnl"])
            equity_curve.append({"time": self.config.end.isoformat(), "equity": round(balance, 4)})

        return equity_curve, trades

    def _score_at_time(
        self,
        timestamp: datetime,
        frames: dict[str, pd.DataFrame],
        usd_proxy_frames: dict[str, pd.DataFrame],
        events: list[CalendarEvent],
        session_name: str,
    ) -> Opportunity | None:
        df_m15 = frames["M15"][frames["M15"]["time"] <= timestamp].tail(160)
        df_h1 = frames["H1"][frames["H1"]["time"] <= timestamp].tail(240)
        df_h4 = frames["H4"][frames["H4"]["time"] <= timestamp].tail(260)
        df_d1 = frames["D"][frames["D"]["time"] <= timestamp].tail(180)
        usd_h4 = {
            instrument: frame[frame["time"] <= timestamp].tail(120)
            for instrument, frame in usd_proxy_frames.items()
        }

        opportunities = [
            score_macro_breakout(self.settings, timestamp, session_name, df_m15, df_h1, events),
            score_exhaustion_reversal(self.settings, df_h4, df_d1),
            score_trend_pullback(self.settings, df_h1, df_h4, usd_h4),
        ]
        return select_best_opportunity([item for item in opportunities if item is not None])

    def _advance_trade(
        self,
        trade: dict[str, Any],
        bar: pd.Series,
        frames: dict[str, pd.DataFrame],
        closed_trades: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if self._stop_hit(trade, bar):
            closed = self._close_trade(trade, exit_price=float(trade["stop_price"]), exit_time=bar["time"], reason="STOP_LOSS")
            closed_trades.append(closed)
            return closed

        exit_plan = dict(trade.get("exit_plan", {}))
        if exit_plan:
            if (not trade.get("partial_taken")) and self._level_hit(trade["direction"], bar, float(exit_plan["partial_take_profit_price"])):
                partial_size = max(0.1, float(trade["remaining_size"]) * float(exit_plan.get("partial_take_profit_fraction", 0.5)))
                trade["realized_partial_pnl"] = float(trade.get("realized_partial_pnl", 0.0)) + self._price_pnl(trade["direction"], trade["entry_price"], float(exit_plan["partial_take_profit_price"]), partial_size)
                trade["remaining_size"] = max(0.0, float(trade["remaining_size"]) - partial_size)
                trade["partial_taken"] = True

            if (not trade.get("break_even_moved")) and self._level_hit(trade["direction"], bar, float(exit_plan["break_even_trigger_price"])):
                trade["stop_price"] = float(trade["entry_price"])
                trade["break_even_moved"] = True

            trailing_stop = self._compute_trailing_stop(trade, bar["time"], frames)
            if trailing_stop is not None:
                if trade["direction"] == "LONG" and trailing_stop > float(trade["stop_price"]):
                    trade["stop_price"] = round(trailing_stop, 3)
                if trade["direction"] == "SHORT" and trailing_stop < float(trade["stop_price"]):
                    trade["stop_price"] = round(trailing_stop, 3)

        return None

    def _compute_trailing_stop(self, trade: dict[str, Any], timestamp: datetime, frames: dict[str, pd.DataFrame]) -> float | None:
        exit_plan = trade.get("exit_plan", {})
        timeframe = str(exit_plan.get("trail_timeframe", "H1"))
        candles = frames.get(timeframe)
        if candles is None:
            return None
        window = candles[candles["time"] <= timestamp]
        ema_period = int(exit_plan.get("trail_ema_period", self.settings.trailing_ema_period))
        if len(window) < ema_period:
            return None
        ema = calc_ema(window["close"], ema_period)
        atr = calc_atr(window, self.settings.atr_period)
        distance = max(float(exit_plan.get("trailing_stop_distance", 0.0)), atr * float(exit_plan.get("trail_atr_mult", self.settings.trailing_atr_mult)))
        ema_value = float(ema.iloc[-1])
        return ema_value - distance if trade["direction"] == "LONG" else ema_value + distance

    def _close_trade(self, trade: dict[str, Any], *, exit_price: float, exit_time: datetime, reason: str) -> dict[str, Any]:
        remaining_pnl = self._price_pnl(trade["direction"], float(trade["entry_price"]), exit_price, float(trade["remaining_size"]))
        pnl = float(trade.get("realized_partial_pnl", 0.0)) + remaining_pnl
        risk_amount = max(float(trade.get("risk_amount", 0.0)), 1e-9)
        return {
            "instrument": trade["instrument"],
            "symbol": trade["instrument"],
            "strategy": trade["strategy"],
            "entry_signal": trade["entry_signal"],
            "direction": trade["direction"],
            "entry_time": trade["entry_time"],
            "exit_time": exit_time.isoformat(),
            "entry_price": float(trade["entry_price"]),
            "exit_price": float(exit_price),
            "size": float(trade["size"]),
            "remaining_size": 0.0,
            "risk_amount": risk_amount,
            "pnl": round(pnl, 4),
            "pnl_pct": round((pnl / risk_amount) * 100.0, 4),
            "exit_reason": reason,
        }

    def _is_cooldown_active(self, cooldowns: dict[tuple[str, str], datetime], opportunity: Opportunity, now: datetime) -> bool:
        key = (opportunity.strategy, opportunity.direction)
        expires_at = cooldowns.get(key)
        if expires_at is None:
            return False
        if now >= expires_at:
            cooldowns.pop(key, None)
            return False
        return True

    def _register_stopout_cooldown(self, cooldowns: dict[tuple[str, str], datetime], trade: dict[str, Any], exit_time: datetime) -> None:
        hours = self.settings.trend_stopout_cooldown_hours
        if hours <= 0:
            return
        cooldowns[(str(trade["strategy"]), str(trade["direction"]))] = exit_time.astimezone(timezone.utc) + timedelta(hours=hours)

    def _entry_price(self, opportunity: Opportunity) -> float:
        half_spread = self.config.simulated_spread / 2.0
        if opportunity.direction == "LONG":
            return round(float(opportunity.entry_price) + half_spread, 3)
        return round(float(opportunity.entry_price) - half_spread, 3)

    def _unrealized_pnl(self, trade: dict[str, Any] | None, mark_price: float) -> float:
        if trade is None:
            return 0.0
        return self._price_pnl(trade["direction"], float(trade["entry_price"]), mark_price, float(trade["remaining_size"])) + float(trade.get("realized_partial_pnl", 0.0))

    @staticmethod
    def _price_pnl(direction: str, entry_price: float, exit_price: float, size: float) -> float:
        move = exit_price - entry_price if direction == "LONG" else entry_price - exit_price
        return move * size

    @staticmethod
    def _level_hit(direction: str, bar: pd.Series, level: float) -> bool:
        if direction == "LONG":
            return float(bar["high"]) >= level
        return float(bar["low"]) <= level

    @staticmethod
    def _stop_hit(trade: dict[str, Any], bar: pd.Series) -> bool:
        stop_price = float(trade["stop_price"])
        if trade["direction"] == "LONG":
            return float(bar["low"]) <= stop_price
        return float(bar["high"]) >= stop_price

    def _session_name(self, now: datetime) -> str:
        hour = now.hour
        if self.settings.overlap_start_utc <= hour < self.settings.overlap_end_utc:
            return "OVERLAP"
        if self.settings.london_open_utc <= hour < self.settings.london_close_utc:
            return "LONDON"
        if self.settings.ny_open_utc <= hour < self.settings.ny_close_utc:
            return "NEW_YORK"
        if 0 <= hour < self.settings.london_open_utc:
            return "ASIA"
        return "OFF_HOURS"