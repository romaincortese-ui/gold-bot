from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from goldbot.backtest_config import GoldBacktestConfig
from goldbot.backtest_data import GoldHistoricalDataProvider
from goldbot.backtest_microstructure import (
    SpreadModel,
    exit_slippage_cost,
    financing_charge,
    hours_between,
    is_weekend_gap_boundary,
    parse_event_times,
    weekend_gap_adjusted_stop,
)
from goldbot.config import Settings
from goldbot.indicators import calc_atr, calc_ema
from goldbot.marketdata import OandaClient
from goldbot.models import CalendarEvent, Opportunity
from goldbot.backtest_reporter import build_report
from goldbot.real_yields import apply_real_yield_overlay, build_real_yield_signal, fetch_real_yield_history
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
        self._spread_model: SpreadModel | None = (
            SpreadModel(
                base_spread=float(self.config.simulated_spread),
                news_window_minutes=int(getattr(self.settings, "backtest_spread_news_window_minutes", 2)),
                news_multiplier=float(getattr(self.settings, "backtest_spread_news_multiplier", 6.0)),
            )
            if getattr(self.settings, "backtest_spread_model_enabled", False)
            else None
        )

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        frames = self.provider.load_frames(self.config, self.settings.instrument)
        usd_proxy_frames = self.provider.load_aux_h4_frames(self.config, self.USD_PROXY_INSTRUMENTS) if self.settings.usd_regime_filter_enabled else {}
        events = self.provider.load_events(self.config.event_file)
        real_yield_frame = self._load_real_yield_frame()
        h1_times = [timestamp for timestamp in frames["H1"]["time"] if self.config.start <= timestamp <= self.config.end]
        if not h1_times:
            raise RuntimeError("No H1 candles available inside the requested backtest window")

        balance = float(self.config.initial_balance)
        equity_curve: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        open_trade: dict[str, Any] | None = None
        cooldowns: dict[tuple[str, str], datetime] = {}
        last_checkpoint = frames["M15"]["time"].iloc[0]
        previous_h1_timestamp: datetime | None = None

        for timestamp in h1_times:
            # 3.4: weekend gap. If a trade is still open across the Friday→Monday
            # boundary and the Monday open gaps through the stop, fill at the
            # gap-open price (not the stop price). Symmetric for shorts.
            if (
                open_trade is not None
                and getattr(self.settings, "backtest_weekend_gap_enabled", False)
                and previous_h1_timestamp is not None
                and is_weekend_gap_boundary(previous_h1_timestamp, timestamp)
            ):
                first_bar = self._first_m15_after(frames["M15"], previous_h1_timestamp, timestamp)
                if first_bar is not None:
                    monday_open = float(first_bar["open"])
                    stopped, fill_price = weekend_gap_adjusted_stop(
                        direction=str(open_trade["direction"]),
                        stop_price=float(open_trade["stop_price"]),
                        monday_open_price=monday_open,
                        weekend_was_crossed=True,
                    )
                    if stopped:
                        closed = self._close_trade(open_trade, exit_price=fill_price, exit_time=first_bar["time"], reason="WEEKEND_GAP_STOP")
                        trades.append(closed)
                        balance += float(closed["pnl"])
                        open_trade = None
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
            previous_h1_timestamp = timestamp

            if open_trade is not None:
                continue

            session_name = self._session_name(timestamp)
            if session_name in {"ASIA", "OFF_HOURS"}:
                continue

            opportunity = self._score_at_time(timestamp, frames, usd_proxy_frames, events, session_name, real_yield_frame)
            if opportunity is None:
                continue
            if self._is_cooldown_active(cooldowns, opportunity, timestamp):
                continue

            risk_multiplier = float(opportunity.metadata.get("risk_multiplier", 1.0) or 1.0)
            risk_amount = balance * self.settings.gold_budget_allocation * self.settings.max_risk_per_trade * risk_multiplier
            size = self.client.calculate_xau_size(risk_amount, opportunity.risk_per_unit, account_currency="USD")
            if size <= 0:
                continue

            entry_price = self._entry_price(opportunity, now=timestamp, events=events)
            open_trade = {
                "instrument": self.settings.instrument,
                "strategy": opportunity.strategy,
                "direction": opportunity.direction,
                "entry_signal": opportunity.strategy,
                "entry_time": timestamp.isoformat(),
                "entry_price": entry_price,
                "entry_spread": self._effective_spread(now=timestamp, events=events),
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

    def run_walk_forward(self, *, train_days: int, test_days: int, step_days: int | None = None) -> dict[str, Any]:
        if train_days <= 0 or test_days <= 0:
            return {"windows_evaluated": 0, "windows": [], "aggregate_test_report": build_report([], [])}

        windows: list[dict[str, Any]] = []
        aggregate_test_trades: list[dict[str, Any]] = []
        step = test_days if step_days is None or step_days <= 0 else step_days
        cursor = self.config.start.astimezone(timezone.utc)
        deadline = self.config.end.astimezone(timezone.utc)

        while True:
            train_start = cursor
            train_end = train_start + timedelta(days=train_days)
            test_start = train_end
            test_end = test_start + timedelta(days=test_days)
            if test_end > deadline:
                break

            train_config = replace(self.config, start=train_start, end=train_end)
            test_config = replace(self.config, start=test_start, end=test_end)
            train_engine = self.__class__(self.settings, train_config, self.provider)
            test_engine = self.__class__(self.settings, test_config, self.provider)
            _, train_trades = train_engine.run()
            _, test_trades = test_engine.run()
            train_report = build_report([], train_trades)
            test_report = build_report([], test_trades)
            windows.append(
                {
                    "train_start": train_start.isoformat(),
                    "train_end": train_end.isoformat(),
                    "test_start": test_start.isoformat(),
                    "test_end": test_end.isoformat(),
                    "train_report": train_report,
                    "test_report": test_report,
                }
            )
            aggregate_test_trades.extend(test_trades)
            cursor = cursor + timedelta(days=step)

        return {
            "windows_evaluated": len(windows),
            "windows": windows,
            "aggregate_test_report": build_report([], aggregate_test_trades),
        }

    def _score_at_time(
        self,
        timestamp: datetime,
        frames: dict[str, pd.DataFrame],
        usd_proxy_frames: dict[str, pd.DataFrame],
        events: list[CalendarEvent],
        session_name: str,
        real_yield_frame: pd.DataFrame | None,
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
        real_yield_signal = build_real_yield_signal(real_yield_frame, timestamp, self.settings.real_yield_lookback_days)
        return select_best_opportunity(
            [
                filtered
                for item in opportunities
                if item is not None
                for filtered in [apply_real_yield_overlay(self.settings, item, real_yield_signal)]
                if filtered is not None
            ]
        )

    def _load_real_yield_frame(self) -> pd.DataFrame | None:
        if not self.settings.real_yield_filter_enabled:
            return None
        try:
            return fetch_real_yield_history(
                self.config.start - timedelta(days=max(self.config.warmup_days, self.settings.real_yield_lookback_days + 10)),
                self.config.end,
                cache_dir=self.config.cache_dir,
            )
        except Exception:
            return None

    def _advance_trade(
        self,
        trade: dict[str, Any],
        bar: pd.Series,
        frames: dict[str, pd.DataFrame],
        closed_trades: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if self._stop_hit(trade, bar):
            stop_price = float(trade["stop_price"])
            entry_price = float(trade["entry_price"])
            if trade["direction"] == "LONG" and stop_price > entry_price:
                reason = "TRAILING_STOP"
            elif trade["direction"] == "SHORT" and stop_price < entry_price:
                reason = "TRAILING_STOP"
            else:
                reason = "STOP_LOSS"
            closed = self._close_trade(trade, exit_price=stop_price, exit_time=bar["time"], reason=reason)
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

        # 3.4 microstructure costs ------------------------------------------
        slippage_cost = 0.0
        financing_cost = 0.0
        if reason in {"STOP_LOSS", "WEEKEND_GAP_STOP"} and getattr(self.settings, "backtest_spread_model_enabled", False):
            half_spread = float(trade.get("entry_spread", self.config.simulated_spread)) / 2.0
            slip = exit_slippage_cost(
                half_spread=half_spread,
                slippage_multiplier=float(getattr(self.settings, "backtest_exit_slippage_multiplier", 1.5)),
            )
            slippage_cost = slip * float(trade.get("remaining_size", 0.0))
            pnl -= slippage_cost
        if getattr(self.settings, "backtest_financing_enabled", False):
            entry_time = self._parse_iso(trade.get("entry_time"))
            hours_held = hours_between(entry_time, exit_time) if entry_time is not None else 0.0
            notional = float(trade["entry_price"]) * float(trade["size"])
            financing_cost = financing_charge(
                direction=str(trade["direction"]),
                notional=notional,
                hours_held=hours_held,
                long_apr=float(getattr(self.settings, "backtest_financing_long_apr", 0.05)),
                short_apr=float(getattr(self.settings, "backtest_financing_short_apr", 0.0)),
            )
            pnl -= financing_cost

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
            "slippage_cost": round(slippage_cost, 4),
            "financing_cost": round(financing_cost, 4),
        }

    @staticmethod
    def _parse_iso(text: object) -> datetime | None:
        if isinstance(text, datetime):
            return text if text.tzinfo else text.replace(tzinfo=timezone.utc)
        if not isinstance(text, str) or not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _first_m15_after(m15: pd.DataFrame, prev_ts: datetime, current_ts: datetime) -> pd.Series | None:
        window = m15[(m15["time"] > prev_ts) & (m15["time"] <= current_ts)]
        if window.empty:
            return None
        return window.iloc[0]

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

    def _entry_price(self, opportunity: Opportunity, *, now: datetime | None = None, events: list[CalendarEvent] | None = None) -> float:
        spread = self._effective_spread(now=now, events=events)
        half_spread = spread / 2.0
        if opportunity.direction == "LONG":
            return round(float(opportunity.entry_price) + half_spread, 3)
        return round(float(opportunity.entry_price) - half_spread, 3)

    def _effective_spread(self, *, now: datetime | None, events: list[CalendarEvent] | None) -> float:
        if self._spread_model is None or now is None:
            return float(self.config.simulated_spread)
        event_times = parse_event_times(events or [])
        return self._spread_model.effective_spread(now, event_times)

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