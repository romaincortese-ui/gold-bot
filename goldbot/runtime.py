import json
import logging
import os
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from goldbot.indicators import calc_atr, calc_ema

from goldbot.budget import SharedBudgetManager
from goldbot.config import load_settings
from goldbot.marketdata import OandaClient, SpreadTooWideError
from goldbot.news import fetch_calendar_events, filter_gold_events
from goldbot.shared_backend import load_json_payload, publish_runtime_status, save_json_payload
from goldbot.strategies import (
    score_exhaustion_reversal,
    score_macro_breakout,
    score_trend_pullback,
    select_best_opportunity,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
)
log = logging.getLogger(__name__)


class GoldBotRuntime:
    def __init__(self) -> None:
        self.settings = load_settings()
        self.client = OandaClient(self.settings)
        self.budget = SharedBudgetManager(self.settings)
        self.state_path = Path(self.settings.state_file)
        self.state_key = os.getenv("GOLD_RUNTIME_STATE_KEY", "gold_runtime_state").strip()
        self.status_key = os.getenv("GOLD_BOT_STATUS_KEY", "gold_bot_runtime_status").strip()
        self.status_ttl = int(os.getenv("GOLD_STATUS_TTL", "1800"))

    def run_forever(self) -> None:
        while True:
            try:
                self.run_cycle()
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.exception("Gold-bot cycle failed: %s", exc)
            time.sleep(self.settings.poll_interval_seconds)

    def run_cycle(self) -> dict | None:
        now = datetime.now(timezone.utc)
        session_name = self._session_name(now)
        state = self._load_state()

        self._process_control_requests(state)
        self._manage_open_trades(state)

        if state.get("paused", False):
            log.info("Gold-bot is paused; managing open trades only")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "paused_manual"})
            self._save_state(state)
            self._publish_runtime_status("paused", state, balance=None)
            return None

        if session_name == "ASIA" or session_name == "OFF_HOURS":
            log.info("Skipping scan during %s session", session_name)
            state.update({"last_run_at": now.isoformat(), "last_session": session_name})
            self._save_state(state)
            self._publish_runtime_status("idle", state, balance=None)
            return None

        account = self.client.get_account_summary()
        balance = float(account["balance"])
        account_currency = str(account["currency"])
        snapshot = self.budget.build_snapshot(balance)
        state["account_balance"] = balance
        state["account_currency"] = account_currency

        open_gold_trades = list(state.get("open_trades", []))
        if len(open_gold_trades) >= self.settings.max_open_gold_trades:
            log.info("Skipping scan because %s open gold trade(s) already exist", len(open_gold_trades))
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "open_gold_position"})
            self._save_state(state)
            self._publish_runtime_status("active_trade", state, balance=balance)
            return None

        events = fetch_calendar_events(self.settings.news_urls, self.settings.news_cache_file)
        relevant_events = filter_gold_events(
            events,
            now=now,
            lookback_hours=self.settings.breakout_news_lookback_hours,
            lookahead_hours=self.settings.breakout_news_lookahead_hours,
        )

        if any(0 <= (event.occurs_at - now).total_seconds() <= self.settings.pre_news_pause_minutes * 60 for event in relevant_events):
            log.info("Skipping scan inside pre-news pause window")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "pre_news_pause"})
            self._save_state(state)
            self._publish_runtime_status("pre_news_pause", state, balance=balance)
            return None

        df_m15 = self.client.fetch_candles(self.settings.instrument, "M15", 160)
        df_h1 = self.client.fetch_candles(self.settings.instrument, "H1", 240)
        df_h4 = self.client.fetch_candles(self.settings.instrument, "H4", 260)
        df_d1 = self.client.fetch_candles(self.settings.instrument, "D", 180)
        if any(frame is None for frame in (df_m15, df_h1, df_h4, df_d1)):
            log.info("Skipping scan because candle history is incomplete")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "missing_candles"})
            self._save_state(state)
            self._publish_runtime_status("waiting_data", state, balance=balance)
            return None

        opportunities = [
            score_macro_breakout(self.settings, now, session_name, df_m15, df_h1, relevant_events),
            score_exhaustion_reversal(self.settings, df_h4, df_d1),
            score_trend_pullback(self.settings, df_h1, df_h4),
        ]
        best = select_best_opportunity([opportunity for opportunity in opportunities if opportunity is not None])
        if best is None:
            log.info("No XAU/USD opportunity passed filters")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "no_signal"})
            self._save_state(state)
            self._publish_runtime_status("scanning", state, balance=balance)
            return None

        risk_amount = min(snapshot.max_trade_risk_amount, snapshot.available_gold_risk)
        if risk_amount <= 0:
            log.info("No gold risk budget available")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "risk_budget_exhausted"})
            self._save_state(state)
            self._publish_runtime_status("risk_budget_exhausted", state, balance=balance)
            return None

        size = self.client.calculate_xau_size(risk_amount, best.risk_per_unit, account_currency)
        if size <= 0:
            log.info("Unable to size XAU/USD trade")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "size_zero"})
            self._save_state(state)
            self._publish_runtime_status("size_zero", state, balance=balance)
            return None

        try:
            result = self.client.place_market_order(best, size)
        except SpreadTooWideError as exc:
            log.info("Skipping execution because spread is too wide: %s", exc)
            self._record_event(state, "spread_too_wide", str(exc), now=now)
            self._write_state(
                {
                    "last_run_at": now.isoformat(),
                    "last_session": session_name,
                    "skip_reason": "spread_too_wide",
                    "last_signal": {
                        **asdict(best),
                        "risk_amount": risk_amount,
                        "size": size,
                        "budget_snapshot": asdict(snapshot),
                    },
                }
            )
            self._publish_runtime_status("spread_too_wide", self._load_state(), balance=balance)
            return None
        self.budget.reserve_gold_risk(str(result["id"]), risk_amount, best.strategy)
        log.info(
            "Opened %s %s via %s | size=%s | risk=%.2f | mode=%s",
            best.direction,
            self.settings.instrument,
            best.strategy,
            size,
            risk_amount,
            result.get("mode", self.settings.execution_mode),
        )
        self._record_event(
            state,
            "trade_opened",
            f"{best.strategy} {best.direction} opened at {result.get('price', best.entry_price)} | size {size} | mode {result.get('mode', self.settings.execution_mode)}",
            now=now,
        )
        trade_record = self._build_trade_record(best, result, size, risk_amount, now)
        state.setdefault("open_trades", []).append(trade_record)
        state.setdefault("signals", []).append({
            "opened_at": now.isoformat(),
            "strategy": best.strategy,
            "direction": best.direction,
            "trade_id": str(result["id"]),
        })
        state.update(
            {
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": None,
                "last_signal": {
                    **asdict(best),
                    "result": result,
                    "risk_amount": risk_amount,
                    "size": size,
                    "budget_snapshot": asdict(snapshot),
                },
            }
        )
        self._save_state(state)
        self._publish_runtime_status("trade_opened", state, balance=balance)
        return result

    def _build_trade_record(self, opportunity: dict, result: dict, size: float, risk_amount: float, opened_at: datetime) -> dict:
        return {
            "id": str(result["id"]),
            "instrument": self.settings.instrument,
            "strategy": opportunity.strategy,
            "direction": opportunity.direction,
            "entry_price": float(result.get("price", opportunity.entry_price) or opportunity.entry_price),
            "stop_price": float(opportunity.stop_price),
            "initial_stop_price": float(opportunity.stop_price),
            "initial_risk_per_unit": float(opportunity.risk_per_unit),
            "size": float(size),
            "remaining_size": float(size),
            "risk_amount": float(risk_amount),
            "exit_plan": dict(opportunity.exit_plan),
            "partial_taken": False,
            "break_even_moved": False,
            "opened_at": opened_at.isoformat(),
        }

    def _manage_open_trades(self, state: dict) -> None:
        open_trades = list(state.get("open_trades", []))
        if not open_trades:
            return

        live_trade_ids = None
        if self.settings.execution_mode == "live":
            live_trade_ids = {str(trade.get("id")) for trade in self.client.list_open_trades()}

        managed: list[dict] = []
        for trade in open_trades:
            trade_id = str(trade.get("id", ""))
            if live_trade_ids is not None and trade_id and trade_id not in live_trade_ids:
                self._record_event(state, "trade_closed", f"Tracked trade {trade_id} is no longer open at broker", now=datetime.now(timezone.utc))
                self.budget.release_gold_risk(trade_id)
                continue
            if self._apply_exit_plan(trade, state):
                managed.append(trade)
            else:
                self.budget.release_gold_risk(trade_id)

        state["open_trades"] = managed
        self._save_state(state)

    def _process_control_requests(self, state: dict) -> None:
        requests = list(state.get("control_requests", []))
        if not requests:
            return

        remaining_requests: list[dict] = []
        for request in requests:
            command = str(request.get("command", "") or "").lower()
            if command == "pause":
                state["paused"] = True
                self._record_event(state, "manual_pause", "Gold-bot paused by control request", now=datetime.now(timezone.utc))
                continue
            if command == "resume":
                state["paused"] = False
                self._record_event(state, "manual_resume", "Gold-bot resumed by control request", now=datetime.now(timezone.utc))
                continue
            if command == "close_all":
                self._handle_close_all(state)
                continue
            if command == "sync":
                self._handle_sync(state)
                continue
            remaining_requests.append(request)

        state["control_requests"] = remaining_requests
        self._save_state(state)

    def _handle_close_all(self, state: dict) -> None:
        open_trades = list(state.get("open_trades", []))
        if not open_trades:
            self._record_event(state, "close_all", "Close-all requested but no open trades were tracked", now=datetime.now(timezone.utc))
            return

        remaining_trades: list[dict] = []
        for trade in open_trades:
            trade_id = str(trade.get("id", "") or "")
            if self.client.close_trade(trade_id):
                self._record_event(state, "trade_closed", f"Trade {trade_id} closed by /closeall command", now=datetime.now(timezone.utc))
                self.budget.release_gold_risk(trade_id)
            else:
                remaining_trades.append(trade)
        state["open_trades"] = remaining_trades

    def _handle_sync(self, state: dict) -> None:
        if self.settings.execution_mode != "live":
            self._record_event(state, "sync", "Sync requested in non-live mode; tracked state left unchanged", now=datetime.now(timezone.utc))
            return

        broker_trades = self.client.list_open_trades()
        existing_by_id = {str(trade.get("id", "") or ""): trade for trade in state.get("open_trades", [])}
        synced_trades: list[dict] = []
        broker_ids: set[str] = set()
        for broker_trade in broker_trades:
            normalized = self._build_trade_from_broker(broker_trade, existing_by_id.get(str(broker_trade.get("id", "") or "")))
            if normalized is None:
                continue
            trade_id = str(normalized.get("id", "") or "")
            broker_ids.add(trade_id)
            synced_trades.append(normalized)

        for trade_id, tracked_trade in existing_by_id.items():
            if trade_id and trade_id not in broker_ids:
                self.budget.release_gold_risk(trade_id)
                self._record_event(state, "trade_closed", f"Trade {trade_id} removed during broker sync", now=datetime.now(timezone.utc))

        state["open_trades"] = synced_trades
        self._record_event(state, "sync", f"Broker sync completed with {len(synced_trades)} tracked trade(s)", now=datetime.now(timezone.utc))

    def _build_trade_from_broker(self, broker_trade: dict, existing: dict | None = None) -> dict | None:
        try:
            trade_id = str(broker_trade.get("id", "") or "")
            current_units = float(broker_trade.get("currentUnits", broker_trade.get("initialUnits", 0)) or 0.0)
            entry_price = float(broker_trade.get("price", 0.0) or 0.0)
            if not trade_id or current_units == 0 or entry_price <= 0:
                return None

            trade = dict(existing or {})
            trade.update(
                {
                    "id": trade_id,
                    "instrument": self.settings.instrument,
                    "strategy": trade.get("strategy", "RESTORED"),
                    "direction": "LONG" if current_units > 0 else "SHORT",
                    "entry_price": entry_price,
                    "size": abs(current_units),
                    "remaining_size": abs(current_units),
                    "opened_at": broker_trade.get("openTime", trade.get("opened_at", datetime.now(timezone.utc).isoformat())),
                    "initial_stop_price": trade.get("initial_stop_price", trade.get("stop_price", entry_price)),
                    "stop_price": trade.get("stop_price", trade.get("initial_stop_price", entry_price)),
                    "risk_amount": float(trade.get("risk_amount", 0.0) or 0.0),
                    "exit_plan": dict(trade.get("exit_plan", {})),
                    "partial_taken": bool(trade.get("partial_taken", False)),
                    "break_even_moved": bool(trade.get("break_even_moved", False)),
                    "initial_risk_per_unit": float(trade.get("initial_risk_per_unit", 0.0) or 0.0),
                }
            )
            return trade
        except Exception:
            return None

    def _apply_exit_plan(self, trade: dict, state: dict) -> bool:
        quote = self.client.get_price(self.settings.instrument)
        current_price = quote["bid"] if trade["direction"] == "LONG" else quote["ask"]
        if current_price <= 0:
            return True

        if self._trade_reached_stop(trade, current_price):
            self._record_event(state, "trade_stopped", f"Trade {trade['id']} hit stop at {trade.get('stop_price')}", now=datetime.now(timezone.utc))
            return False

        exit_plan = dict(trade.get("exit_plan", {}))
        if not exit_plan:
            return True

        if (not trade.get("partial_taken")) and self._reached_level(trade["direction"], current_price, float(exit_plan["partial_take_profit_price"])):
            partial_size = max(0.1, float(trade.get("remaining_size", trade["size"])) * float(exit_plan.get("partial_take_profit_fraction", 0.5)))
            if self.client.close_trade(str(trade["id"]), size=partial_size):
                trade["partial_taken"] = True
                trade["remaining_size"] = max(0.0, float(trade.get("remaining_size", trade["size"])) - partial_size)
                self._record_event(state, "partial_profit", f"Trade {trade['id']} took partial profit, remaining size {trade['remaining_size']}", now=datetime.now(timezone.utc))

        if (not trade.get("break_even_moved")) and self._reached_level(trade["direction"], current_price, float(exit_plan["break_even_trigger_price"])):
            if self._tighten_stop(trade, float(trade["entry_price"])):
                trade["break_even_moved"] = True
                self._record_event(state, "break_even", f"Trade {trade['id']} stop moved to break-even at {trade['stop_price']}", now=datetime.now(timezone.utc))

        trailing_stop = self._compute_trailing_stop(trade)
        if trailing_stop is not None:
            previous_stop = float(trade.get("stop_price", 0.0) or 0.0)
            if self._tighten_stop(trade, trailing_stop):
                if float(trade.get("stop_price", 0.0) or 0.0) != previous_stop:
                    self._record_event(state, "trail_update", f"Trade {trade['id']} trailing stop tightened to {trade['stop_price']}", now=datetime.now(timezone.utc))

        if float(trade.get("remaining_size", 0.0)) <= 0:
            self._record_event(state, "trade_closed", f"Trade {trade['id']} fully closed after partial exits", now=datetime.now(timezone.utc))
            return False
        return True

    def _compute_trailing_stop(self, trade: dict) -> float | None:
        exit_plan = trade.get("exit_plan", {})
        timeframe = str(exit_plan.get("trail_timeframe", "H1"))
        candles = self.client.fetch_candles(self.settings.instrument, timeframe, 120)
        if candles is None or len(candles) < int(exit_plan.get("trail_ema_period", self.settings.trailing_ema_period)):
            return None
        ema = calc_ema(candles["close"], int(exit_plan.get("trail_ema_period", self.settings.trailing_ema_period)))
        atr = calc_atr(candles, self.settings.atr_period)
        distance = max(float(exit_plan.get("trailing_stop_distance", 0.0)), atr * float(exit_plan.get("trail_atr_mult", self.settings.trailing_atr_mult)))
        ema_value = float(ema.iloc[-1])
        if trade["direction"] == "LONG":
            return ema_value - distance
        return ema_value + distance

    def _tighten_stop(self, trade: dict, candidate_stop: float) -> bool:
        current_stop = float(trade.get("stop_price", trade.get("initial_stop_price", 0.0)) or 0.0)
        if trade["direction"] == "LONG":
            if candidate_stop <= current_stop:
                return False
        else:
            if current_stop and candidate_stop >= current_stop:
                return False
        if self.client.modify_trade(str(trade["id"]), stop_price=candidate_stop):
            trade["stop_price"] = round(candidate_stop, 3)
            return True
        return False

    @staticmethod
    def _reached_level(direction: str, current_price: float, target_price: float) -> bool:
        if direction == "LONG":
            return current_price >= target_price
        return current_price <= target_price

    @staticmethod
    def _trade_reached_stop(trade: dict, current_price: float) -> bool:
        stop_price = float(trade.get("stop_price", trade.get("initial_stop_price", 0.0)) or 0.0)
        if trade["direction"] == "LONG":
            return current_price <= stop_price
        return current_price >= stop_price

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

    def _write_state(self, update: dict) -> None:
        state = self._load_state()
        state.update(update)
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("events", [])
        state.setdefault("paused", False)
        save_json_payload(str(self.state_path), state, self.state_key)

    def _save_state(self, state: dict) -> None:
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("events", [])
        state.setdefault("control_requests", [])
        state.setdefault("paused", False)
        save_json_payload(str(self.state_path), state, self.state_key)

    def _load_state(self) -> dict:
        state = load_json_payload(str(self.state_path), self.state_key, {"signals": [], "open_trades": [], "events": []})
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("events", [])
        state.setdefault("control_requests", [])
        state.setdefault("paused", False)
        return state

    @staticmethod
    def _record_event(state: dict, event_type: str, message: str, *, now: datetime) -> None:
        events = state.setdefault("events", [])
        events.append(
            {
                "id": str(uuid.uuid4()),
                "timestamp": now.isoformat(),
                "type": event_type,
                "message": message,
            }
        )
        if len(events) > 200:
            del events[:-200]

    def _publish_runtime_status(self, state_name: str, state: dict, balance: float | None) -> None:
        publish_runtime_status(
            service="gold-bot",
            state=state_name,
            redis_key=self.status_key,
            ttl_seconds=self.status_ttl,
            balance=balance,
            paused=bool(state.get("paused", False)),
            open_trades=len(state.get("open_trades", [])),
            last_run_at=state.get("last_run_at"),
            last_session=state.get("last_session"),
            skip_reason=state.get("skip_reason"),
        )