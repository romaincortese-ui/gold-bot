import json
import logging
import os
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from goldbot.indicators import calc_atr, calc_ema

from goldbot.budget import SharedBudgetManager
from goldbot.calibration import (
    CALIBRATION_FILE,
    CALIBRATION_MAX_AGE_HOURS,
    CALIBRATION_MIN_TRADES,
    CALIBRATION_REDIS_KEY,
    get_strategy_adjustment,
    load_calibration,
    validate_calibration,
)
from goldbot.config import load_settings
from goldbot.marketdata import OandaClient, SpreadTooWideError
from goldbot.models import Opportunity
from goldbot.news import fetch_calendar_events, filter_gold_events
from goldbot.news_scoring import load_event_scores
from goldbot.kill_switch import EquityHistory, evaluate_kill_switch, latch_halt_state
from goldbot.real_yields import apply_real_yield_overlay, load_real_yield_signal_from_macro_state
from goldbot.cftc import apply_cftc_overlay, load_cftc_signal_from_macro_state
from goldbot.shared_backend import load_json_payload, publish_runtime_status, save_json_payload
from goldbot.sizing import compute_risk_amount
from goldbot.spread_tracker import SpreadTracker
from goldbot.weekend_guard import (
    WeekendDecision,
    decision_to_metadata,
    evaluate_weekend,
    widened_stop_price,
)
from goldbot.strategies import (
    score_exhaustion_reversal,
    score_macro_breakout,
    score_trend_pullback,
    select_best_opportunity,
)
from goldbot.telegram import GoldTelegramClient
from goldbot.volume_oracle import load_breakout_volume_signal


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
)
log = logging.getLogger(__name__)


class GoldBotRuntime:
    USD_PROXY_INSTRUMENTS = ["EUR_USD", "GBP_USD", "USD_JPY"]

    def __init__(self) -> None:
        self.settings = load_settings()
        self.client = OandaClient(self.settings)
        self.budget = SharedBudgetManager(self.settings)
        self.state_path = Path(self.settings.state_file)
        self.state_key = os.getenv("GOLD_RUNTIME_STATE_KEY", "gold_runtime_state").strip()
        self.status_key = os.getenv("GOLD_BOT_STATUS_KEY", "gold_bot_runtime_status").strip()
        self.status_path = Path(os.getenv("GOLD_BOT_STATUS_FILE", str(self.state_path.with_name("gold_bot_runtime_status.json")))).expanduser()
        self.status_ttl = int(os.getenv("GOLD_STATUS_TTL", "1800"))
        self.telegram_token = os.getenv("GOLD_TELEGRAM_TOKEN", "").strip()
        self.telegram_chat_id = os.getenv("GOLD_TELEGRAM_CHAT_ID", "").strip()
        self.telegram_poll_seconds = max(1, int(os.getenv("GOLD_TELEGRAM_POLL_SECONDS", "5")))
        self.telegram_status_heartbeat_minutes = max(0, int(os.getenv("GOLD_TELEGRAM_HEARTBEAT_MINUTES", "60")))
        self.telegram_offset_path = Path(os.getenv("GOLD_TELEGRAM_OFFSET_FILE", "telegram_state.json"))
        self.heartbeat_interval = int(os.getenv("GOLD_HEARTBEAT_INTERVAL", "3600"))
        self.last_heartbeat_at = 0.0
        self.calibration_file = os.getenv("GOLD_CALIBRATION_FILE", CALIBRATION_FILE).strip()
        self.calibration_redis_key = os.getenv("GOLD_CALIBRATION_REDIS_KEY", CALIBRATION_REDIS_KEY).strip()
        self.calibration: dict | None = None
        self.telegram_client = self._build_telegram_client()
        # Sprint 1: rolling-median spread tracker fed by every quote fetch.
        self._spread_tracker = SpreadTracker(
            window_minutes=self.settings.adaptive_spread_window_minutes,
            multiplier=self.settings.adaptive_spread_multiplier,
            floor=self.settings.adaptive_spread_floor,
            min_samples=self.settings.adaptive_spread_min_samples,
            static_cap=self.settings.max_entry_spread,
        )

    def run_forever(self) -> None:
        bootstrap_state = self._load_state()
        bootstrap_state.setdefault("last_run_at", None)
        bootstrap_state.setdefault("last_session", None)
        bootstrap_state.setdefault("skip_reason", None)
        self._publish_runtime_status("booting", bootstrap_state, balance=bootstrap_state.get("account_balance"))
        self._announce_telegram_startup()
        next_cycle_at = time.monotonic()
        next_telegram_at = time.monotonic() if self.telegram_client is not None else float("inf")
        while True:
            try:
                now = time.monotonic()
                if now >= next_telegram_at:
                    self._service_telegram()
                    next_telegram_at = time.monotonic() + self.telegram_poll_seconds
                    now = time.monotonic()
                if now >= next_cycle_at:
                    self.run_cycle()
                    next_cycle_at = time.monotonic() + self.settings.poll_interval_seconds
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.exception("Gold-bot cycle failed: %s", exc)
                state = self._load_state()
                error_msg = self._sanitize_error(str(exc))
                self._record_event(state, "runtime_error", f"Cycle error: {error_msg}", now=datetime.now(timezone.utc))
                state["last_error"] = error_msg
                self._save_state(state)
                self._publish_runtime_status("error", state, balance=state.get("account_balance"))
                next_cycle_at = time.monotonic() + self.settings.poll_interval_seconds
            sleep_until = min(next_cycle_at, next_telegram_at)
            sleep_seconds = max(0.2, sleep_until - time.monotonic())
            time.sleep(min(sleep_seconds, float(self.telegram_poll_seconds)))

    def _build_telegram_client(self) -> GoldTelegramClient | None:
        if not self.telegram_token or not self.telegram_chat_id:
            return None
        return GoldTelegramClient(
            token=self.telegram_token,
            chat_id=self.telegram_chat_id,
            state_path=self.state_path,
            offset_path=self.telegram_offset_path,
        )

    def _refresh_calibration(self) -> None:
        try:
            data = load_calibration(file_path=self.calibration_file, redis_key=self.calibration_redis_key)
            if data is None:
                log.debug("No calibration data available")
                self.calibration = None
                return
            valid, reason = validate_calibration(data)
            if not valid:
                log.info("Calibration invalid: %s", reason)
                self.calibration = None
                return
            self.calibration = data
            log.info(
                "Loaded calibration: %d trades, pf=%.2f, wr=%.0f%%",
                data.get("total_trades", 0),
                data.get("profit_factor", 0),
                data.get("win_rate", 0) * 100,
            )
        except Exception:
            log.exception("Failed to load calibration")
            self.calibration = None

    def _announce_telegram_startup(self) -> None:
        if self.telegram_client is None:
            return
        try:
            self.telegram_client.announce_startup()
        except Exception as exc:
            log.exception("Gold Telegram startup announcement failed: %s", exc)

    def _service_telegram(self) -> None:
        if self.telegram_client is None:
            return
        try:
            self.telegram_client.service_once(heartbeat_minutes=self.telegram_status_heartbeat_minutes)
        except requests.exceptions.RequestException as exc:
            # Transient network failure to api.telegram.org -- drop it at
            # warning level instead of dumping a 70-line traceback every time
            # Telegram read-times-out. The next cycle will retry.
            log.warning("Gold Telegram service skipped (network): %s", exc)
        except Exception as exc:
            log.exception("Gold Telegram service failed: %s", exc)

    def run_cycle(self) -> dict | None:
        now = datetime.now(timezone.utc)
        session_name = self._session_name(now)
        state = self._load_state()
        state.pop("last_error", None)

        self._process_control_requests(state)
        self._prune_cooldowns(state, now)
        self._manage_open_trades(state)
        weekend_decision = self._apply_weekend_management(state, now)

        if state.get("paused", False):
            log.info("Gold-bot is paused; managing open trades only")
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "paused_manual"})
            self._save_state(state)
            self._publish_runtime_status("paused", state, balance=None)
            return None

        if session_name == "OFF_HOURS" or (session_name == "ASIA" and not self.settings.scan_asia_active):
            log.info("Skipping scan during %s session", session_name)
            state.update({"last_run_at": now.isoformat(), "last_session": session_name})
            self._save_state(state)
            self._publish_runtime_status("idle", state, balance=None)
            return None
        if session_name == "ASIA_QUIET":
            log.info("Skipping scan during ASIA quiet window")
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
        state["account_nav"] = float(account.get("nav", balance) or balance)
        state["account_unrealized_pl"] = float(account.get("unrealized_pl", 0.0) or 0.0)
        state["account_margin_used"] = float(account.get("margin_used", 0.0) or 0.0)
        state["account_margin_available"] = float(account.get("margin_available", 0.0) or 0.0)
        state["execution_mode"] = self.settings.execution_mode

        # Sprint 1 (2.8): update rolling equity history and evaluate the
        # drawdown kill switch BEFORE we decide to scan. A halted bot still
        # manages open trades (above) but refuses to open new ones.
        kill_decision = self._evaluate_drawdown_kill_switch(state, now)
        if kill_decision is not None and kill_decision.halt:
            log.warning("Drawdown kill-switch halt: %s", kill_decision.reason)
            state.update({
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": f"kill_switch_halt:{kill_decision.reason}",
            })
            self._save_state(state)
            self._publish_runtime_status("halted", state, balance=balance)
            return None

        open_gold_trades = list(state.get("open_trades", []))
        if len(open_gold_trades) >= self.settings.max_open_gold_trades:
            log.info("Skipping scan because %s open gold trade(s) already exist", len(open_gold_trades))
            state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "open_gold_position"})
            self._save_state(state)
            self._publish_runtime_status("active_trade", state, balance=balance)
            return None

        if weekend_decision is not None and weekend_decision.block_new_entries:
            log.info("Skipping scan: weekend gap guard (%s)", weekend_decision.reason)
            state.update({
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": f"weekend_guard:{weekend_decision.reason}",
            })
            self._save_state(state)
            self._publish_runtime_status("idle", state, balance=balance)
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

        usd_proxy_frames = {}
        if self.settings.usd_regime_filter_enabled:
            usd_proxy_frames = {
                instrument: self.client.fetch_candles(instrument, "H4", 120)
                for instrument in self.USD_PROXY_INSTRUMENTS
            }
            if any(frame is None for frame in usd_proxy_frames.values()):
                log.info("Skipping scan because USD proxy candle history is incomplete")
                state.update({"last_run_at": now.isoformat(), "last_session": session_name, "skip_reason": "missing_usd_proxy_candles"})
                self._save_state(state)
                self._publish_runtime_status("waiting_data", state, balance=balance)
                return None

        breakout_volume_signal = load_breakout_volume_signal(
            self.settings.breakout_external_volume_file,
            now,
            max_age_minutes=self.settings.breakout_external_volume_max_age_minutes,
        )
        real_yield_signal = load_real_yield_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_hours=self.settings.real_yield_state_max_age_hours,
        )
        cftc_signal = load_cftc_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_days=self.settings.cftc_state_max_age_days,
        )
        scored_events = load_event_scores(
            self.settings.macro_state_file,
            now=now,
            max_age_minutes=self.settings.news_score_state_max_age_minutes,
        )

        self._refresh_calibration()

        rejection_reasons: list[str] = []
        if session_name == "ASIA":
            # Limited ASIA window: only run mean-reversion / exhaustion scorer.
            opportunities = [
                score_exhaustion_reversal(self.settings, df_h4, df_d1, reasons=rejection_reasons),
            ]
        else:
            opportunities = [
                score_macro_breakout(
                    self.settings,
                    now,
                    session_name,
                    df_m15,
                    df_h1,
                    relevant_events,
                    breakout_volume_signal,
                    reasons=rejection_reasons,
                    scored_events=scored_events,
                ),
                score_exhaustion_reversal(self.settings, df_h4, df_d1, reasons=rejection_reasons),
                score_trend_pullback(self.settings, df_h1, df_h4, usd_proxy_frames, reasons=rejection_reasons),
            ]

        calibrated: list[Opportunity] = []
        for opportunity in opportunities:
            if opportunity is None or self._is_cooldown_active(state, opportunity, now):
                continue
            adj = get_strategy_adjustment(self.calibration, opportunity.strategy)
            if adj.get("block_reason"):
                log.info("Calibration blocked %s: %s", opportunity.strategy, adj["block_reason"])
                continue
            opportunity.score += float(adj.get("score_offset", 0.0))
            opportunity.metadata["calibration_risk_mult"] = float(adj.get("risk_mult", 1.0))
            filtered = apply_real_yield_overlay(self.settings, opportunity, real_yield_signal)
            if filtered is not None:
                filtered = apply_cftc_overlay(self.settings, filtered, cftc_signal)
                calibrated.append(filtered)

        best = select_best_opportunity(calibrated)
        if best is None:
            if rejection_reasons:
                log.info("No XAU/USD opportunity passed filters | %s", " ; ".join(rejection_reasons))
            else:
                log.info("No XAU/USD opportunity passed filters")
            state.update({
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": "no_signal",
                "last_filter_reasons": rejection_reasons,
            })
            self._save_state(state)
            self._publish_runtime_status("scanning", state, balance=balance)
            return None

        calibration_risk_mult = float(best.metadata.get("calibration_risk_mult", 1.0) or 1.0)
        calibration_risk_mult = max(0.5, min(1.5, calibration_risk_mult))  # clamp to safe range
        risk_multiplier = float(best.metadata.get("risk_multiplier", 1.0) or 1.0) * calibration_risk_mult

        # Sprint 1 (2.4): volatility-target sizing. Compute a vol-adjusted
        # risk budget so that 1 ATR of adverse move consumes `vol_target_nav_bps`
        # of NAV. Always clamped by the legacy %-of-sleeve cap and the
        # portfolio-wide available budget — this only ever reduces size on
        # high-vol days vs. fixed 0.75%-of-sleeve sizing.
        # Sprint 1 (2.8): if the drawdown kill-switch soft-cut fired this
        # cycle, override max_trade_risk_amount to the soft-cut fraction of
        # the gold sleeve balance. Hard halt is already handled earlier.
        legacy_cap = snapshot.max_trade_risk_amount
        if kill_decision is not None and kill_decision.soft_cut and kill_decision.risk_per_trade_override is not None:
            legacy_cap = snapshot.gold_sleeve_balance * float(kill_decision.risk_per_trade_override)
            log.info(
                "Drawdown soft-cut active: reducing per-trade risk to %.2f%% of sleeve (%.2f)",
                kill_decision.risk_per_trade_override * 100,
                legacy_cap,
            )

        nav = float(state.get("account_nav", balance) or balance)
        atr_for_sizing = float(best.metadata.get("atr") or 0.0)
        sizing_decision = compute_risk_amount(
            nav=nav,
            atr=atr_for_sizing,
            stop_distance=best.risk_per_unit,
            target_nav_bps=self.settings.vol_target_nav_bps,
            legacy_max_trade_risk=legacy_cap,
            available_gold_risk=snapshot.available_gold_risk,
            enabled=self.settings.vol_target_sizing_enabled,
            risk_multiplier=risk_multiplier,
        )
        risk_amount = sizing_decision.risk_amount
        best.metadata["sizing_source"] = sizing_decision.source
        best.metadata["sizing_nav"] = round(sizing_decision.nav, 2)
        best.metadata["sizing_atr"] = round(sizing_decision.atr, 4)
        best.metadata["sizing_target_nav_bps"] = sizing_decision.target_nav_bps
        if risk_amount <= 0:
            log.info("No gold risk budget available (source=%s)", sizing_decision.source)
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
            entry_quote = self._await_entry_quote(best)
            result = self.client.place_market_order(best, size, quote=entry_quote)
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

    def _evaluate_drawdown_kill_switch(self, state: dict, now: datetime):
        """Maintain equity history in state and evaluate the kill switch.

        Returns the DrawdownDecision (or None if disabled). The caller is
        responsible for honouring .halt (refuse new entries) and .soft_cut
        (scale per-trade risk). Open positions are always managed through to
        their documented exit.
        """
        if not getattr(self.settings, "drawdown_kill_switch_enabled", False):
            state.pop("kill_switch", None)
            return None

        equity = float(state.get("account_nav", state.get("account_balance", 0.0)) or 0.0)
        history = EquityHistory(state.get("equity_history") or [])
        history.append_today(now, equity)
        history.trim(keep_days=int(self.settings.drawdown_equity_history_max_days), now=now)

        decision = evaluate_kill_switch(
            history=history,
            equity_now=equity,
            now=now,
            latched_halt=state.get("kill_switch"),
            soft_window_days=int(self.settings.drawdown_soft_window_days),
            soft_threshold_pct=float(self.settings.drawdown_soft_threshold_pct),
            soft_risk_per_trade=float(self.settings.drawdown_soft_risk_per_trade),
            hard_window_days=int(self.settings.drawdown_hard_window_days),
            hard_threshold_pct=float(self.settings.drawdown_hard_threshold_pct),
        )

        state["equity_history"] = history.to_list()
        state["kill_switch"] = latch_halt_state(state.get("kill_switch"), decision, now=now)
        return decision

    def _await_entry_quote(self, opportunity: Opportunity) -> dict[str, float]:
        quote = self.client.get_price(self.settings.instrument)
        self._record_spread_sample(quote)
        if opportunity.strategy != "MACRO_BREAKOUT" or self.settings.execution_mode != "live":
            self._validate_spread_adaptive(quote)
            return quote

        settle_seconds = self.settings.macro_breakout_spread_settle_seconds
        if settle_seconds <= 0:
            self._validate_spread_adaptive(quote)
            return quote

        stable_spreads: list[float] = []
        deadline = time.monotonic() + settle_seconds
        while True:
            spread = float(quote.get("spread", 0.0) or 0.0)
            allowed = self._current_allowed_spread()
            if spread <= allowed:
                stable_spreads.append(spread)
                stable_spreads = stable_spreads[-self.settings.macro_breakout_spread_stability_checks :]
                if len(stable_spreads) == self.settings.macro_breakout_spread_stability_checks:
                    spread_range = max(stable_spreads) - min(stable_spreads)
                    if spread_range <= self.settings.macro_breakout_spread_stability_tolerance:
                        return quote
            else:
                stable_spreads.clear()

            if time.monotonic() >= deadline:
                raise SpreadTooWideError(
                    f"Spread did not stabilize within {settle_seconds}s; last spread {spread:.3f} > allowed {allowed:.3f}"
                )
            time.sleep(1.0)
            quote = self.client.get_price(self.settings.instrument)
            self._record_spread_sample(quote)

    def _record_spread_sample(self, quote: dict[str, float]) -> None:
        if not getattr(self.settings, "adaptive_spread_enabled", False):
            return
        spread = quote.get("spread")
        if spread is None:
            return
        try:
            self._spread_tracker.record(float(spread))
        except (TypeError, ValueError):
            return

    def _current_allowed_spread(self) -> float:
        if not getattr(self.settings, "adaptive_spread_enabled", False):
            return float(self.settings.max_entry_spread)
        return float(self._spread_tracker.allowed_spread())

    def _validate_spread_adaptive(self, quote: dict[str, float]) -> None:
        if not getattr(self.settings, "adaptive_spread_enabled", False):
            self.client.validate_entry_spread(quote)
            return
        spread = float(quote.get("spread", 0.0) or 0.0)
        allowed = self._current_allowed_spread()
        if spread > allowed + 1e-9:
            raise SpreadTooWideError(
                f"Spread {spread:.3f} exceeds adaptive max {allowed:.3f} (median={self._spread_tracker.median() or 0.0:.3f}, samples={self._spread_tracker.sample_count()})"
            )

    def _build_trade_record(self, opportunity: Opportunity, result: dict, size: float, risk_amount: float, opened_at: datetime) -> dict:
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
                if str(trade.get("exit_reason", "")) == "STOP_LOSS":
                    self._register_cooldown(state, trade, datetime.now(timezone.utc))
                self.budget.release_gold_risk(trade_id)

        state["open_trades"] = managed
        self._save_state(state)

    def _apply_weekend_management(self, state: dict, now: datetime) -> WeekendDecision | None:
        """Evaluate the weekend guard and act on open trades.

        Returns the resolved :class:`WeekendDecision` so the caller can use
        ``decision.block_new_entries`` to short-circuit scanning. ``None`` is
        returned when the feature is fully disabled by config so callers can
        treat that as a clean "do nothing".
        """
        if not getattr(self.settings, "weekend_gap_handling_enabled", False):
            return None
        decision = evaluate_weekend(
            now,
            enabled=True,
            flatten_weekday=self.settings.weekend_flatten_weekday,
            flatten_hour_utc=self.settings.weekend_flatten_hour_utc,
            stop_widen_enabled=self.settings.weekend_stop_widen_enabled,
            stop_widen_hour_utc=self.settings.weekend_stop_widen_hour_utc,
            block_new_entries_hour_utc=self.settings.weekend_block_new_entries_hour_utc,
        )
        state["weekend_guard"] = decision_to_metadata(decision)

        open_trades = list(state.get("open_trades", []))
        if not open_trades:
            return decision

        if decision.flatten:
            remaining: list[dict] = []
            for trade in open_trades:
                trade_id = str(trade.get("id", "") or "")
                if self.client.close_trade(trade_id):
                    self._record_event(state, "weekend_flatten", f"Trade {trade_id} closed by weekend guard ({decision.reason})", now=now)
                    self.budget.release_gold_risk(trade_id)
                else:
                    remaining.append(trade)
            state["open_trades"] = remaining
            self._save_state(state)
            return decision

        if decision.widen_stops:
            atr_value = self._latest_atr_estimate(timeframe="H1")
            if atr_value is None or atr_value <= 0:
                return decision
            for trade in open_trades:
                try:
                    new_stop = widened_stop_price(
                        direction=str(trade.get("direction", "")),
                        entry_price=float(trade.get("entry_price", 0.0) or 0.0),
                        current_stop=float(trade.get("stop_price", 0.0) or 0.0),
                        atr=float(atr_value),
                        atr_mult=self.settings.weekend_stop_widen_atr_mult,
                        max_weekend_gap_pct=0.012,
                    )
                except (TypeError, ValueError):
                    continue
                current = float(trade.get("stop_price", 0.0) or 0.0)
                if abs(new_stop - current) < 1e-6:
                    continue
                if self.client.modify_trade(str(trade["id"]), stop_price=new_stop):
                    trade["stop_price"] = round(new_stop, 3)
                    self._record_event(state, "weekend_widen", f"Trade {trade['id']} stop widened to {trade['stop_price']} ({decision.reason})", now=now)
            state["open_trades"] = open_trades
            self._save_state(state)

        return decision

    def _latest_atr_estimate(self, *, timeframe: str = "H1") -> float | None:
        """Best-effort ATR fetch used by ancillary modules (weekend guard)."""
        try:
            candles = self.client.fetch_candles(self.settings.instrument, timeframe, 60)
            if candles is None or len(candles) < self.settings.atr_period + 1:
                return None
            return float(calc_atr(candles, self.settings.atr_period))
        except Exception:
            log.exception("ATR estimate fetch failed")
            return None

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
            trade["exit_reason"] = "STOP_LOSS"
            self._record_event(state, "trade_stopped", f"Trade {trade['id']} hit stop at {trade.get('stop_price')}", now=datetime.now(timezone.utc))
            return False

        exit_plan = dict(trade.get("exit_plan", {}))
        if not exit_plan:
            return True

        if (not trade.get("partial_taken")) and self._reached_level(trade["direction"], current_price, float(exit_plan["partial_take_profit_price"])):
            remaining = float(trade.get("remaining_size", trade["size"]))
            partial_size = round(remaining * float(exit_plan.get("partial_take_profit_fraction", 0.5)), 2)
            if partial_size < 0.01:
                partial_size = remaining  # close all if remainder too small
            if self.client.close_trade(str(trade["id"]), size=partial_size):
                trade["partial_taken"] = True
                trade["remaining_size"] = max(0.0, float(trade.get("remaining_size", trade["size"])) - partial_size)
                self._record_event(state, "partial_profit", f"Trade {trade['id']} took partial profit, remaining size {trade['remaining_size']}", now=datetime.now(timezone.utc))

        if (not trade.get("break_even_moved")) and exit_plan.get("break_even_trigger_price") is not None and self._reached_level(trade["direction"], current_price, float(exit_plan["break_even_trigger_price"])):
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
        try:
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
        except Exception:
            log.exception("Trailing stop computation failed for trade %s", trade.get("id"))
            return None

    def _tighten_stop(self, trade: dict, candidate_stop: float) -> bool:
        current_stop = float(trade.get("stop_price", trade.get("initial_stop_price", 0.0)) or 0.0)
        if trade["direction"] == "LONG":
            if candidate_stop <= current_stop:
                return False
        else:
            if current_stop > 0 and candidate_stop >= current_stop:
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
            if (
                self.settings.scan_asia_active
                and self.settings.asia_active_start_utc <= hour < self.settings.asia_active_end_utc
            ):
                return "ASIA"
            return "ASIA_QUIET"
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
        state.setdefault("cooldowns", [])
        state.setdefault("paused", False)
        save_json_payload(str(self.state_path), state, self.state_key)

    def _load_state(self) -> dict:
        state = load_json_payload(str(self.state_path), self.state_key, {"signals": [], "open_trades": [], "events": []})
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("events", [])
        state.setdefault("control_requests", [])
        state.setdefault("cooldowns", [])
        state.setdefault("paused", False)
        return state

    def _register_cooldown(self, state: dict, trade: dict, now: datetime) -> None:
        hours = self.settings.trend_stopout_cooldown_hours
        if hours <= 0:
            return
        cooldowns = state.setdefault("cooldowns", [])
        strategy = str(trade.get("strategy", ""))
        direction = str(trade.get("direction", ""))
        cooldowns[:] = [item for item in cooldowns if not (item.get("strategy") == strategy and item.get("direction") == direction)]
        cooldowns.append({"strategy": strategy, "direction": direction, "expires_at": (now + timedelta(hours=hours)).isoformat(), "reason": "STOP_LOSS"})

    def _prune_cooldowns(self, state: dict, now: datetime) -> None:
        cooldowns = state.setdefault("cooldowns", [])
        retained = []
        for item in cooldowns:
            try:
                expires_at = datetime.fromisoformat(str(item["expires_at"]))
            except Exception:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at > now:
                retained.append(item)
        state["cooldowns"] = retained

    def _is_cooldown_active(self, state: dict, opportunity, now: datetime) -> bool:
        for item in state.get("cooldowns", []):
            if item.get("strategy") != opportunity.strategy or item.get("direction") != opportunity.direction:
                continue
            try:
                expires_at = datetime.fromisoformat(str(item["expires_at"]))
            except Exception:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at > now:
                return True
        return False

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

    @staticmethod
    def _sanitize_error(error_text: str) -> str:
        import re
        text = re.sub(r'for url: https?://\S+', '', error_text).strip()
        text = re.sub(r'https?://\S+', '[OANDA API]', text)
        return text[:300]

    def _calibration_summary(self) -> dict:
        """Build a compact calibration summary for status payloads."""
        if not self.calibration:
            return {"active": False}
        adj = self.calibration.get("strategy_adjustments", {})
        return {
            "active": True,
            "generated_at": self.calibration.get("generated_at"),
            "total_trades": self.calibration.get("total_trades", 0),
            "win_rate": self.calibration.get("win_rate", 0),
            "profit_factor": self.calibration.get("profit_factor", 0),
            "strategies": {
                s: {
                    "score_offset": v.get("score_offset", 0),
                    "risk_mult": v.get("risk_mult", 1),
                    "blocked": bool(v.get("block_reason")),
                }
                for s, v in adj.items()
            },
        }

    def _publish_runtime_status(self, state_name: str, state: dict, balance: float | None) -> None:
        publish_runtime_status(
            service="gold-bot",
            state=state_name,
            redis_key=self.status_key,
            ttl_seconds=self.status_ttl,
            file_path=str(self.status_path),
            balance=balance,
            nav=state.get("account_nav"),
            unrealized_pl=state.get("account_unrealized_pl"),
            margin_used=state.get("account_margin_used"),
            margin_available=state.get("account_margin_available"),
            account_currency=state.get("account_currency"),
            execution_mode=state.get("execution_mode", self.settings.execution_mode),
            paused=bool(state.get("paused", False)),
            open_trades=len(state.get("open_trades", [])),
            last_run_at=state.get("last_run_at"),
            last_session=state.get("last_session"),
            skip_reason=state.get("skip_reason"),
            error=state.get("last_error"),
            calibration=self._calibration_summary(),
        )
        self._maybe_send_heartbeat(state_name, state, balance)

    def _maybe_send_heartbeat(self, state_name: str, state: dict, balance: float | None) -> None:
        if self.heartbeat_interval <= 0:
            return
        if not self.telegram_token or not self.telegram_chat_id:
            return
        now = time.time()
        if now - self.last_heartbeat_at < self.heartbeat_interval:
            return
        self.last_heartbeat_at = now
        self._send_telegram_message(self._build_heartbeat_message(state_name, state, balance))

    def _build_heartbeat_message(self, state_name: str, state: dict, balance: float | None) -> str:
        open_trades = list(state.get("open_trades", []))
        session = state.get("last_session") or "unknown"
        skip_reason = state.get("skip_reason")
        currency = state.get("account_currency") or "GBP"
        gold_balance = balance * self.settings.gold_budget_allocation if balance is not None else None
        balance_text = f"{currency}{gold_balance:,.2f}" if gold_balance is not None else "n/a"
        execution_mode = state.get("execution_mode", self.settings.execution_mode)
        mode_labels = {
            "live": "\U0001f4b0 LIVE",
            "paper": "\U0001f4dd PAPER",
            "signal_only": "\U0001f4e1 SIGNAL",
        }
        mode_text = mode_labels.get(execution_mode, execution_mode.upper())
        session_labels = {
            "OFF_HOURS": "\U0001f319 OFF_HOURS",
            "ASIA": "\U0001f30f ASIA",
            "LONDON": "\U0001f1ec\U0001f1e7 LONDON",
            "OVERLAP": "\U0001f525 OVERLAP",
            "NEW_YORK": "\U0001f5fd NEW_YORK",
        }
        session_text = session_labels.get(session.upper(), session.upper())
        state_labels = {
            "scanning": "\U0001f7e2 Scanning",
            "idle": "\U0001f7e2 Idle",
            "trade_opened": "\U0001f7e2 Trade opened",
            "active_trade": "\U0001f7e2 Managing trade",
            "paused": "\u23f8\ufe0f Paused",
            "error": "\U0001f534 Error",
            "booting": "\U0001f7e1 Booting",
            "waiting_data": "\U0001f7e1 Waiting for data",
            "pre_news_pause": "\U0001f7e1 Pre-news pause",
        }
        state_text = state_labels.get(state_name, state_name.replace("_", " ").title())
        last_run_text = self._format_heartbeat_time(state.get("last_run_at"))
        # Calibration summary line
        cal = self._calibration_summary()
        if cal.get("active"):
            blocked = [s for s, v in cal.get("strategies", {}).items() if v.get("blocked")]
            cal_parts = [
                f"pf={cal.get('profit_factor', 0):.1f}",
                f"wr={cal.get('win_rate', 0) * 100:.0f}%",
                f"{cal.get('total_trades', 0)} trades",
            ]
            if blocked:
                cal_parts.append(f"blocked: {', '.join(blocked)}")
            cal_text = " | ".join(cal_parts)
        else:
            cal_text = "none loaded"

        lines = [
            f"\U0001f493 <b>Gold Heartbeat</b> [{mode_text}]",
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501",
            f"{session_text} | {state_text}",
            f"\U0001f4b0 {balance_text} | Open: {len(open_trades)}",
            f"\U0001f552 Last run: {last_run_text}",
            f"\U0001f4ca Calibration: {cal_text}",
        ]
        if skip_reason and skip_reason != "none":
            reason_labels = {
                "no_signal": "no signal",
                "open_gold_position": "existing gold position",
                "pre_news_pause": "pre-news pause",
                "missing_candles": "waiting for candle data",
                "paused_manual": "paused manually",
            }
            lines.append(f"\u23ed\ufe0f Skip: {reason_labels.get(skip_reason, skip_reason.replace('_', ' '))}")
        return "\n".join(lines)

    @staticmethod
    def _format_heartbeat_time(value: str | None) -> str:
        if not value:
            return "never"
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return "unknown"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        day_delta = (parsed.date() - now.date()).days
        if day_delta == 0:
            prefix = "Today"
        elif day_delta == -1:
            prefix = "Yesterday"
        else:
            prefix = parsed.strftime("%a %d %b %Y")
        return f"{prefix} at {parsed.strftime('%H:%M')} UTC"

    def _send_telegram_message(self, message: str) -> None:
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.telegram_token}/sendMessage",
                json={"chat_id": self.telegram_chat_id, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("Failed to send Telegram heartbeat: %s", exc)