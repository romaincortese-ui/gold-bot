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
from goldbot.event_policy import apply_gold_event_policy, evaluate_gold_event_catalyst
from goldbot.marketdata import OandaClient, SpreadTooWideError
from goldbot.models import Opportunity
from goldbot.news import fetch_calendar_events, filter_gold_events
from goldbot.news_scoring import load_event_scores
from goldbot.kill_switch import EquityHistory, evaluate_kill_switch, latch_halt_state
from goldbot.real_yields import apply_real_yield_overlay, load_real_yield_signal_from_macro_state
from goldbot.cftc import apply_cftc_overlay, load_cftc_signal_from_macro_state
from goldbot.co_trade import apply_co_trade_gates, load_co_trade_signal_from_macro_state
from goldbot.options_iv import (
    evaluate_options_iv_gate,
    load_options_iv_signal_from_macro_state,
    should_gate_strategy as options_iv_should_gate_strategy,
)
from goldbot.regime import classify_from_settings, parse_strategy_csv, strategy_allowed_in_regime
from goldbot.miners_overlay import apply_miners_overlay, load_miners_signal_from_macro_state
from goldbot.factor_model import apply_factor_overlay, load_factor_signal_from_macro_state
from goldbot.central_bank_flow import (
    apply_central_bank_short_veto,
    load_central_bank_flow_from_macro_state,
)
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
    score_event_catalyst_breakout,
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
        self._gold_event_state: dict | None = None
        self._last_gold_event_refresh_at = 0.0
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
        # Gate A G1+G4 (memo 1 §7): single structured boot line so the operator
        # can see exactly which mode and which overlays are actually live.
        self._log_boot_manifest()
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
        co_trade_signal = load_co_trade_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_hours=self.settings.co_trade_state_max_age_hours,
        )
        options_iv_signal = load_options_iv_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_hours=self.settings.options_iv_state_max_age_hours,
        )
        miners_signal = load_miners_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_hours=self.settings.miners_state_max_age_hours,
        )
        factor_signal = load_factor_signal_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_hours=self.settings.factor_model_state_max_age_hours,
        )
        central_bank_signal = load_central_bank_flow_from_macro_state(
            self.settings.macro_state_file,
            now,
            max_age_days=self.settings.central_bank_state_max_age_days,
        )
        scored_events = load_event_scores(
            self.settings.macro_state_file,
            now=now,
            max_age_minutes=self.settings.news_score_state_max_age_minutes,
        )
        gold_event_state = self._refresh_gold_event_state()

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
            catalyst_decision = evaluate_gold_event_catalyst(self.settings, gold_event_state, now)
            if catalyst_decision.direction:
                catalyst_opportunity = score_event_catalyst_breakout(
                    self.settings,
                    now,
                    session_name,
                    df_m15,
                    df_h1,
                    direction=catalyst_decision.direction,
                    catalyst_metadata={
                        **catalyst_decision.metadata,
                        "gold_event_catalyst_reason": catalyst_decision.reason,
                    },
                    breakout_volume_signal=breakout_volume_signal,
                    reasons=rejection_reasons,
                    scored_events=scored_events,
                )
                if catalyst_opportunity is not None:
                    log.info(
                        "Gold event catalyst created %s candidate (bias=%.2f events=%d)",
                        catalyst_opportunity.direction,
                        catalyst_decision.gold_bias_score,
                        catalyst_decision.event_count,
                    )
                    opportunities.append(catalyst_opportunity)
            elif catalyst_decision.reason not in {"disabled", "catalyst_disabled", "no_fresh_event_state"}:
                rejection_reasons.append(f"gold_event_catalyst:{catalyst_decision.reason}")

        # Sprint 3 §3.2: classify current vol regime from H4 ATR% (falls back
        # to H1 if H4 unavailable) and filter strategies not allowed under it.
        regime_classification = self._classify_regime(df_h1, df_h4, relevant_events, now)
        if regime_classification is not None:
            log.info(
                "Regime classification: %s (atr_pct=%.4f news_burst=%s)",
                regime_classification.regime,
                regime_classification.atr_pct,
                regime_classification.news_burst,
            )

        calibrated: list[Opportunity] = []
        for opportunity in opportunities:
            if opportunity is None or self._is_cooldown_active(state, opportunity, now):
                continue
            if regime_classification is not None and self.settings.regime_filter_enabled:
                if not strategy_allowed_in_regime(
                    regime_classification.regime,
                    opportunity.strategy,
                    quiet_strategies=parse_strategy_csv(self.settings.regime_quiet_strategies),
                    trend_strategies=parse_strategy_csv(self.settings.regime_trend_strategies),
                    spike_strategies=parse_strategy_csv(self.settings.regime_spike_strategies),
                ):
                    rejection_reasons.append(
                        f"regime_filter:{opportunity.strategy}_not_allowed_in_{regime_classification.regime}"
                    )
                    continue
            adj = get_strategy_adjustment(self.calibration, opportunity.strategy)
            if adj.get("block_reason"):
                log.info("Calibration blocked %s: %s", opportunity.strategy, adj["block_reason"])
                continue
            opportunity.score += float(adj.get("score_offset", 0.0))
            opportunity.metadata["calibration_risk_mult"] = float(adj.get("risk_mult", 1.0))
            if regime_classification is not None:
                opportunity.metadata["regime"] = regime_classification.regime
                opportunity.metadata["regime_atr_pct"] = round(regime_classification.atr_pct, 5)
            # Sprint 3 §3.3: options-IV gate for MACRO_BREAKOUT only.
            if options_iv_should_gate_strategy(self.settings, opportunity.strategy):
                realised_1h_pct = self._realised_1h_move_pct(df_m15)
                if (
                    options_iv_signal is not None
                    and realised_1h_pct is not None
                ):
                    gate = evaluate_options_iv_gate(
                        realised_move_pct=realised_1h_pct,
                        implied_1d_move_pct=options_iv_signal.implied_1d_move_pct,
                        threshold_fraction=self.settings.options_iv_realised_fraction_threshold,
                    )
                    opportunity.metadata["options_iv_ratio"] = round(gate.ratio, 3)
                    opportunity.metadata["options_iv_threshold"] = gate.threshold_fraction
                    if not gate.passed:
                        rejection_reasons.append(f"options_iv_gate:{gate.reason}")
                        continue
            filtered = apply_real_yield_overlay(self.settings, opportunity, real_yield_signal)
            if filtered is not None:
                filtered = apply_cftc_overlay(self.settings, filtered, cftc_signal)
                # Q2 §4.1: miners overlay (XAU daily % feeds divergence).
                gold_daily_change_pct = self._gold_daily_change_pct(df_h1)
                filtered = apply_miners_overlay(
                    self.settings,
                    filtered,
                    miners_signal,
                    gold_daily_change_pct=gold_daily_change_pct,
                )
                # Q2 §4.2: 3-factor model (TIPS / DXY / GLD flow).
                filtered = apply_factor_overlay(self.settings, filtered, factor_signal)
                # Q2 §4.3: central-bank flow veto for EXHAUSTION_REVERSAL shorts.
                filtered = apply_central_bank_short_veto(
                    self.settings, filtered, central_bank_signal
                )
                # Sprint 3 §3.1: co-trade gates can still veto an opportunity.
                if filtered is not None:
                    filtered = apply_co_trade_gates(self.settings, filtered, co_trade_signal)
                if filtered is not None:
                    filtered, event_decision = apply_gold_event_policy(self.settings, filtered, gold_event_state, now)
                    if filtered is None:
                        rejection_reasons.append(f"gold_event_policy:{event_decision.reason}")
                    elif event_decision.reason not in {"disabled", "no_fresh_event_state", "neutral"}:
                        log.info(
                            "Gold event policy %s for %s %s (bias=%.2f risk_mult=%.2f events=%d)",
                            event_decision.reason,
                            filtered.strategy,
                            filtered.direction,
                            event_decision.gold_bias_score,
                            event_decision.risk_multiplier,
                            event_decision.event_count,
                        )
                if filtered is not None:
                    calibrated.append(filtered)

        best = select_best_opportunity(calibrated)
        if best is None:
            # Gate A G5 (memo 1 §7): structured telemetry so the operator can
            # see which sleeves were evaluated and why they were rejected
            # without having to reconstruct it from free-text log lines.
            candidate_count = len(calibrated)
            reasons_str = " ; ".join(rejection_reasons) if rejection_reasons else "no_candidate"
            log.info(
                "[OPPORTUNITY] session=%s candidates=%d reasons=%s",
                session_name,
                candidate_count,
                reasons_str,
            )
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

        target_risk_amount = risk_amount
        size = self.client.calculate_xau_size(target_risk_amount, best.risk_per_unit, account_currency)
        if size <= 0:
            log.info("Unable to size XAU/USD trade")
            state.update({
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": "size_zero",
                "last_signal": {
                    **asdict(best),
                    "risk_amount": target_risk_amount,
                    "size": size,
                    "budget_snapshot": asdict(snapshot),
                    "sizing_reject_reason": "minimum_xau_unit_exceeds_risk_budget",
                },
            })
            self._save_state(state)
            self._publish_runtime_status("size_zero", state, balance=balance)
            return None

        risk_amount = self.client.estimate_xau_risk_amount(size, best.risk_per_unit, account_currency)
        if risk_amount <= 0 or risk_amount > target_risk_amount + max(0.01, target_risk_amount * 0.01):
            log.warning(
                "Sized XAU/USD risk %.2f exceeds target budget %.2f; skipping entry",
                risk_amount,
                target_risk_amount,
            )
            state.update({
                "last_run_at": now.isoformat(),
                "last_session": session_name,
                "skip_reason": "size_exceeds_risk_budget",
                "last_signal": {
                    **asdict(best),
                    "risk_amount": target_risk_amount,
                    "sized_risk_amount": risk_amount,
                    "size": size,
                    "budget_snapshot": asdict(snapshot),
                },
            })
            self._save_state(state)
            self._publish_runtime_status("size_zero", state, balance=balance)
            return None

        try:
            entry_quote = self._await_entry_quote(best)
            # Gate A G5 (memo 1 §7): audit line immediately before the OANDA
            # call — exposes spread the bot actually saw vs. the configured
            # cap at entry time, matching the FX-bot [ENTRY_SPREAD] pattern.
            entry_spread = float(entry_quote.get("spread", 0.0) or 0.0) if entry_quote else 0.0
            log.info(
                "[ENTRY_SPREAD] strategy=%s instrument=%s direction=%s spread=%.4f cap=%.2f mode=%s",
                best.strategy,
                self.settings.instrument,
                best.direction,
                entry_spread,
                self.settings.max_entry_spread,
                self.settings.execution_mode,
            )
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
        trade_record = self._build_trade_record(best, result, size, risk_amount, now)
        reserved_after = snapshot.reserved_gold_risk + risk_amount
        self._record_event(
            state,
            "trade_opened",
            f"{best.strategy} {best.direction} opened at {result.get('price', best.entry_price)} | size {size} | risk {risk_amount:.2f} | mode {result.get('mode', self.settings.execution_mode)}",
            now=now,
            details={
                **trade_record,
                "mode": result.get("mode", self.settings.execution_mode),
                "score": float(best.score),
                "account_currency": account_currency,
                "target_risk_amount": float(target_risk_amount),
                "gold_sleeve_balance": float(snapshot.gold_sleeve_balance),
                "max_trade_risk_amount": float(snapshot.max_trade_risk_amount),
                "max_total_risk_amount": float(snapshot.max_total_risk_amount),
                "reserved_gold_risk_before": float(snapshot.reserved_gold_risk),
                "reserved_gold_risk_after": float(reserved_after),
                "available_gold_risk_after": float(max(0.0, snapshot.max_total_risk_amount - reserved_after)),
            },
        )
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

    def _refresh_gold_event_state(self) -> dict | None:
        if not getattr(self.settings, "gold_event_policy_enabled", True):
            return None
        now_monotonic = time.monotonic()
        if (
            self._gold_event_state is not None
            and now_monotonic - self._last_gold_event_refresh_at < self.settings.gold_event_refresh_seconds
        ):
            return self._gold_event_state
        self._last_gold_event_refresh_at = now_monotonic
        try:
            payload = load_json_payload(
                self.settings.gold_event_state_file,
                self.settings.gold_event_redis_key,
                {},
            )
        except Exception:
            log.exception("Failed to refresh gold event state")
            return self._gold_event_state
        self._gold_event_state = payload if isinstance(payload, dict) and payload else None
        return self._gold_event_state

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

    def _classify_regime(self, df_h1, df_h4, relevant_events, now):
        """Sprint 3 §3.2: classify the current regime from ATR% and news burst."""
        if df_h4 is None and df_h1 is None:
            return None
        frame = df_h4 if df_h4 is not None and not df_h4.empty else df_h1
        if frame is None or frame.empty:
            return None
        try:
            atr = float(calc_atr(frame, self.settings.atr_period))
            close = float(frame.iloc[-1]["close"])
        except Exception:
            log.exception("Regime ATR calc failed")
            return None
        if close <= 0:
            return None
        atr_pct = atr / close
        news_burst = False
        if relevant_events:
            burst_window = timedelta(minutes=self.settings.post_news_settle_minutes)
            for event in relevant_events:
                occurs_at = getattr(event, "occurs_at", None)
                if occurs_at is None:
                    continue
                delta = now - occurs_at
                if timedelta(0) <= delta <= burst_window:
                    news_burst = True
                    break
        return classify_from_settings(self.settings, atr_pct=atr_pct, news_burst=news_burst)

    @staticmethod
    def _realised_1h_move_pct(df_m15) -> float | None:
        """Return the last-hour realised abs-% move on the M15 frame.

        Uses the last four M15 closes (≈1h) to approximate a 1h move without
        needing a separate H1 re-fetch. Returns ``None`` when the frame is
        too short or the reference close is non-positive.
        """
        if df_m15 is None or len(df_m15) < 5:
            return None
        try:
            latest_close = float(df_m15.iloc[-1]["close"])
            ref_close = float(df_m15.iloc[-5]["close"])
        except Exception:
            return None
        if ref_close <= 0:
            return None
        return abs(latest_close - ref_close) / ref_close

    @staticmethod
    def _gold_daily_change_pct(df_h1) -> float | None:
        """Return the last 24h signed % change on the H1 frame.

        Used by the miners overlay to detect miners-vs-gold divergences.
        Returns ``None`` when the frame is too short or the reference
        close is non-positive.
        """
        if df_h1 is None or len(df_h1) < 25:
            return None
        try:
            latest_close = float(df_h1.iloc[-1]["close"])
            ref_close = float(df_h1.iloc[-25]["close"])
        except Exception:
            return None
        if ref_close <= 0:
            return None
        return (latest_close - ref_close) / ref_close

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
            fraction = float(exit_plan.get("partial_take_profit_fraction", 0.5))
            # OANDA XAU_USD trades are denominated in integer units. Snap the
            # partial-close size to a positive integer that does not exceed the
            # remaining position; otherwise OANDA rejects the close with
            # 400 Bad Request (e.g. units=0 on a 1-unit trade where 50% rounds
            # down to 0). Without this guard the failed close keeps retrying
            # every cycle because partial_taken is never set.
            remaining_units = int(round(remaining))
            partial_units = int(round(remaining * fraction))
            if remaining_units <= 1 or partial_units < 1:
                # Position too small to scale out. Mark partial_taken so the
                # break-even / trailing-stop logic below can take over and we
                # do not retry the broken partial-close every cycle.
                trade["partial_taken"] = True
                self._record_event(
                    state,
                    "partial_skipped",
                    f"Trade {trade['id']} partial profit skipped (remaining size {remaining_units} too small to scale out)",
                    now=datetime.now(timezone.utc),
                )
            elif partial_units >= remaining_units:
                # Snap to a full close instead of asking OANDA to close more
                # than the trade currently holds.
                if self.client.close_trade(str(trade["id"])):
                    trade["partial_taken"] = True
                    trade["remaining_size"] = 0.0
                    self._record_event(
                        state,
                        "partial_profit",
                        f"Trade {trade['id']} closed in full at partial-profit target (size {remaining_units} too small to scale out)",
                        now=datetime.now(timezone.utc),
                    )
            else:
                if self.client.close_trade(str(trade["id"]), size=partial_units):
                    trade["partial_taken"] = True
                    trade["remaining_size"] = max(0.0, remaining - partial_units)
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

    def _log_boot_manifest(self) -> None:
        """Gate A G1+G4 — single ``[BOOT]`` log line so deploy-state is readable at a glance.

        Covers:

        * OANDA environment (practice / live) and the effective ``EXECUTION_MODE``.
          On a fxPractice account ``live`` hits the OANDA demo order path; on a
          real live account ``live`` ships real orders. ``signal_only`` never
          calls ``place_order`` at all. If the operator is on practice but
          still in ``signal_only`` a WARN is emitted so the state is impossible
          to miss.
        * All Q2 / Tier-3 overlay flags (miners, 3-factor, CB-flow veto, risk-parity,
          options-IV, CFTC, co-trade, real-yield, USD regime, volume mode).
        """
        s = self.settings
        overlays = {
            "usd_regime": s.usd_regime_filter_enabled,
            "real_yield_filter": s.real_yield_filter_enabled,
            "breakout_volume_mode": s.breakout_volume_mode,
            "vol_target_sizing": s.vol_target_sizing_enabled,
            "drawdown_kill": s.drawdown_kill_switch_enabled,
            "cftc": s.cftc_filter_enabled,
            "co_trade": s.co_trade_gates_enabled,
            "options_iv": s.options_iv_gate_enabled,
            "miners_overlay": s.miners_overlay_enabled,
            "factor_model": s.factor_model_enabled,
            "central_bank_flow": s.central_bank_flow_enabled,
            "risk_parity": s.risk_parity_enabled,
        }
        overlay_str = " ".join(
            f"{k}={'on' if v is True else ('off' if v is False else v)}"
            for k, v in overlays.items()
        )
        log.info(
            "[BOOT] env=%s exec_mode=%s account_type=%s instrument=%s max_entry_spread=%.2f %s",
            s.oanda_environment,
            s.execution_mode,
            s.account_type,
            s.instrument,
            s.max_entry_spread,
            overlay_str,
        )
        # Surface the most common misconfiguration: being on a demo account
        # but still in ``signal_only`` so nothing ever gets placed against the
        # fxPractice broker. Loud warning rather than silent log so the
        # operator reads it on redeploy.
        if (
            s.oanda_environment == "practice"
            and s.execution_mode == "signal_only"
        ):
            log.warning(
                "[BOOT] EXECUTION_MODE=signal_only on practice account — "
                "no orders will be submitted. Set EXECUTION_MODE=live to "
                "exercise the OANDA fxPractice order path."
            )

    # Gate A G2 (memo 1 §7): refuse to persist test-sentinel payloads into
    # the production state file. Historical state.json contained 10 residual
    # ``Cycle error: boom`` events from dev/test runs that wrote into the
    # real file. This guard is intentionally narrow — it only rejects exact
    # token matches known to come from unit-test mocks.
    _TEST_SENTINEL_TOKENS = ("boom",)

    @classmethod
    def _is_test_sentinel(cls, message: str) -> bool:
        if not message:
            return False
        lowered = message.strip().lower()
        for token in cls._TEST_SENTINEL_TOKENS:
            # Exact-word match only; avoid swallowing a real error that
            # happens to contain the substring.
            if lowered == token or lowered.endswith(f": {token}"):
                return True
        return False

    @classmethod
    def _record_event(cls, state: dict, event_type: str, message: str, *, now: datetime, details: dict | None = None) -> None:
        if cls._is_test_sentinel(message):
            return
        events = state.setdefault("events", [])
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": now.isoformat(),
            "type": event_type,
            "message": message,
        }
        if details:
            event["details"] = details
        events.append(event)
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