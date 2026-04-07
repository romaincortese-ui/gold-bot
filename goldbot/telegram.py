import json
import logging
import os
import time
from html import escape
from datetime import datetime, timezone
from pathlib import Path

import requests

from goldbot.budget import SharedBudgetManager
from goldbot.config import load_settings
from goldbot.marketdata import OandaClient
from goldbot.shared_backend import load_json_payload, load_runtime_status, publish_runtime_status, save_json_payload


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
)
log = logging.getLogger(__name__)


def run_telegram_bot() -> None:
    token = os.getenv("GOLD_TELEGRAM_TOKEN", "").strip()
    chat_id = os.getenv("GOLD_TELEGRAM_CHAT_ID", "").strip()
    poll_seconds = int(os.getenv("GOLD_TELEGRAM_POLL_SECONDS", "5"))
    heartbeat_minutes = int(os.getenv("GOLD_TELEGRAM_HEARTBEAT_MINUTES", "60"))
    offset_path = Path(os.getenv("GOLD_TELEGRAM_OFFSET_FILE", "telegram_state.json"))
    settings = load_settings()
    state_path = Path(settings.state_file)

    if not token or not chat_id:
        raise ValueError("GOLD_TELEGRAM_TOKEN and GOLD_TELEGRAM_CHAT_ID are required for the Telegram worker")

    client = GoldTelegramClient(token=token, chat_id=chat_id, state_path=state_path, offset_path=offset_path)
    client.run_forever(poll_seconds=poll_seconds, heartbeat_minutes=heartbeat_minutes)


class GoldTelegramClient:
    def __init__(self, *, token: str, chat_id: str, state_path: Path, offset_path: Path) -> None:
        self.settings = load_settings()
        self.token = token
        self.chat_id = str(chat_id)
        self.state_path = state_path
        self.offset_path = offset_path
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.state_key = os.getenv("GOLD_RUNTIME_STATE_KEY", "gold_runtime_state").strip()
        self.bot_status_key = os.getenv("GOLD_BOT_STATUS_KEY", "gold_bot_runtime_status").strip()
        self.bot_status_path = Path(os.getenv("GOLD_BOT_STATUS_FILE", str(self.state_path.with_name("gold_bot_runtime_status.json")))).expanduser()
        self.status_key = os.getenv("GOLD_TELEGRAM_STATUS_KEY", "gold_telegram_runtime_status").strip()
        self.status_path = Path(os.getenv("GOLD_TELEGRAM_STATUS_FILE", str(self.state_path.with_name("gold_telegram_runtime_status.json")))).expanduser()
        self.status_ttl = int(os.getenv("GOLD_STATUS_TTL", "1800"))
        self.budget = SharedBudgetManager(self.settings)
        self.marketdata = OandaClient(self.settings)

    def run_forever(self, *, poll_seconds: int, heartbeat_minutes: int) -> None:
        self.send_message("Gold Telegram worker online. Use /help for commands.")
        last_heartbeat = 0.0
        while True:
            try:
                self.flush_new_events()
                self.poll_commands()
                now = time.time()
                if heartbeat_minutes > 0 and now - last_heartbeat >= heartbeat_minutes * 60:
                    self._publish_status("running")
                    last_heartbeat = now
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.exception("Gold Telegram loop failed: %s", exc)
            time.sleep(max(1, poll_seconds))

    def flush_new_events(self) -> None:
        state = self._load_state()
        events = list(state.get("events", []))
        sent_ids = set(self._load_offset().get("sent_event_ids", []))
        updated = False
        for event in events:
            event_id = str(event.get("id", ""))
            if not event_id or event_id in sent_ids:
                continue
            self.send_message(self._format_event(event))
            sent_ids.add(event_id)
            updated = True
        if updated:
            offset = self._load_offset()
            offset["sent_event_ids"] = list(sent_ids)[-500:]
            self._save_offset(offset)

    def poll_commands(self) -> None:
        offset = self._load_offset()
        last_update_id = int(offset.get("last_update_id", 0) or 0)
        response = requests.get(
            f"{self.base_url}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 1},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        for update in payload.get("result", []):
            update_id = int(update.get("update_id", 0) or 0)
            offset["last_update_id"] = update_id
            message = update.get("message", {})
            chat_id = str(message.get("chat", {}).get("id", ""))
            text = str(message.get("text", "")).strip()
            if chat_id != self.chat_id or not text:
                continue
            reply = self._handle_command(text)
            if reply:
                self.send_message(reply)
        self._save_offset(offset)

    def send_message(self, message: str) -> None:
        response = requests.post(
            f"{self.base_url}/sendMessage",
            json={
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        response.raise_for_status()

    def _handle_command(self, text: str) -> str:
        state = self._load_state()
        command = text.lower().split()[0]
        if command == "/help":
            return self._build_help_message()
        if command == "/status":
            return self._build_status_message(state)
        if command == "/last":
            return self._build_last_signal_message(state)
        if command == "/events":
            return self._build_recent_events_message(state)
        if command == "/open":
            return self._build_open_trades_message(state)
        if command == "/risk":
            return self._build_risk_message(state)
        if command == "/pause":
            self._append_control_request(state, "pause")
            self._append_event(state, "manual_pause_requested", "Telegram operator requested Gold-bot pause")
            self._save_state(state)
            return "Pause request queued. Gold-bot will stop new entries and keep managing open trades."
        if command == "/resume":
            self._append_control_request(state, "resume")
            self._append_event(state, "manual_resume_requested", "Telegram operator requested Gold-bot resume")
            self._save_state(state)
            return "Resume request queued. Gold-bot will re-enable entries on the next runtime cycle."
        if command == "/sync":
            self._append_control_request(state, "sync")
            self._append_event(state, "sync_requested", "Telegram operator requested broker sync")
            self._save_state(state)
            return "Sync request queued. Gold-bot will reconcile tracked trades with the broker on the next cycle."
        if command == "/closeall":
            self._append_control_request(state, "close_all")
            self._append_event(state, "close_all_requested", "Telegram operator requested close-all")
            self._save_state(state)
            return "Close-all request queued. Gold-bot will attempt to close all tracked gold trades on the next cycle."
        return self._build_help_message()

    def _build_help_message(self) -> str:
        return (
            "🧭 <b>Gold Bot Commands</b>\n"
            "━━━━━━━━━━━━━━━\n"
            "/status - Runtime, balance, risk, trades\n"
            "/last - Latest signal snapshot\n"
            "/events - Recent runtime events\n"
            "/open - Open trade details\n"
            "/risk - Gold sleeve risk snapshot\n"
            "/pause - Queue a pause request\n"
            "/resume - Queue a resume request\n"
            "/sync - Queue a broker sync\n"
            "/closeall - Queue close-all"
        )

    def _load_broker_snapshot(self) -> dict | None:
        if not self.settings.oanda_api_key or not self.settings.oanda_account_id:
            return None
        try:
            account = self.marketdata.get_account_summary(force_broker=True)
            trades = self.marketdata.list_open_trades(force_broker=True)
        except Exception as exc:
            log.warning("Gold telegram broker snapshot unavailable: %s", exc)
            return None
        gold_trades = [trade for trade in trades if str(trade.get("instrument", "")).upper() == self.settings.instrument]
        gold_unrealized = sum(float(trade.get("unrealizedPL", 0.0) or 0.0) for trade in gold_trades)
        return {
            "balance": account.get("balance"),
            "nav": account.get("nav"),
            "unrealized_pl": account.get("unrealized_pl"),
            "margin_used": account.get("margin_used"),
            "margin_available": account.get("margin_available"),
            "currency": account.get("currency") or "GBP",
            "gold_open_trades": len(gold_trades),
            "gold_unrealized_pl": gold_unrealized,
        }

    def _runtime_snapshot(self, state: dict) -> dict:
        runtime_status = load_runtime_status(self.bot_status_key, str(self.bot_status_path)) or {}
        state_balance = state.get("account_balance")
        state_currency = state.get("account_currency")
        return {
            "worker_state": runtime_status.get("state"),
            "worker_heartbeat": runtime_status.get("generated_at"),
            "last_run_at": runtime_status.get("last_run_at") or state.get("last_run_at"),
            "last_session": runtime_status.get("last_session") or state.get("last_session"),
            "skip_reason": runtime_status.get("skip_reason") or state.get("skip_reason"),
            "worker_error": runtime_status.get("error") or state.get("last_error"),
            "paused": runtime_status.get("paused") if "paused" in runtime_status else bool(state.get("paused", False)),
            "open_trade_count": int(runtime_status.get("open_trades", len(state.get("open_trades", []))) or 0),
            "balance": state_balance if state_balance is not None else runtime_status.get("balance"),
            "account_currency": state_currency or runtime_status.get("account_currency") or "GBP",
        }

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _format_timestamp(cls, value: str | None) -> str:
        parsed = cls._parse_timestamp(value)
        if parsed is None:
            return "never"
        now = datetime.now(timezone.utc)
        day_delta = (parsed.date() - now.date()).days
        if day_delta == 0:
            prefix = "Today"
        elif day_delta == -1:
            prefix = "Yesterday"
        else:
            prefix = parsed.strftime("%a %d %b %Y")
        return f"{prefix} at {parsed.strftime('%H:%M')} UTC"

    @staticmethod
    def _format_currency(amount: float | int | str | None, currency: str | None = "GBP") -> str:
        if amount is None or amount == "":
            return "n/a"
        try:
            numeric = float(amount)
        except (TypeError, ValueError):
            return escape(str(amount))
        code = (currency or "GBP").strip().upper()
        return f"{code}{numeric:,.2f}"

    @staticmethod
    def _format_bool(value: bool) -> str:
        return "Paused" if value else "Running"

    @staticmethod
    def _format_reason(reason: str | None) -> str:
        if not reason:
            return "none"
        mapping = {
            "no_signal": "no signal",
            "open_gold_position": "existing gold position",
            "pre_news_pause": "pre-news pause",
            "risk_budget_exhausted": "risk budget exhausted",
            "size_zero": "size rounded to zero",
            "spread_too_wide": "spread too wide",
            "missing_candles": "waiting for candle history",
            "missing_usd_proxy_candles": "waiting for USD proxy history",
            "paused_manual": "paused manually",
        }
        cleaned = mapping.get(reason, str(reason).replace("_", " "))
        return escape(cleaned)

    @staticmethod
    def _format_session(session: str | None) -> str:
        name = (session or "unknown").strip().upper()
        labels = {
            "OFF_HOURS": "🌙 OFF_HOURS",
            "ASIA": "🌏 ASIA",
            "LONDON": "🇬🇧 LONDON",
            "OVERLAP": "🔥 OVERLAP",
            "NEW_YORK": "🗽 NEW_YORK",
            "UNKNOWN": "❔ UNKNOWN",
        }
        return labels.get(name, escape(name))

    @staticmethod
    def _format_worker_state(state_name: str | None, has_recent_run: bool) -> str:
        if not state_name:
            return "🟠 No heartbeat" if has_recent_run else "🔴 Offline"
        name = str(state_name).strip().lower()
        labels = {
            "booting": "🟡 Booting",
            "idle": "🟢 Idle",
            "paused": "⏸️ Paused",
            "error": "🔴 Error",
            "trade_opened": "🟢 Trade opened",
            "active_trade": "🟢 Managing trade",
            "scanning": "🟢 Scanning",
            "waiting_data": "🟡 Waiting for data",
            "pre_news_pause": "🟡 Pre-news pause",
            "risk_budget_exhausted": "🟠 Risk capped",
            "size_zero": "🟠 Size zero",
            "spread_too_wide": "🟠 Spread blocked",
        }
        return labels.get(name, escape(name.replace("_", " ").title()))

    @staticmethod
    def _direction_emoji(direction: str | None) -> str:
        return "🟢" if str(direction or "").upper() == "LONG" else "🔴"

    @staticmethod
    def _event_emoji(event_type: str | None) -> str:
        event_name = str(event_type or "").lower()
        mapping = {
            "trade_opened": "🟢",
            "trade_closed": "⚪",
            "partial_profit": "💰",
            "break_even": "🛡️",
            "manual_pause": "⏸️",
            "manual_resume": "▶️",
            "sync": "🔄",
            "close_all": "🛑",
            "spread_too_wide": "🟠",
        }
        return mapping.get(event_name, "ℹ️")

    def _build_status_message(self, state: dict) -> str:
        open_trades = list(state.get("open_trades", []))
        snapshot = self._runtime_snapshot(state)
        broker = self._load_broker_snapshot()
        currency = (broker or {}).get("currency") or snapshot["account_currency"] or "GBP"
        balance = (broker or {}).get("balance", snapshot["balance"])
        nav = (broker or {}).get("nav")
        unrealized = (broker or {}).get("unrealized_pl")
        gold_unrealized = (broker or {}).get("gold_unrealized_pl")
        margin_used = (broker or {}).get("margin_used")
        margin_available = (broker or {}).get("margin_available")
        open_trade_count = int((broker or {}).get("gold_open_trades", snapshot["open_trade_count"]))
        budget_snapshot = None
        if balance is not None:
            try:
                budget_snapshot = self.budget.build_snapshot(float(balance))
            except Exception:
                budget_snapshot = None
        lines = [
            f"📊 <b>Gold Status</b> | {self._format_session(snapshot['last_session'])}",
            "━━━━━━━━━━━━━━━",
            f"🤖 Worker: {self._format_worker_state(snapshot['worker_state'], bool(snapshot['last_run_at']))}",
            f"💓 Worker heartbeat: {escape(self._format_timestamp(snapshot['worker_heartbeat']))}",
            f"🕒 Last run: {escape(self._format_timestamp(snapshot['last_run_at']))}",
        ]
        if nav is not None:
            lines.append(f"NAV: {escape(self._format_currency(nav, currency))}")
        if balance is not None:
            lines.append(f"💰 Balance: {escape(self._format_currency(balance, currency))}")
        if unrealized is not None:
            unrealized_text = escape(self._format_currency(unrealized, currency))
            if gold_unrealized is not None:
                unrealized_text += f" | gold {escape(self._format_currency(gold_unrealized, currency))}"
            lines.append(f"📉 Unrealized: {unrealized_text}")
        if margin_used is not None:
            lines.append(f"Margin used: {escape(self._format_currency(margin_used, currency))}")
        if margin_available is not None:
            lines.append(f"Margin available: {escape(self._format_currency(margin_available, currency))}")
        if budget_snapshot is not None:
            lines.append(f"🛡️ Gold sleeve: {escape(self._format_currency(budget_snapshot.gold_sleeve_balance, currency))}")
            lines.append(f"Gold reserved: {escape(self._format_currency(budget_snapshot.reserved_gold_risk, currency))}")
            lines.append(f"FX reserved: {escape(self._format_currency(budget_snapshot.sibling_fx_reserved_risk, currency))}")
            lines.append(f"Available gold risk: {escape(self._format_currency(budget_snapshot.available_gold_risk, currency))}")
        lines.extend(
            [
                f"Open trades: {open_trade_count}",
                f"⏯️ Bot: {self._format_bool(bool(snapshot['paused']))}",
                f"⏭️ Last skip: {self._format_reason(snapshot['skip_reason'])}",
            ]
        )
        if snapshot.get("worker_error"):
            lines.append(f"⚠️ Worker error: {escape(str(snapshot['worker_error']))}")
        last_signal = state.get("last_signal")
        if isinstance(last_signal, dict) and last_signal:
            strategy = escape(str(last_signal.get("strategy", "unknown")))
            direction = escape(str(last_signal.get("direction", "unknown")))
            score = last_signal.get("score", "n/a")
            lines.append(
                f"🎯 Last setup: {self._direction_emoji(direction)} {strategy} {direction} | score {escape(str(score))}"
            )
        if open_trades:
            lines.append("")
            lines.append("📂 <b>Open trades</b>")
            for trade in open_trades:
                opened_at = self._format_timestamp(trade.get("opened_at"))
                lines.append(
                    f"{self._direction_emoji(trade.get('direction'))} {escape(str(trade.get('strategy', 'TRADE')))} "
                    f"{escape(str(trade.get('direction', '')))} | entry {escape(str(trade.get('entry_price', 'n/a')))} "
                    f"| stop {escape(str(trade.get('stop_price', 'n/a')))} | opened {escape(opened_at)}"
                )
        return "\n".join(lines)

    def _build_last_signal_message(self, state: dict) -> str:
        signal = state.get("last_signal")
        if not signal:
            return "🧠 <b>Last Signal</b>\n━━━━━━━━━━━━━━━\nNo Gold-bot signal recorded yet."
        result = signal.get("result", {})
        runtime = self._runtime_snapshot(state)
        take_profit = signal.get("take_profit_price")
        rationale = escape(str(signal.get("rationale", "n/a")))
        lines = [
            "🧠 <b>Last Signal</b>",
            "━━━━━━━━━━━━━━━",
            f"🕒 Seen: {escape(self._format_timestamp(runtime['last_run_at']))}",
            f"Strategy: {escape(str(signal.get('strategy', 'unknown')))}",
            f"Direction: {self._direction_emoji(signal.get('direction'))} {escape(str(signal.get('direction', 'unknown')))}",
            f"Score: {escape(str(signal.get('score', 'n/a')))}",
            f"Entry: {escape(str(signal.get('entry_price', 'n/a')))} | Stop: {escape(str(signal.get('stop_price', 'n/a')))}",
            f"Take profit: {escape(str(take_profit if take_profit is not None else 'n/a'))}",
            f"Risk: {escape(self._format_currency(signal.get('risk_amount'), runtime['account_currency']))} | Size: {escape(str(signal.get('size', 'n/a')))}",
            f"Mode: {escape(str(result.get('mode', 'n/a')))}",
            f"Why: {rationale}",
        ]
        return "\n".join(lines)

    def _build_recent_events_message(self, state: dict) -> str:
        events = list(state.get("events", []))[-5:]
        if not events:
            return "🗂️ <b>Recent Events</b>\n━━━━━━━━━━━━━━━\nNo recent Gold-bot events."
        lines = ["🗂️ <b>Recent Events</b>", "━━━━━━━━━━━━━━━"]
        for event in events:
            lines.append(
                f"{self._event_emoji(event.get('type'))} {escape(self._format_timestamp(event.get('timestamp')))} | "
                f"{escape(str(event.get('type', '')).replace('_', ' '))}"
            )
            lines.append(escape(str(event.get("message", ""))))
        return "\n".join(lines)

    def _build_open_trades_message(self, state: dict) -> str:
        open_trades = list(state.get("open_trades", []))
        if not open_trades:
            return "📂 <b>Open Gold Trades</b>\n━━━━━━━━━━━━━━━\nNo open gold trades."
        lines = ["📂 <b>Open Gold Trades</b>", "━━━━━━━━━━━━━━━"]
        for trade in open_trades:
            lines.append(
                f"{self._direction_emoji(trade.get('direction'))} {escape(str(trade.get('strategy', 'TRADE')))} "
                f"{escape(str(trade.get('direction', '')))}"
            )
            lines.append(
                f"Entry {escape(str(trade.get('entry_price', 'n/a')))} | Stop {escape(str(trade.get('stop_price', 'n/a')))}"
            )
            lines.append(
                f"Size {escape(str(trade.get('remaining_size', trade.get('size', 'n/a'))))} | "
                f"Risk {escape(str(trade.get('risk_amount', 'n/a')))} | "
                f"Opened {escape(self._format_timestamp(trade.get('opened_at')))}"
            )
        return "\n".join(lines)

    def _build_risk_message(self, state: dict) -> str:
        runtime = self._runtime_snapshot(state)
        broker = self._load_broker_snapshot()
        currency = (broker or {}).get("currency") or runtime.get("account_currency") or state.get("account_currency") or "GBP"
        balance = float((broker or {}).get("balance", runtime.get("balance", 0.0)) or 0.0)
        if balance <= 0:
            return "🛡️ <b>Gold Risk</b>\n━━━━━━━━━━━━━━━\nRisk snapshot unavailable until the Gold runtime publishes account balance."
        snapshot = self.budget.build_snapshot(balance)
        open_risk = sum(float(trade.get("risk_amount", 0.0) or 0.0) for trade in state.get("open_trades", []))
        return (
            "🛡️ <b>Gold Risk</b>\n"
            "━━━━━━━━━━━━━━━\n"
            f"Balance: {escape(self._format_currency(snapshot.account_balance, currency))}\n"
            f"Gold sleeve: {escape(self._format_currency(snapshot.gold_sleeve_balance, currency))}\n"
            f"Max per trade: {escape(self._format_currency(snapshot.max_trade_risk_amount, currency))}\n"
            f"Max total: {escape(self._format_currency(snapshot.max_total_risk_amount, currency))}\n"
            f"Gold reserved: {escape(self._format_currency(snapshot.reserved_gold_risk, currency))}\n"
            f"FX reserved: {escape(self._format_currency(snapshot.sibling_fx_reserved_risk, currency))}\n"
            f"Available gold risk: {escape(self._format_currency(snapshot.available_gold_risk, currency))}\n"
            f"Tracked open risk: {escape(self._format_currency(open_risk, currency))}"
        )

    def _build_heartbeat_message(self) -> str:
        state = self._load_state()
        runtime = self._runtime_snapshot(state)
        open_count = runtime["open_trade_count"]
        return (
            "💓 <b>Gold Telegram Heartbeat</b>\n"
            "━━━━━━━━━━━━━━━\n"
            f"Last runtime update: {escape(self._format_timestamp(runtime['last_run_at']))}\n"
            f"Worker: {self._format_worker_state(runtime['worker_state'], bool(runtime['last_run_at']))}\n"
            f"Open trades: {open_count}\n"
            f"Bot: {self._format_bool(bool(runtime['paused']))}"
        )

    @staticmethod
    def _format_event(event: dict) -> str:
        timestamp = GoldTelegramClient._format_timestamp(event.get("timestamp"))
        event_type = escape(str(event.get("type", "")).replace("_", " "))
        message = escape(str(event.get("message", "")))
        return (
            "🔔 <b>Gold Event</b>\n"
            "━━━━━━━━━━━━━━━\n"
            f"{escape(timestamp)}\n"
            f"{event_type}\n"
            f"{message}"
        )

    def _load_state(self) -> dict:
        state = load_json_payload(str(self.state_path), self.state_key, {"events": [], "signals": [], "open_trades": [], "paused": False})
        state.setdefault("events", [])
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("control_requests", [])
        state.setdefault("paused", False)
        return state

    def _save_state(self, state: dict) -> None:
        state.setdefault("events", [])
        state.setdefault("signals", [])
        state.setdefault("open_trades", [])
        state.setdefault("control_requests", [])
        state.setdefault("paused", False)
        save_json_payload(str(self.state_path), state, self.state_key)

    def _load_offset(self) -> dict:
        if not self.offset_path.exists():
            return {"last_update_id": 0, "sent_event_ids": []}
        try:
            return json.loads(self.offset_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"last_update_id": 0, "sent_event_ids": []}

    def _save_offset(self, payload: dict) -> None:
        self.offset_path.parent.mkdir(parents=True, exist_ok=True)
        self.offset_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _append_event(state: dict, event_type: str, message: str) -> None:
        events = state.setdefault("events", [])
        events.append(
            {
                "id": f"manual-{int(time.time() * 1000)}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": event_type,
                "message": message,
            }
        )
        if len(events) > 200:
            del events[:-200]

    @staticmethod
    def _append_control_request(state: dict, command: str) -> None:
        queue = state.setdefault("control_requests", [])
        queue.append(
            {
                "id": f"control-{int(time.time() * 1000)}",
                "command": command,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if len(queue) > 50:
            del queue[:-50]

    def _publish_status(self, state_name: str) -> None:
        state = self._load_state()
        publish_runtime_status(
            service="gold-telegram",
            state=state_name,
            redis_key=self.status_key,
            ttl_seconds=self.status_ttl,
            file_path=str(self.status_path),
            open_trades=len(state.get("open_trades", [])),
            paused=bool(state.get("paused", False)),
            last_run_at=state.get("last_run_at"),
        )