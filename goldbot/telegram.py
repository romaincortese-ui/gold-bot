import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from goldbot.budget import SharedBudgetManager
from goldbot.config import load_settings
from goldbot.shared_backend import load_json_payload, publish_runtime_status, save_json_payload


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
        self.token = token
        self.chat_id = str(chat_id)
        self.state_path = state_path
        self.offset_path = offset_path
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.state_key = os.getenv("GOLD_RUNTIME_STATE_KEY", "gold_runtime_state").strip()
        self.status_key = os.getenv("GOLD_TELEGRAM_STATUS_KEY", "gold_telegram_runtime_status").strip()
        self.status_ttl = int(os.getenv("GOLD_STATUS_TTL", "1800"))
        self.budget = SharedBudgetManager(load_settings())

    def run_forever(self, *, poll_seconds: int, heartbeat_minutes: int) -> None:
        self.send_message("Gold Telegram worker online. Use /help for commands.")
        last_heartbeat = 0.0
        while True:
            try:
                self.flush_new_events()
                self.poll_commands()
                now = time.time()
                if heartbeat_minutes > 0 and now - last_heartbeat >= heartbeat_minutes * 60:
                    self.send_message(self._build_heartbeat_message())
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
            json={"chat_id": self.chat_id, "text": message},
            timeout=10,
        )
        response.raise_for_status()

    def _handle_command(self, text: str) -> str:
        state = self._load_state()
        command = text.lower().split()[0]
        if command == "/help":
            return "Supported commands: /status, /last, /events, /open, /risk, /pause, /resume, /sync, /closeall"
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
        return "Supported commands: /status, /last, /events, /open, /risk, /pause, /resume, /sync, /closeall"

    def _build_status_message(self, state: dict) -> str:
        open_trades = list(state.get("open_trades", []))
        last_run_at = state.get("last_run_at") or "never"
        session = state.get("last_session") or "unknown"
        skip_reason = state.get("skip_reason") or "none"
        return (
            f"Gold-bot status\n"
            f"Last run: {last_run_at}\n"
            f"Session: {session}\n"
            f"Open trades: {len(open_trades)}\n"
            f"Paused: {state.get('paused', False)}\n"
            f"Last skip reason: {skip_reason}"
        )

    def _build_last_signal_message(self, state: dict) -> str:
        signal = state.get("last_signal")
        if not signal:
            return "No Gold-bot signal recorded yet."
        result = signal.get("result", {})
        return (
            f"Last signal\n"
            f"Strategy: {signal.get('strategy', 'unknown')}\n"
            f"Direction: {signal.get('direction', 'unknown')}\n"
            f"Score: {signal.get('score', 'n/a')}\n"
            f"Entry: {signal.get('entry_price', 'n/a')}\n"
            f"Stop: {signal.get('stop_price', 'n/a')}\n"
            f"Mode: {result.get('mode', 'n/a')}"
        )

    def _build_recent_events_message(self, state: dict) -> str:
        events = list(state.get("events", []))[-5:]
        if not events:
            return "No recent Gold-bot events."
        lines = ["Recent events"]
        for event in events:
            lines.append(f"- {event.get('timestamp', '')} | {event.get('type', '')} | {event.get('message', '')}")
        return "\n".join(lines)

    def _build_open_trades_message(self, state: dict) -> str:
        open_trades = list(state.get("open_trades", []))
        if not open_trades:
            return "No open gold trades."
        lines = ["Open gold trades"]
        for trade in open_trades:
            lines.append(
                f"- {trade.get('strategy', '')} {trade.get('direction', '')} | entry {trade.get('entry_price', '')} | stop {trade.get('stop_price', '')} | remaining {trade.get('remaining_size', '')}"
            )
        return "\n".join(lines)

    def _build_risk_message(self, state: dict) -> str:
        balance = float(state.get("account_balance", 0.0) or 0.0)
        if balance <= 0:
            return "Risk snapshot unavailable until the Gold runtime publishes account balance."
        snapshot = self.budget.build_snapshot(balance)
        open_risk = sum(float(trade.get("risk_amount", 0.0) or 0.0) for trade in state.get("open_trades", []))
        return (
            f"Gold risk\n"
            f"Account balance: {snapshot.account_balance:.2f}\n"
            f"Gold sleeve: {snapshot.gold_sleeve_balance:.2f}\n"
            f"Gold reserved: {snapshot.reserved_gold_risk:.2f}\n"
            f"FX reserved: {snapshot.sibling_fx_reserved_risk:.2f}\n"
            f"Available gold risk: {snapshot.available_gold_risk:.2f}\n"
            f"Tracked open risk: {open_risk:.2f}"
        )

    def _build_heartbeat_message(self) -> str:
        state = self._load_state()
        last_run = state.get("last_run_at") or "never"
        open_count = len(state.get("open_trades", []))
        return f"Gold Telegram heartbeat\nLast runtime update: {last_run}\nOpen trades: {open_count}"

    @staticmethod
    def _format_event(event: dict) -> str:
        return f"Gold event\n{event.get('timestamp', '')}\n{event.get('type', '')}\n{event.get('message', '')}"

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
            open_trades=len(state.get("open_trades", [])),
            paused=bool(state.get("paused", False)),
            last_run_at=state.get("last_run_at"),
        )