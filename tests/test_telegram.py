from datetime import datetime, timezone
from pathlib import Path

from goldbot.telegram import GoldTelegramClient
import goldbot.telegram as telegram_module


def test_build_status_message_reports_core_runtime_fields(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client.bot_status_key = "gold_bot_runtime_status"
    client.bot_status_path = tmp_path / "gold_bot_runtime_status.json"
    original_load_runtime_status = telegram_module.load_runtime_status
    telegram_module.load_runtime_status = lambda key, file_path=None, max_age_seconds=None: None
    client._load_broker_snapshot = lambda: None
    message = client._build_status_message(
        {
            "last_run_at": "2026-04-06T12:00:00+00:00",
            "last_session": "OVERLAP",
            "skip_reason": "none",
            "paused": True,
            "open_trades": [{"id": "1"}],
        }
    )
    telegram_module.load_runtime_status = original_load_runtime_status

    assert "📊 <b>Gold Status</b>" in message
    assert "Last run: " in message
    assert "Open trades: 1" in message
    assert "⏯️ Bot: Paused" in message
    assert "🤖 Worker: 🟠 No heartbeat" in message


def test_build_recent_events_message_uses_latest_events(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    message = client._build_recent_events_message(
        {
            "events": [
                {"timestamp": "2026-04-06T12:00:00+00:00", "type": "trade_opened", "message": "Opened long"},
                {"timestamp": "2026-04-06T12:05:00+00:00", "type": "break_even", "message": "Moved stop"},
            ]
        }
    )

    assert "trade opened" in message
    assert "Moved stop" in message


def test_status_message_labels_paper_balance_in_signal_only_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client._load_broker_snapshot = lambda: None
    message = client._build_status_message(
        {
            "last_run_at": "2026-04-07T08:22:00+00:00",
            "last_session": "LONDON",
            "skip_reason": "no_signal",
            "paused": False,
            "open_trades": [],
            "account_balance": 10000.0,
            "account_currency": "GBP",
        }
    )

    assert "💰 Balance: GBP10,000.00 (paper)" in message


def test_handle_unknown_command_returns_help(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=Path(tmp_path / "state.json"),
        offset_path=Path(tmp_path / "telegram_state.json"),
    )
    reply = client._handle_command("/wat")
    assert "Gold Bot Commands" in reply


def test_pause_and_resume_commands_queue_control_requests(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=Path(tmp_path / "state.json"),
        offset_path=Path(tmp_path / "telegram_state.json"),
    )
    client._save_state({"events": [], "signals": [], "open_trades": [], "paused": False})

    pause_reply = client._handle_command("/pause")
    paused_state = client._load_state()
    resume_reply = client._handle_command("/resume")
    resumed_state = client._load_state()

    assert "queued" in pause_reply.lower()
    assert paused_state["paused"] is False
    assert paused_state["control_requests"][0]["command"] == "pause"
    assert "queued" in resume_reply.lower()
    assert resumed_state["paused"] is False
    assert [request["command"] for request in resumed_state["control_requests"]] == ["pause", "resume"]


def test_sync_and_closeall_commands_queue_control_requests(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=Path(tmp_path / "state.json"),
        offset_path=Path(tmp_path / "telegram_state.json"),
    )
    client._save_state({"events": [], "signals": [], "open_trades": [], "paused": False})

    sync_reply = client._handle_command("/sync")
    close_reply = client._handle_command("/closeall")
    state = client._load_state()

    assert "queued" in sync_reply.lower()
    assert "queued" in close_reply.lower()
    assert [request["command"] for request in state["control_requests"]] == ["sync", "close_all"]


def test_risk_command_uses_budget_snapshot(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=Path(tmp_path / "state.json"),
        offset_path=Path(tmp_path / "telegram_state.json"),
    )
    client._load_broker_snapshot = lambda: None
    message = client._build_risk_message(
        {
            "account_balance": 10000.0,
            "open_trades": [{"risk_amount": 25.0}],
        }
    )

    assert "Balance: GBP10,000.00" in message
    assert "Tracked open risk: GBP25.00" in message


def test_heartbeat_prefers_runtime_status_when_state_is_stale(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    original_load_runtime_status = telegram_module.load_runtime_status
    telegram_module.load_runtime_status = lambda key, file_path=None, max_age_seconds=None: {
        "state": "idle",
        "generated_at": "2026-04-06T21:55:00+00:00",
        "last_run_at": "2026-04-06T21:54:28+00:00",
        "open_trades": 2,
        "paused": False,
    }
    client._save_state({"events": [], "signals": [], "open_trades": [], "paused": False})

    message = client._build_heartbeat_message()

    telegram_module.load_runtime_status = original_load_runtime_status

    assert "Last runtime update: " in message
    assert "never" not in message
    assert "Worker: 🟢 Idle" in message
    assert "Open trades: 2" in message


def test_status_message_uses_file_backed_runtime_status_when_redis_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client.bot_status_path = tmp_path / "gold_bot_runtime_status.json"
    now = datetime.now(timezone.utc)
    client.bot_status_path.write_text(
        (
            '{\n'
            f'  "state": "idle",\n'
            f'  "generated_at": "{now.isoformat()}",\n'
            f'  "last_run_at": "{now.isoformat()}",\n'
            '  "last_session": "ASIA",\n'
            '  "open_trades": 0,\n'
            '  "paused": false\n'
            '}'
        ),
        encoding="utf-8",
    )
    original_load_runtime_status = telegram_module.load_runtime_status
    telegram_module.load_runtime_status = original_load_runtime_status
    client._load_broker_snapshot = lambda: None

    message = client._build_status_message({"events": [], "signals": [], "open_trades": [], "paused": False})

    assert "🤖 Worker: 🟢 Idle" in message
    assert "💓 Worker heartbeat: Today at " in message


def test_status_message_ignores_stale_file_backed_runtime_status(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client.status_ttl = 60
    client.bot_status_path = tmp_path / "gold_bot_runtime_status.json"
    client.bot_status_path.write_text(
        '{\n  "state": "scanning",\n  "generated_at": "2026-04-07T08:22:00+00:00",\n  "last_run_at": "2026-04-07T08:22:00+00:00",\n  "last_session": "LONDON",\n  "open_trades": 0,\n  "paused": false\n}',
        encoding="utf-8",
    )
    client._load_broker_snapshot = lambda: None

    message = client._build_status_message({"events": [], "signals": [], "open_trades": [], "paused": False})

    assert "🤖 Worker: 🔴 Offline" in message
    assert "💓 Worker heartbeat: never" in message


def test_status_message_surfaces_worker_error(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client.bot_status_path = tmp_path / "gold_bot_runtime_status.json"
    now = datetime.now(timezone.utc)
    client.bot_status_path.write_text(
        (
            '{\n'
            '  "state": "error",\n'
            f'  "generated_at": "{now.isoformat()}",\n'
            '  "error": "boom"\n'
            '}'
        ),
        encoding="utf-8",
    )
    client._load_broker_snapshot = lambda: None

    message = client._build_status_message({"events": [], "signals": [], "open_trades": [], "paused": False})

    assert "🤖 Worker: 🔴 Error" in message
    assert "⚠️ Worker error: boom" in message


def test_status_message_surfaces_broker_snapshot_error(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    message = client._build_status_message(
        {
            "events": [],
            "signals": [],
            "open_trades": [],
            "paused": False,
            "account_balance": 11000.0,
            "account_nav": 11120.0,
            "account_unrealized_pl": 120.0,
            "account_margin_used": 220.0,
            "account_margin_available": 10880.0,
            "account_currency": "GBP",
            "execution_mode": "live",
        }
    )

    assert "NAV: GBP11,120.00" in message
    assert "📉 Unrealized: GBP120.00" in message
    assert "Margin used: GBP220.00" in message
    assert "Margin available: GBP10,880.00" in message