from pathlib import Path

from goldbot.telegram import GoldTelegramClient


def test_build_status_message_reports_core_runtime_fields(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    message = client._build_status_message(
        {
            "last_run_at": "2026-04-06T12:00:00+00:00",
            "last_session": "OVERLAP",
            "skip_reason": "none",
            "paused": True,
            "open_trades": [{"id": "1"}],
        }
    )

    assert "Last run: 2026-04-06T12:00:00+00:00" in message
    assert "Open trades: 1" in message
    assert "Paused: True" in message


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

    assert "trade_opened" in message
    assert "Moved stop" in message


def test_handle_unknown_command_returns_help(tmp_path) -> None:
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=Path(tmp_path / "state.json"),
        offset_path=Path(tmp_path / "telegram_state.json"),
    )
    reply = client._handle_command("/wat")
    assert "Supported commands" in reply


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
    message = client._build_risk_message(
        {
            "account_balance": 10000.0,
            "open_trades": [{"risk_amount": 25.0}],
        }
    )

    assert "Account balance: 10000.00" in message
    assert "Tracked open risk: 25.00" in message