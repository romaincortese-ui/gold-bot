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

    assert "Trade Opened" in message
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


def test_status_prefers_runtime_status_when_state_is_stale(tmp_path) -> None:
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
        "last_session": "LONDON",
        "open_trades": 2,
        "paused": False,
    }
    client._save_state({"events": [], "signals": [], "open_trades": [], "paused": False})

    message = client._build_status_message(client._load_state())

    telegram_module.load_runtime_status = original_load_runtime_status

    assert "Gold Status" in message
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
    assert "🏦 <b>Broker margin</b>" in message
    assert "Used: GBP220.00" in message
    assert "Available: GBP10,880.00" in message
    assert "not the Gold-bot budget" in message


def test_status_message_separates_margin_from_gold_risk_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )
    client.budget.path = tmp_path / "shared_budget_state.json"
    client.budget.redis_key = "test_shared_budget"
    client.budget.path.write_text(
        '{"bots":{"gold":{"reserved_risk":0.72,"trades":{"live-1":{"risk_amount":0.72,"strategy":"TREND_PULLBACK"}}}}}',
        encoding="utf-8",
    )

    message = client._build_status_message(
        {
            "last_run_at": "2026-04-27T13:25:00+00:00",
            "last_session": "OVERLAP",
            "skip_reason": "open_gold_position",
            "paused": False,
            "account_balance": 191.90,
            "account_nav": 197.02,
            "account_unrealized_pl": 5.12,
            "account_margin_used": 173.31,
            "account_margin_available": 23.97,
            "account_currency": "GBP",
            "execution_mode": "live",
            "open_trades": [
                {
                    "id": "live-1",
                    "instrument": "XAU_USD",
                    "strategy": "TREND_PULLBACK",
                    "direction": "SHORT",
                    "entry_price": 4705.81,
                    "stop_price": 4764.92126537547,
                    "initial_stop_price": 4764.92126537547,
                    "initial_risk_per_unit": 59.11126537547,
                    "size": 1.0,
                    "remaining_size": 1.0,
                    "risk_amount": 0.72,
                    "opened_at": "2026-04-27T09:01:00+00:00",
                    "exit_plan": {
                        "partial_take_profit_price": 4652.61,
                        "break_even_trigger_price": 4628.966,
                        "trail_timeframe": "H1",
                        "trailing_stop_distance": 165.511,
                    },
                }
            ],
        }
    )

    assert "🛡️ <b>Gold risk budget</b>" in message
    assert "Sleeve: GBP95.95" in message
    assert "Reserved by gold: GBP0.72" in message
    assert "🏦 <b>Broker margin</b>" in message
    assert "Used: GBP173.31" in message
    assert "not the Gold-bot budget" in message
    assert "Stop 4764.921" in message
    assert "Partial TP: 4652.610 (50%)" in message


def test_trade_opened_event_shows_budget_tp_and_sl() -> None:
    message = GoldTelegramClient._format_event(
        {
            "timestamp": "2026-04-27T09:01:00+00:00",
            "type": "trade_opened",
            "message": "legacy fallback",
            "details": {
                "instrument": "XAU_USD",
                "strategy": "TREND_PULLBACK",
                "direction": "SHORT",
                "entry_price": 4705.81,
                "stop_price": 4764.92126537547,
                "initial_stop_price": 4764.92126537547,
                "initial_risk_per_unit": 59.11126537547,
                "size": 1.0,
                "remaining_size": 1.0,
                "risk_amount": 0.72,
                "opened_at": "2026-04-27T09:01:00+00:00",
                "exit_plan": {
                    "partial_take_profit_price": 4652.61,
                    "break_even_trigger_price": 4628.966,
                    "trail_timeframe": "H1",
                    "trailing_stop_distance": 165.511,
                },
                "mode": "live",
                "score": 76.3069,
                "account_currency": "GBP",
                "gold_sleeve_balance": 95.95,
                "max_trade_risk_amount": 0.72,
                "max_total_risk_amount": 2.88,
                "reserved_gold_risk_after": 0.72,
                "available_gold_risk_after": 2.16,
            },
        }
    )

    assert "Gold: Trade Opened" in message
    assert "Entry 4705.810" in message
    assert "Stop 4764.921" in message
    assert "Partial TP: 4652.610 (50%)" in message
    assert "Break-even trigger: 4628.966" in message
    assert "Risk reserved: GBP0.72 / max trade GBP0.72" in message
    assert "OANDA margin shown in /status" in message


def test_poll_commands_swallows_read_timeout(tmp_path, monkeypatch, caplog) -> None:
    """Regression for the 80-line ReadTimeout traceback observed on 2026-04-19:
    a single transient api.telegram.org hiccup must not raise out of
    poll_commands; it should log a one-line warning and return."""
    import logging
    import requests

    client = GoldTelegramClient(
        token="token",
        chat_id="123",
        state_path=tmp_path / "state.json",
        offset_path=tmp_path / "telegram_state.json",
    )

    def _boom(*_args, **_kwargs):
        raise requests.exceptions.ReadTimeout("simulated read timeout")

    monkeypatch.setattr(telegram_module.requests, "get", _boom)

    with caplog.at_level(logging.WARNING, logger="goldbot.telegram"):
        client.poll_commands()  # must not raise

    assert any("Gold Telegram poll skipped" in rec.message for rec in caplog.records)