import json
from datetime import datetime, timezone

from goldbot.runtime import GoldBotRuntime


def test_manage_open_trade_moves_break_even_and_partial(tmp_path, monkeypatch) -> None:
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"

    state = {
        "signals": [],
        "open_trades": [
            {
                "id": "paper-1",
                "instrument": "XAU_USD",
                "strategy": "TREND_PULLBACK",
                "direction": "LONG",
                "entry_price": 3000.0,
                "stop_price": 2995.0,
                "initial_stop_price": 2995.0,
                "initial_risk_per_unit": 5.0,
                "size": 2.0,
                "remaining_size": 2.0,
                "risk_amount": 10.0,
                "partial_taken": False,
                "break_even_moved": False,
                "exit_plan": {
                    "partial_take_profit_fraction": 0.5,
                    "partial_take_profit_price": 3005.0,
                    "break_even_trigger_price": 3005.0,
                    "trail_timeframe": "H1",
                    "trail_ema_period": 20,
                    "trail_atr_mult": 2.2,
                    "trailing_stop_distance": 3.0,
                },
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
    }
    runtime._save_state(state)

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3008.0, "ask": 3008.2, "mid": 3008.1, "spread": 0.2})
    monkeypatch.setattr(runtime.client, "close_trade", lambda trade_id, size=None: True)
    monkeypatch.setattr(runtime.client, "modify_trade", lambda trade_id, stop_price=None: True)
    monkeypatch.setattr(
        runtime.client,
        "fetch_candles",
        lambda instrument, granularity, count: __import__("pandas").DataFrame(
            [{"close": 3000.0 + index * 0.5, "high": 3001.0 + index * 0.5, "low": 2999.0 + index * 0.5, "open": 3000.0 + index * 0.5, "volume": 100} for index in range(40)]
        ),
    )

    loaded = runtime._load_state()
    runtime._manage_open_trades(loaded)
    updated = runtime._load_state()
    trade = updated["open_trades"][0]

    assert trade["partial_taken"] is True
    assert trade["break_even_moved"] is True
    assert trade["remaining_size"] == 1.0
    assert trade["stop_price"] >= 3000.0


def test_run_cycle_skips_new_entry_when_state_trade_exists(tmp_path, monkeypatch) -> None:
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    runtime._save_state(
        {
            "signals": [],
            "open_trades": [
                {
                    "id": "paper-1",
                    "instrument": "XAU_USD",
                    "strategy": "TREND_PULLBACK",
                    "direction": "LONG",
                    "entry_price": 3000.0,
                    "stop_price": 2995.0,
                    "initial_stop_price": 2995.0,
                    "initial_risk_per_unit": 5.0,
                    "size": 1.0,
                    "remaining_size": 1.0,
                    "risk_amount": 10.0,
                    "partial_taken": False,
                    "break_even_moved": False,
                    "exit_plan": {},
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }
    )

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3000.0, "ask": 3000.2, "mid": 3000.1, "spread": 0.2})
    monkeypatch.setattr(runtime.client, "fetch_candles", lambda instrument, granularity, count: None)
    monkeypatch.setattr(runtime.client, "get_account_summary", lambda: {"balance": 10000.0, "currency": "GBP"})

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "open_gold_position"


def test_run_cycle_honors_manual_pause_state(tmp_path, monkeypatch) -> None:
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    runtime._save_state({"signals": [], "open_trades": [], "events": [], "paused": True})

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3000.0, "ask": 3000.2, "mid": 3000.1, "spread": 0.2})

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "paused_manual"


def test_run_cycle_applies_queued_pause_request(tmp_path, monkeypatch) -> None:
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    runtime._save_state(
        {
            "signals": [],
            "open_trades": [],
            "events": [],
            "paused": False,
            "control_requests": [{"id": "control-1", "command": "pause", "requested_at": datetime.now(timezone.utc).isoformat()}],
        }
    )

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3000.0, "ask": 3000.2, "mid": 3000.1, "spread": 0.2})

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))

    assert saved["paused"] is True
    assert saved["skip_reason"] == "paused_manual"
    assert saved["control_requests"] == []
    assert any(event["type"] == "manual_pause" for event in saved["events"])


def test_process_control_requests_closes_all_trades(tmp_path, monkeypatch) -> None:
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    released_ids: list[str] = []
    state = {
        "signals": [],
        "events": [],
        "paused": False,
        "open_trades": [
            {
                "id": "paper-1",
                "instrument": "XAU_USD",
                "strategy": "TREND_PULLBACK",
                "direction": "LONG",
                "entry_price": 3000.0,
                "stop_price": 2995.0,
                "initial_stop_price": 2995.0,
                "initial_risk_per_unit": 5.0,
                "size": 1.0,
                "remaining_size": 1.0,
                "risk_amount": 10.0,
                "partial_taken": False,
                "break_even_moved": False,
                "exit_plan": {},
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
        "control_requests": [{"id": "control-1", "command": "close_all", "requested_at": datetime.now(timezone.utc).isoformat()}],
    }

    monkeypatch.setattr(runtime.client, "close_trade", lambda trade_id, size=None: True)
    monkeypatch.setattr(runtime.budget, "release_gold_risk", lambda trade_id: released_ids.append(trade_id))

    runtime._process_control_requests(state)

    assert state["open_trades"] == []
    assert state["control_requests"] == []
    assert released_ids == ["paper-1"]
    assert any(event["type"] == "trade_closed" for event in state["events"])