import json
from datetime import datetime, timezone

from goldbot.runtime import GoldBotRuntime


def _disable_redis(monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)


def test_manage_open_trade_moves_break_even_and_partial(tmp_path, monkeypatch) -> None:
    _disable_redis(monkeypatch)
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
    _disable_redis(monkeypatch)
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
    monkeypatch.setattr(runtime, "_session_name", lambda now: "LONDON")

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "open_gold_position"


def test_run_cycle_honors_manual_pause_state(tmp_path, monkeypatch) -> None:
    _disable_redis(monkeypatch)
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    runtime._save_state({"signals": [], "open_trades": [], "events": [], "paused": True})

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3000.0, "ask": 3000.2, "mid": 3000.1, "spread": 0.2})

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "paused_manual"


def test_run_cycle_applies_queued_pause_request(tmp_path, monkeypatch) -> None:
    _disable_redis(monkeypatch)
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
    _disable_redis(monkeypatch)
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


def test_publish_runtime_status_sends_heartbeat_when_interval_elapsed(monkeypatch) -> None:
    _disable_redis(monkeypatch)
    runtime = GoldBotRuntime()
    sent_messages: list[str] = []

    runtime.telegram_token = "token"
    runtime.telegram_chat_id = "chat"
    runtime.heartbeat_interval = 60
    runtime.last_heartbeat_at = 0.0

    monkeypatch.setattr("goldbot.runtime.publish_runtime_status", lambda *args, **kwargs: True)
    monkeypatch.setattr(runtime, "_send_telegram_message", lambda message: sent_messages.append(message))

    state = {
        "last_run_at": "2026-04-06T12:00:00+00:00",
        "last_session": "LONDON",
        "skip_reason": "none",
        "open_trades": [],
        "paused": False,
    }

    runtime._publish_runtime_status("idle", state, balance=10000.0)

    assert sent_messages
    assert "Gold-bot heartbeat" in sent_messages[0]
    assert "State: idle" in sent_messages[0]


def test_await_entry_quote_requires_macro_spread_stability(monkeypatch) -> None:
    _disable_redis(monkeypatch)
    runtime = GoldBotRuntime()
    runtime.settings = type(runtime.settings)(
        **{
            **runtime.settings.__dict__,
            "execution_mode": "live",
            "macro_breakout_spread_settle_seconds": 5,
            "macro_breakout_spread_stability_checks": 2,
            "macro_breakout_spread_stability_tolerance": 0.05,
        }
    )
    quotes = iter(
        [
            {"bid": 3000.0, "ask": 3001.4, "mid": 3000.7, "spread": 1.4},
            {"bid": 3000.0, "ask": 3000.4, "mid": 3000.2, "spread": 0.4},
            {"bid": 3000.0, "ask": 3000.42, "mid": 3000.21, "spread": 0.42},
        ]
    )

    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: next(quotes))
    monkeypatch.setattr("goldbot.runtime.time.sleep", lambda _: None)

    quote = runtime._await_entry_quote(type("OpportunityStub", (), {"strategy": "MACRO_BREAKOUT"})())

    assert quote["spread"] == 0.42


def test_run_cycle_scales_risk_when_real_yields_are_adverse(tmp_path, monkeypatch) -> None:
    _disable_redis(monkeypatch)
    runtime = GoldBotRuntime()
    runtime.state_path = tmp_path / "state.json"
    runtime._save_state({"signals": [], "open_trades": [], "events": []})
    runtime.settings = type(runtime.settings)(
        **{
            **runtime.settings.__dict__,
            "real_yield_filter_enabled": True,
            "macro_state_file": str(tmp_path / "macro.json"),
            "execution_mode": "paper",
        }
    )
    runtime.client.settings = runtime.settings
    (tmp_path / "macro.json").write_text(
        json.dumps(
            {
                "real_yields": {
                    "as_of": "2026-04-07T12:00:00+00:00",
                    "nominal_10y": 4.2,
                    "tips_10y": 2.1,
                    "real_yield_10y": 2.1,
                    "real_yield_change_bps": 10.0,
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "_session_name", lambda now: "LONDON")
    monkeypatch.setattr(runtime.client, "get_account_summary", lambda: {"balance": 10000.0, "currency": "GBP"})
    monkeypatch.setattr(runtime.client, "fetch_candles", lambda instrument, granularity, count: __import__("pandas").DataFrame([{"time": datetime.now(timezone.utc), "open": 3000.0, "high": 3001.0, "low": 2999.0, "close": 3000.5, "volume": 100}] * 260))
    monkeypatch.setattr(runtime.client, "calculate_xau_size", lambda risk_amount, stop_distance, account_currency: round(risk_amount, 2))
    monkeypatch.setattr(runtime.client, "place_market_order", lambda opportunity, size, quote=None: {"id": "paper-1", "price": 3000.5, "mode": "paper"})
    monkeypatch.setattr(runtime.client, "get_price", lambda instrument: {"bid": 3000.4, "ask": 3000.6, "mid": 3000.5, "spread": 0.2})
    monkeypatch.setattr("goldbot.runtime.fetch_calendar_events", lambda *args, **kwargs: [])
    monkeypatch.setattr("goldbot.runtime.filter_gold_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "goldbot.runtime.score_macro_breakout",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "goldbot.runtime.score_exhaustion_reversal",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "goldbot.runtime.score_trend_pullback",
        lambda *args, **kwargs: __import__("goldbot.models", fromlist=["Opportunity"]).Opportunity(
            strategy="TREND_PULLBACK",
            direction="LONG",
            score=75.0,
            entry_price=3000.5,
            stop_price=2995.5,
            take_profit_price=None,
            risk_per_unit=5.0,
            rationale="test",
            metadata={},
            exit_plan={},
        ),
    )

    runtime.run_cycle()
    saved = json.loads(runtime.state_path.read_text(encoding="utf-8"))

    assert saved["last_signal"]["risk_amount"] == 18.75