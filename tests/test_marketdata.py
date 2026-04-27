from datetime import datetime, timezone

import pytest

from goldbot.marketdata import OandaClient, SpreadTooWideError
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


def test_place_market_order_blocks_wide_spread(monkeypatch) -> None:
    client = OandaClient(build_settings())
    monkeypatch.setattr(
        client,
        "get_price",
        lambda instrument: {"bid": 3000.0, "ask": 3001.2, "mid": 3000.6, "spread": 1.2},
    )

    opportunity = Opportunity(
        strategy="MACRO_BREAKOUT",
        direction="LONG",
        score=80.0,
        entry_price=3000.6,
        stop_price=2995.0,
        take_profit_price=None,
        risk_per_unit=5.6,
        rationale="test",
    )

    with pytest.raises(SpreadTooWideError):
        client.place_market_order(opportunity, 1.0)


def test_fetch_candles_range_paginates_large_windows(monkeypatch) -> None:
    client = OandaClient(build_settings())
    requests_seen: list[dict] = []

    def fake_get(path: str, params: dict | None = None) -> dict:
        assert params is not None
        requests_seen.append(params)
        return {
            "candles": [
                {
                    "time": params["from"],
                    "complete": True,
                    "mid": {"o": "3000.0", "h": "3001.0", "l": "2999.0", "c": "3000.5"},
                    "volume": 100,
                }
            ]
        }

    monkeypatch.setattr(client, "_get", fake_get)

    frame = client.fetch_candles_range(
        "XAU_USD",
        "M15",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 3, 1, tzinfo=timezone.utc),
    )

    assert frame is not None
    assert len(requests_seen) >= 2


def test_live_xau_size_does_not_round_up_past_risk_budget(monkeypatch) -> None:
    settings = build_settings()
    settings = type(settings)(
        **{
            **settings.__dict__,
            "execution_mode": "live",
            "oanda_api_key": "key",
            "oanda_account_id": "account",
        }
    )
    client = OandaClient(settings)
    monkeypatch.setattr(client, "_estimate_conversion_rate", lambda base, quote: 0.75)

    assert client.calculate_xau_size(0.72, 59.111, "GBP") == 0.0
    assert client.calculate_xau_size(100.0, 50.0, "GBP") == 2.0
    assert client.estimate_xau_risk_amount(2.0, 50.0, "GBP") == 75.0