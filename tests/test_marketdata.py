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