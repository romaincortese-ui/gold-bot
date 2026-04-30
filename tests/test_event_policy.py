from datetime import datetime, timedelta, timezone

from goldbot.event_policy import apply_gold_event_policy, evaluate_gold_event_policy, is_event_state_fresh
from goldbot.models import Opportunity
from tests.test_strategies import build_settings


def _opportunity(direction: str = "LONG") -> Opportunity:
    return Opportunity(
        strategy="TREND_PULLBACK",
        direction=direction,
        score=75.0,
        entry_price=3000.0,
        stop_price=2995.0,
        take_profit_price=None,
        risk_per_unit=5.0,
        rationale="test",
        metadata={},
        exit_plan={},
    )


def test_event_state_freshness_uses_generated_at() -> None:
    now = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
    state = {"generated_at": (now - timedelta(minutes=5)).isoformat()}

    assert is_event_state_fresh(state, now, max_age_seconds=600) is True
    assert is_event_state_fresh(state, now, max_age_seconds=60) is False


def test_policy_reduces_long_when_real_yields_jump() -> None:
    now = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
    state = {
        "generated_at": now.isoformat(),
        "real_yields": {
            "as_of": now.isoformat(),
            "real_yield_10y": 2.0,
            "real_yield_change_bps": 12.0,
        },
    }

    decision = evaluate_gold_event_policy(_opportunity("LONG"), state, now)

    assert decision.allowed is True
    assert decision.reason == "event_adverse_reduce"
    assert decision.risk_multiplier == 0.5
    assert decision.gold_bias_score < 0


def test_policy_boosts_long_on_dovish_scored_event() -> None:
    now = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
    state = {
        "generated_at": now.isoformat(),
        "event_scores": [
            {
                "as_of": (now - timedelta(minutes=20)).isoformat(),
                "composite": 0.8,
                "usd_direction": "DOWN",
            }
        ],
    }

    settings = build_settings()
    opportunity, decision = apply_gold_event_policy(settings, _opportunity("LONG"), state, now)

    assert opportunity is not None
    assert decision.reason == "event_favourable_boost"
    assert opportunity.metadata["risk_multiplier"] > 1.0
    assert opportunity.score > 75.0


def test_policy_blocks_extreme_adverse_event() -> None:
    now = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
    state = {
        "generated_at": now.isoformat(),
        "events": [
            {
                "title": "Hot CPI inflation surprise",
                "currency": "USD",
                "impact": "high",
                "occurs_at": (now - timedelta(minutes=5)).isoformat(),
            }
        ],
        "event_scores": [
            {
                "as_of": (now - timedelta(minutes=5)).isoformat(),
                "composite": 0.9,
                "usd_direction": "UP",
            }
        ],
    }

    decision = evaluate_gold_event_policy(_opportunity("LONG"), state, now)

    assert decision.allowed is False
    assert decision.reason == "extreme_event_adverse"