import json
from datetime import datetime, timedelta, timezone

from goldbot.news_scoring import (
    EventScore,
    load_event_scores,
    score_event,
    select_best_for_breakout,
)


NOW = datetime(2026, 4, 5, 13, 0, tzinfo=timezone.utc)


def test_score_event_requires_timestamp() -> None:
    assert score_event({"title": "no-ts"}) is None


def test_score_event_needs_at_least_two_factors() -> None:
    # Only rates move provided — single factor should return composite=None.
    event = score_event({
        "event_key": "one-factor",
        "as_of": NOW.isoformat(),
        "rates_move_bps": 10.0,
    })
    assert event is not None
    assert event.composite is None


def test_score_event_composite_with_two_factors() -> None:
    event = score_event({
        "event_key": "NFP",
        "as_of": NOW.isoformat(),
        "rates_move_bps": 10.0,
        "dxy_move_pct": 0.60,
        "usd_direction": "UP",
    })
    assert event is not None
    assert event.composite is not None
    assert 0.5 <= event.composite <= 0.95


def test_load_event_scores_drops_stale(tmp_path) -> None:
    recent = NOW - timedelta(minutes=30)
    stale = NOW - timedelta(minutes=600)
    (tmp_path / "macro.json").write_text(json.dumps({
        "event_scores": [
            {
                "event_key": "recent",
                "as_of": recent.isoformat(),
                "rates_move_bps": 10.0,
                "dxy_move_pct": 0.5,
                "usd_direction": "UP",
            },
            {
                "event_key": "stale",
                "as_of": stale.isoformat(),
                "rates_move_bps": 10.0,
                "dxy_move_pct": 0.5,
                "usd_direction": "UP",
            },
        ]
    }))
    results = load_event_scores(str(tmp_path / "macro.json"), now=NOW, max_age_minutes=120)
    assert [r.event_key for r in results] == ["recent"]


def test_select_best_for_long_wants_dovish() -> None:
    dovish = EventScore(
        event_key="dovish",
        as_of=NOW - timedelta(minutes=10),
        surprise_z=-1.2,
        rates_move_bps=-8.0,
        dxy_move_pct=-0.4,
        usd_direction="DOWN",
        composite=0.7,
        raw={},
    )
    hawkish = EventScore(
        event_key="hawkish",
        as_of=NOW - timedelta(minutes=5),
        surprise_z=1.2,
        rates_move_bps=8.0,
        dxy_move_pct=0.4,
        usd_direction="UP",
        composite=0.7,
        raw={},
    )
    assert select_best_for_breakout([dovish, hawkish], direction="LONG").event_key == "dovish"
    assert select_best_for_breakout([dovish, hawkish], direction="SHORT").event_key == "hawkish"


def test_select_best_returns_none_when_direction_mismatch() -> None:
    only_hawkish = EventScore(
        event_key="h",
        as_of=NOW,
        surprise_z=1.0,
        rates_move_bps=8.0,
        dxy_move_pct=0.4,
        usd_direction="UP",
        composite=0.7,
        raw={},
    )
    assert select_best_for_breakout([only_hawkish], direction="LONG") is None
