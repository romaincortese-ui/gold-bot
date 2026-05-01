import json
from datetime import datetime, timezone

from goldbot import news
from goldbot.news import fetch_calendar_events


def test_fetch_calendar_events_uses_cache_and_backs_off_after_failure(tmp_path, monkeypatch) -> None:
    cache_file = tmp_path / "calendar.json"
    cache_file.write_text(
        json.dumps([
            {
                "title": "US CPI",
                "currency": "USD",
                "impact": "high",
                "occurs_at": datetime(2026, 4, 6, 12, 30, tzinfo=timezone.utc).isoformat(),
                "source": "cache",
            }
        ]),
        encoding="utf-8",
    )
    calls = {"count": 0}

    def failing_get(*args, **kwargs):
        calls["count"] += 1
        raise RuntimeError("rate limited")

    monkeypatch.setattr(news.requests, "get", failing_get)
    monkeypatch.setattr(news.time, "monotonic", lambda: 1000.0)
    news._calendar_failed_until.clear()
    news._calendar_last_warning_at.clear()

    first = fetch_calendar_events(["https://calendar.example/feed.xml"], str(cache_file), failure_backoff_minutes=15)
    second = fetch_calendar_events(["https://calendar.example/feed.xml"], str(cache_file), failure_backoff_minutes=15)

    assert [event.title for event in first] == ["US CPI"]
    assert [event.title for event in second] == ["US CPI"]
    assert calls["count"] == 1


def test_fetch_calendar_events_retries_after_backoff(tmp_path, monkeypatch) -> None:
    cache_file = tmp_path / "calendar.json"
    cache_file.write_text("[]", encoding="utf-8")
    monotonic_value = {"now": 1000.0}
    calls = {"count": 0}

    def failing_get(*args, **kwargs):
        calls["count"] += 1
        raise RuntimeError("rate limited")

    monkeypatch.setattr(news.requests, "get", failing_get)
    monkeypatch.setattr(news.time, "monotonic", lambda: monotonic_value["now"])
    news._calendar_failed_until.clear()
    news._calendar_last_warning_at.clear()

    assert fetch_calendar_events(["https://calendar.example/feed.xml"], str(cache_file), failure_backoff_minutes=1) == []

    monotonic_value["now"] = 1100.0
    fetch_calendar_events(["https://calendar.example/feed.xml"], str(cache_file), failure_backoff_minutes=1)

    assert calls["count"] == 2