from datetime import datetime, timedelta, timezone

from goldbot.spread_tracker import SpreadTracker


def _t(minutes: int) -> datetime:
    return datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc) + timedelta(minutes=minutes)


def test_warmup_falls_back_to_static_cap() -> None:
    t = SpreadTracker(
        window_minutes=30,
        multiplier=1.8,
        floor=0.25,
        min_samples=6,
        static_cap=0.60,
    )
    # Fewer than min_samples -> allowed spread is the static cap.
    t.record(0.15, now=_t(0))
    t.record(0.20, now=_t(1))
    assert t.allowed_spread(now=_t(2)) == 0.60
    assert t.is_acceptable(0.50, now=_t(2)) is True


def test_rolling_median_caps_acceptance() -> None:
    t = SpreadTracker(
        window_minutes=30,
        multiplier=1.8,
        floor=0.25,
        min_samples=3,
        static_cap=0.60,
    )
    for i, spread in enumerate((0.10, 0.10, 0.10)):
        t.record(spread, now=_t(i))
    # median 0.10 * 1.8 = 0.18 -> raised to floor 0.25
    assert t.allowed_spread(now=_t(5)) == 0.25
    assert t.is_acceptable(0.30, now=_t(5)) is False
    assert t.is_acceptable(0.20, now=_t(5)) is True


def test_static_cap_is_upper_bound() -> None:
    t = SpreadTracker(
        window_minutes=30,
        multiplier=5.0,  # large multiplier
        floor=0.10,
        min_samples=3,
        static_cap=0.50,
    )
    for i, spread in enumerate((0.30, 0.30, 0.30)):
        t.record(spread, now=_t(i))
    # median 0.30 * 5.0 = 1.50 -> clamped at static cap 0.50
    assert t.allowed_spread(now=_t(5)) == 0.50


def test_old_samples_evicted() -> None:
    t = SpreadTracker(
        window_minutes=10,
        multiplier=2.0,
        floor=0.10,
        min_samples=3,
        static_cap=1.00,
    )
    t.record(0.50, now=_t(0))
    t.record(0.50, now=_t(1))
    t.record(0.50, now=_t(2))
    # After 20 min, all original samples should evict.
    t.record(0.10, now=_t(20))
    t.record(0.10, now=_t(21))
    t.record(0.10, now=_t(22))
    assert t.sample_count() == 3
    assert abs(t.median() - 0.10) < 1e-9
