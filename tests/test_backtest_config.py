from datetime import datetime, timezone

from goldbot.backtest_config import GoldBacktestConfig, parse_utc_datetime


def test_parse_utc_datetime_normalizes_z_suffix() -> None:
    parsed = parse_utc_datetime("2026-04-06T12:00:00Z")

    assert parsed == datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)


def test_backtest_config_defaults_to_30_day_window(monkeypatch) -> None:
    monkeypatch.delenv("GOLD_BACKTEST_START", raising=False)
    monkeypatch.delenv("GOLD_BACKTEST_END", raising=False)
    monkeypatch.delenv("GOLD_BACKTEST_ROLLING_DAYS", raising=False)
    monkeypatch.delenv("BACKTEST_START", raising=False)
    monkeypatch.delenv("BACKTEST_END", raising=False)
    monkeypatch.delenv("BACKTEST_ROLLING_DAYS", raising=False)

    config = GoldBacktestConfig.from_env(now=datetime(2026, 4, 6, 15, 44, tzinfo=timezone.utc))

    assert config.end == datetime(2026, 4, 6, 15, 0, tzinfo=timezone.utc)
    assert config.start == datetime(2026, 3, 7, 15, 0, tzinfo=timezone.utc)