from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


def parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _env_text(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip() != "":
            return value.strip()
    return default


def _env_float(*names: str, default: float) -> float:
    raw = _env_text(*names, default="")
    return default if raw == "" else float(raw)


def _env_int(*names: str, default: int) -> int:
    raw = _env_text(*names, default="")
    return default if raw == "" else int(raw)


def _align_hour(moment: datetime) -> datetime:
    normalized = moment.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return normalized


@dataclass(slots=True)
class GoldBacktestConfig:
    start: datetime
    end: datetime
    initial_balance: float = 10_000.0
    output_dir: str = "backtest_output"
    cache_dir: str = "backtest_cache"
    warmup_days: int = 90
    simulated_spread: float = 0.25
    event_file: str = ""

    @classmethod
    def from_env(cls, *, now: datetime | None = None) -> "GoldBacktestConfig":
        reference = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
        rolling_days = _env_float("GOLD_BACKTEST_ROLLING_DAYS", "BACKTEST_ROLLING_DAYS", default=30.0)
        end_raw = _env_text("GOLD_BACKTEST_END", "BACKTEST_END")
        start_raw = _env_text("GOLD_BACKTEST_START", "BACKTEST_START")

        end = parse_utc_datetime(end_raw) if end_raw else _align_hour(reference)
        start = parse_utc_datetime(start_raw) if start_raw else end - timedelta(days=rolling_days)
        if start >= end:
            raise ValueError("Gold backtest start must be earlier than end")

        return cls(
            start=start,
            end=end,
            initial_balance=_env_float("GOLD_BACKTEST_INITIAL_BALANCE", default=10_000.0),
            output_dir=_env_text("GOLD_BACKTEST_OUTPUT_DIR", default="backtest_output"),
            cache_dir=_env_text("GOLD_BACKTEST_CACHE_DIR", default="backtest_cache"),
            warmup_days=_env_int("GOLD_BACKTEST_WARMUP_DAYS", default=90),
            simulated_spread=_env_float("GOLD_BACKTEST_SIMULATED_SPREAD", default=0.25),
            event_file=_env_text("GOLD_BACKTEST_EVENT_FILE", default=""),
        )