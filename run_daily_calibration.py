"""Run a rolling-window backtest and publish calibration for the Gold-bot runtime.

Intended to be executed once per day via a Railway cron service:

    python run_daily_calibration.py

Environment variables:
    GOLD_BACKTEST_ROLLING_DAYS  – lookback window (default: 60)
    GOLD_CALIBRATION_FILE       – calibration JSON path (default: calibration.json)
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Iterator

from goldbot.backtest_config import GoldBacktestConfig
from goldbot.backtest_data import GoldHistoricalDataProvider
from goldbot.backtest_engine import GoldBacktestEngine
from goldbot.backtest_reporter import build_report
from goldbot.calibration import (
    CALIBRATION_FILE,
    CALIBRATION_REDIS_KEY,
    build_calibration,
    save_calibration,
)
from goldbot.config import load_settings
from goldbot.marketdata import OandaClient


@contextmanager
def _force_rolling_window() -> Iterator[None]:
    """Temporarily clear explicit start/end dates so the config uses rolling days."""
    preserved = {
        key: os.environ.get(key)
        for key in ("GOLD_BACKTEST_START", "BACKTEST_START", "GOLD_BACKTEST_END", "BACKTEST_END")
    }
    for key in preserved:
        os.environ.pop(key, None)
    # Default to 60 days if not set
    if not os.environ.get("GOLD_BACKTEST_ROLLING_DAYS") and not os.environ.get("BACKTEST_ROLLING_DAYS"):
        os.environ["GOLD_BACKTEST_ROLLING_DAYS"] = "60"
    try:
        yield
    finally:
        for key, value in preserved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main() -> None:
    with _force_rolling_window():
        config = GoldBacktestConfig.from_env()

    settings = load_settings()
    provider = GoldHistoricalDataProvider(OandaClient(settings), cache_dir=config.cache_dir)
    engine = GoldBacktestEngine(settings, config, provider)
    equity_curve, trades = engine.run()
    report = build_report(equity_curve, trades)

    calibration = build_calibration(
        report,
        window_start=config.start,
        window_end=config.end,
    )

    file_path = os.getenv("GOLD_CALIBRATION_FILE", CALIBRATION_FILE).strip()
    redis_key = os.getenv("GOLD_CALIBRATION_REDIS_KEY", CALIBRATION_REDIS_KEY).strip()
    save_calibration(calibration, file_path=file_path, redis_key=redis_key)

    print(
        json.dumps(
            {
                "calibration_run": {
                    "instrument": settings.instrument,
                    "start": config.start.isoformat(),
                    "end": config.end.isoformat(),
                    "output": file_path,
                    "redis_key": redis_key,
                    "total_trades": report["total_trades"],
                    "rolling_window_forced": True,
                },
                "calibration": calibration,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
