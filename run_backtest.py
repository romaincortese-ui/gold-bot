from __future__ import annotations

import argparse
import json

from goldbot.backtest_config import GoldBacktestConfig, parse_utc_datetime
from goldbot.backtest_data import GoldHistoricalDataProvider
from goldbot.backtest_engine import GoldBacktestEngine
from goldbot.backtest_reporter import build_monte_carlo_report, build_report, export_artifacts
from goldbot.config import load_settings
from goldbot.marketdata import OandaClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Gold-bot backtest")
    parser.add_argument("--start", help="UTC ISO start datetime")
    parser.add_argument("--end", help="UTC ISO end datetime")
    parser.add_argument("--output-dir", help="Artifact output directory")
    args = parser.parse_args()

    settings = load_settings()
    config = GoldBacktestConfig.from_env()
    if args.start:
        config.start = parse_utc_datetime(args.start)
    if args.end:
        config.end = parse_utc_datetime(args.end)
    if args.output_dir:
        config.output_dir = args.output_dir

    provider = GoldHistoricalDataProvider(OandaClient(settings), cache_dir=config.cache_dir)
    engine = GoldBacktestEngine(settings, config, provider)
    equity_curve, trades = engine.run()
    report = build_report(equity_curve, trades)
    robustness: dict[str, object] = {}
    if config.walk_forward_train_days > 0 and config.walk_forward_test_days > 0:
        robustness["walk_forward"] = engine.run_walk_forward(
            train_days=config.walk_forward_train_days,
            test_days=config.walk_forward_test_days,
            step_days=config.walk_forward_step_days,
        )
    if config.monte_carlo_iterations > 0:
        robustness["monte_carlo"] = build_monte_carlo_report(
            trades,
            initial_balance=config.initial_balance,
            iterations=config.monte_carlo_iterations,
            ruin_threshold_pct=config.monte_carlo_ruin_threshold_pct,
        )
    if robustness:
        report["robustness"] = robustness
    export_artifacts(config.output_dir, equity_curve, trades, report)

    print(
        json.dumps(
            {
                "backtest_run": {
                    "instrument": settings.instrument,
                    "start": config.start.isoformat(),
                    "end": config.end.isoformat(),
                    "output_dir": config.output_dir,
                    "total_trades": report["total_trades"],
                },
                "summary": report,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()