from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

import pandas as pd


def _profit_factor(pnl: pd.Series) -> float:
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    return float(wins.sum() / abs(losses.sum())) if not losses.empty else 999.0


def build_report(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_curve)
    if trades_df.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_pnl": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "by_strategy": {},
        }

    pnl = trades_df["pnl"].astype(float)
    curve = equity_df["equity"].astype(float) if not equity_df.empty else pd.Series(dtype=float)
    max_drawdown = 0.0
    if not curve.empty:
        running_max = curve.cummax()
        drawdown = (curve - running_max) / running_max.replace(0, 1)
        max_drawdown = float(drawdown.min())

    by_strategy: dict[str, dict[str, float | int]] = {}
    for strategy, group in trades_df.groupby("strategy"):
        strategy_pnl = group["pnl"].astype(float)
        by_strategy[str(strategy)] = {
            "trades": int(len(group)),
            "win_rate": float((strategy_pnl > 0).mean()),
            "total_pnl": float(strategy_pnl.sum()),
            "expectancy": float(strategy_pnl.mean()),
            "profit_factor": _profit_factor(strategy_pnl),
        }

    return {
        "total_trades": int(len(trades_df)),
        "win_rate": float((pnl > 0).mean()),
        "profit_factor": _profit_factor(pnl),
        "total_pnl": float(pnl.sum()),
        "expectancy": float(pnl.mean()),
        "max_drawdown": max_drawdown,
        "by_strategy": by_strategy,
    }


def build_monte_carlo_report(
    trades: list[dict[str, Any]],
    *,
    initial_balance: float,
    iterations: int,
    ruin_threshold_pct: float,
    seed: int = 42,
) -> dict[str, Any]:
    if iterations <= 0 or not trades:
        return {
            "iterations": max(0, iterations),
            "risk_of_ruin": 0.0,
            "median_total_pnl": 0.0,
            "p05_total_pnl": 0.0,
            "p95_total_pnl": 0.0,
            "median_max_drawdown": 0.0,
        }

    pnl_values = [float(trade.get("pnl", 0.0) or 0.0) for trade in trades]
    ruin_floor = initial_balance * (1.0 - (ruin_threshold_pct / 100.0))
    rng = random.Random(seed)
    total_pnls: list[float] = []
    max_drawdowns: list[float] = []
    ruined_paths = 0

    for _ in range(iterations):
        sample = pnl_values[:]
        rng.shuffle(sample)
        equity = float(initial_balance)
        peak = float(initial_balance)
        max_drawdown = 0.0
        ruined = False
        for pnl in sample:
            equity += pnl
            peak = max(peak, equity)
            if peak > 0:
                max_drawdown = min(max_drawdown, (equity - peak) / peak)
            if equity <= ruin_floor:
                ruined = True
        if ruined:
            ruined_paths += 1
        total_pnls.append(equity - initial_balance)
        max_drawdowns.append(max_drawdown)

    totals = pd.Series(total_pnls, dtype=float)
    drawdowns = pd.Series(max_drawdowns, dtype=float)
    return {
        "iterations": int(iterations),
        "risk_of_ruin": float(ruined_paths / iterations),
        "median_total_pnl": float(totals.median()),
        "p05_total_pnl": float(totals.quantile(0.05)),
        "p95_total_pnl": float(totals.quantile(0.95)),
        "median_max_drawdown": float(drawdowns.median()),
    }


def export_artifacts(output_dir: str, equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]], report: dict[str, Any]) -> None:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(equity_curve).to_csv(base / "equity_curve.csv", index=False)
    pd.DataFrame(trades).to_csv(base / "trade_journal.csv", index=False)
    (base / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")