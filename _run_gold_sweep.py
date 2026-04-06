import json, os, shutil, subprocess, time
from pathlib import Path

root = Path.cwd()
python_exe = Path(r"c:/Users/Rocot/Downloads/mexc-bot2/.venv/Scripts/python.exe")
env_sample = root / ".env.sample"
env_local = root / ".env"
sweeps_root = root / "backtest_output" / "sweeps"
sweeps_root.mkdir(parents=True, exist_ok=True)

def load_env_sample(path: Path):
    data = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data

sample = load_env_sample(env_sample)
if env_local.exists():
    sample.update(load_env_sample(env_local))
override_keys = {
    "TREND_PULLBACK_ATR_TOLERANCE",
    "TREND_MIN_STRENGTH_ATR",
    "TREND_MIN_SLOPE_ATR",
    "TREND_STOPOUT_COOLDOWN_HOURS",
    "PARTIAL_PROFIT_RR",
    "BREAK_EVEN_RR",
    "TRAILING_ATR_MULT",
    "GOLD_BACKTEST_SIMULATED_SPREAD",
    "GOLD_BACKTEST_CACHE_DIR",
    "GOLD_BACKTEST_ROLLING_DAYS",
    "GOLD_BACKTEST_EVENT_FILE",
}
windows = [30, 60, 90]
runs = [
    ("baseline_copy", {}),
    ("tighter_pullback", {"TREND_PULLBACK_ATR_TOLERANCE": "0.45"}),
    ("slower_protection", {"PARTIAL_PROFIT_RR": "1.25", "BREAK_EVEN_RR": "1.25", "TRAILING_ATR_MULT": "2.8"}),
    ("trend_guard", {"TREND_MIN_STRENGTH_ATR": "1.45", "TREND_MIN_SLOPE_ATR": "0.18", "TREND_STOPOUT_COOLDOWN_HOURS": "72"}),
    ("trend_guard_plus", {"TREND_PULLBACK_ATR_TOLERANCE": "0.45", "TREND_MIN_STRENGTH_ATR": "1.45", "TREND_MIN_SLOPE_ATR": "0.18", "TREND_STOPOUT_COOLDOWN_HOURS": "72", "PARTIAL_PROFIT_RR": "1.25", "BREAK_EVEN_RR": "1.25", "TRAILING_ATR_MULT": "2.8"}),
]

results = []
for rolling_days in windows:
    for name, overrides in runs:
        run_name = f"{rolling_days}d_{name}"
        out_dir = sweeps_root / run_name
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        cache_dir = sweeps_root / f"_cache_{run_name}"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env.update(sample)
        for key in override_keys:
            if key not in sample:
                env.pop(key, None)
        env.update(overrides)
        env["GOLD_BACKTEST_CACHE_DIR"] = str(cache_dir)
        env["GOLD_BACKTEST_ROLLING_DAYS"] = str(rolling_days)

        summary_path = out_dir / "summary.json"
        combined_log_parts = []
        success = False
        for attempt in range(1, 7):
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
            proc = subprocess.run(
                [str(python_exe), "run_backtest.py", "--output-dir", str(out_dir)],
                cwd=str(root), env=env, capture_output=True, text=True
            )
            combined = f"===== attempt {attempt} exit={proc.returncode} =====\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}\n"
            combined_log_parts.append(combined)
            (out_dir / "run.txt").write_text("\n".join(combined_log_parts), encoding="utf-8")
            if proc.returncode == 0 and summary_path.exists():
                success = True
                break
            time.sleep(attempt * 4)

        row = {"run_name": run_name, "window_days": rolling_days, "profile": name, "success": success}
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            row.update({
                "trades": summary.get("total_trades"),
                "total_pnl": summary.get("total_pnl"),
                "win_rate": summary.get("win_rate"),
                "profit_factor": summary.get("profit_factor"),
                "max_drawdown": summary.get("max_drawdown"),
            })
        else:
            row.update({"trades": None, "total_pnl": None, "win_rate": None, "profit_factor": None, "max_drawdown": None})
        results.append(row)

(root / "backtest_output" / "sweeps" / "sweep_results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
valid = [r for r in results if r.get("total_pnl") is not None]
valid.sort(key=lambda r: r["total_pnl"], reverse=True)
print(json.dumps({"results": results, "best_run": valid[0] if valid else None}, indent=2))
