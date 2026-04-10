"""Gold-bot calibration: derive entry/risk adjustments from rolling backtest results.

The daily calibration runner produces a calibration payload stored in Redis and/or
a JSON file.  The live runtime loads this payload each cycle and uses it to:

  - Block strategies with persistent underperformance
  - Tighten score thresholds for struggling strategies
  - Adjust risk multipliers up or down
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    import redis as _redis_mod
except ImportError:
    _redis_mod = None  # type: ignore

from goldbot.shared_backend import get_redis_client

log = logging.getLogger(__name__)

CALIBRATION_REDIS_KEY = "gold_trade_calibration"
CALIBRATION_FILE = "calibration.json"
CALIBRATION_MAX_AGE_HOURS = 48.0
CALIBRATION_MIN_TRADES = 2


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _profit_factor(wins_sum: float, losses_sum: float) -> float:
    return float(wins_sum / abs(losses_sum)) if losses_sum < 0 else 999.0


def _derive_strategy_adjustment(metrics: Mapping[str, Any], *, min_trades: int) -> dict[str, Any]:
    """Derive score-offset and risk multiplier for one strategy from its backtest metrics."""
    trades = int(metrics.get("trades", 0) or 0)
    if trades < min_trades:
        return {"score_offset": 0.0, "risk_mult": 1.0, "block_reason": None}

    pf = float(metrics.get("profit_factor", 0.0) or 0.0)
    expectancy = float(metrics.get("expectancy", 0.0) or 0.0)
    win_rate = float(metrics.get("win_rate", 0.0) or 0.0)

    # Hard block: persistent underperformance with meaningful sample
    if trades >= max(6, min_trades * 2) and pf < 0.7 and expectancy < -5.0 and win_rate < 0.35:
        return {
            "score_offset": 0.0,
            "risk_mult": 0.5,
            "block_reason": "calibration block: persistent underperformance",
        }

    # Underperforming: raise the bar
    if pf < 0.95 or expectancy < 0:
        tighten = min(10.0, round(max(0.0, (1.0 - pf) * 12.0) + max(0.0, -expectancy / 5.0), 2))
        risk_mult = max(0.5, round(1.0 - min(0.4, tighten / 20.0), 2))
        return {"score_offset": -tighten, "risk_mult": risk_mult, "block_reason": None}

    # Outperforming: relax slightly
    if pf > 1.2 and expectancy > 5.0 and win_rate > 0.5:
        relax = min(8.0, round((pf - 1.0) * 5.0 + min(3.0, expectancy / 10.0), 2))
        risk_mult = min(1.25, round(1.0 + min(0.25, relax / 20.0), 2))
        return {"score_offset": relax, "risk_mult": risk_mult, "block_reason": None}

    return {"score_offset": 0.0, "risk_mult": 1.0, "block_reason": None}


def build_calibration(report: Mapping[str, Any], *, window_start: datetime, window_end: datetime, min_trades: int = CALIBRATION_MIN_TRADES) -> dict[str, Any]:
    """Build a calibration payload from a backtest summary report."""
    by_strategy = dict(report.get("by_strategy", {}))
    adjustments: dict[str, dict[str, Any]] = {}
    for strategy, metrics in by_strategy.items():
        adjustments[strategy] = _derive_strategy_adjustment(metrics, min_trades=min_trades)

    return {
        "generated_at": _utc_now().isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "total_trades": int(report.get("total_trades", 0) or 0),
        "win_rate": float(report.get("win_rate", 0.0) or 0.0),
        "profit_factor": float(report.get("profit_factor", 0.0) or 0.0),
        "total_pnl": float(report.get("total_pnl", 0.0) or 0.0),
        "by_strategy": by_strategy,
        "strategy_adjustments": adjustments,
    }


def save_calibration(calibration: Mapping[str, Any], *, file_path: str = CALIBRATION_FILE, redis_key: str = CALIBRATION_REDIS_KEY) -> None:
    """Persist calibration to Redis and/or JSON file."""
    import os as _os
    client = get_redis_client()
    if client is not None:
        try:
            client.set(redis_key, json.dumps(calibration))
            log.info("Published calibration to Redis key %s", redis_key)
        except Exception:
            log.warning("Failed to publish calibration to Redis", exc_info=True)
    else:
        redis_url = _os.getenv("REDIS_URL", "")
        if not redis_url:
            log.warning("REDIS_URL not set – skipping Redis publish")
        else:
            log.warning("Redis client unavailable (connection failed?) – skipping Redis publish")
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(calibration, indent=2), encoding="utf-8")
    log.info("Wrote calibration to %s", path)


def load_calibration(*, file_path: str = CALIBRATION_FILE, redis_key: str = CALIBRATION_REDIS_KEY) -> dict[str, Any] | None:
    """Load calibration from Redis (preferred) or file fallback."""
    client = get_redis_client()
    if client is not None:
        try:
            raw = client.get(redis_key)
            if raw:
                log.info("Loaded calibration from Redis key %s", redis_key)
                return json.loads(raw)
            else:
                log.info("Redis key %s is empty, falling back to file", redis_key)
        except Exception:
            log.warning("Failed to read calibration from Redis", exc_info=True)
    else:
        log.debug("Redis unavailable for calibration load, using file fallback")
    path = Path(file_path)
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def validate_calibration(data: Mapping[str, Any], *, max_age_hours: float = CALIBRATION_MAX_AGE_HOURS, min_total_trades: int = CALIBRATION_MIN_TRADES) -> tuple[bool, str | None]:
    """Check if a calibration payload is fresh and has enough trades."""
    total_trades = int(data.get("total_trades", 0) or 0)
    if total_trades < min_total_trades:
        return False, f"insufficient sample ({total_trades} trades < {min_total_trades})"

    generated_at = data.get("generated_at")
    if not generated_at:
        return False, "missing generated_at"
    try:
        created = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return False, "invalid generated_at"

    age_hours = (_utc_now() - created).total_seconds() / 3600.0
    if age_hours > max_age_hours:
        return False, f"stale calibration ({age_hours:.1f}h > {max_age_hours:.1f}h)"
    return True, None


def get_strategy_adjustment(calibration: Mapping[str, Any] | None, strategy: str) -> dict[str, Any]:
    """Get the score/risk adjustment for a given strategy.  Returns neutral defaults if no calibration."""
    neutral = {"score_offset": 0.0, "risk_mult": 1.0, "block_reason": None}
    if not calibration:
        return neutral
    adjustments = calibration.get("strategy_adjustments", {})
    return dict(adjustments.get(strategy, neutral))
