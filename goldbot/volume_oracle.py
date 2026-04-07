from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class BreakoutVolumeSignal:
    source: str
    as_of: datetime
    volume_ratio: float
    current_volume: float | None = None
    baseline_volume: float | None = None


def load_breakout_volume_signal(
    file_path: str,
    now: datetime,
    *,
    max_age_minutes: int,
) -> BreakoutVolumeSignal | None:
    if not file_path:
        return None

    path = Path(file_path)
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    ratio = _coerce_float(
        payload.get("volume_ratio", payload.get("latest_volume_ratio", payload.get("ratio")))
    )
    as_of = _parse_timestamp(
        payload.get("as_of", payload.get("generated_at", payload.get("timestamp")))
    )
    if ratio is None or ratio <= 0 or as_of is None:
        return None

    age = now.astimezone(timezone.utc) - as_of
    if max_age_minutes >= 0 and age > timedelta(minutes=max_age_minutes):
        return None

    return BreakoutVolumeSignal(
        source=str(payload.get("source", "external_oracle") or "external_oracle"),
        as_of=as_of,
        volume_ratio=ratio,
        current_volume=_coerce_float(payload.get("current_volume")),
        baseline_volume=_coerce_float(payload.get("baseline_volume")),
    )


def _parse_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None