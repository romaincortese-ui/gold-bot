from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from goldbot.config import Settings
from goldbot.models import Opportunity


FRED_GRAPH_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


@dataclass(frozen=True)
class RealYieldSignal:
    as_of: datetime
    nominal_10y: float | None
    tips_10y: float | None
    real_yield_10y: float | None
    real_yield_change_bps: float | None


def fetch_real_yield_history(
    start: datetime,
    end: datetime,
    *,
    cache_dir: str | None = None,
) -> pd.DataFrame:
    cache_file = _cache_file(cache_dir, start, end)
    if cache_file is not None and cache_file.exists():
        cached = pd.read_csv(cache_file)
        return _normalize_real_yield_frame(cached)

    nominal = _fetch_fred_series("DGS10", start, end).rename(columns={"value": "nominal_10y"})
    tips = _fetch_fred_series("DFII10", start, end).rename(columns={"value": "tips_10y"})
    frame = nominal.merge(tips, on="time", how="outer").sort_values("time").reset_index(drop=True)
    frame["nominal_10y"] = pd.to_numeric(frame["nominal_10y"], errors="coerce")
    frame["tips_10y"] = pd.to_numeric(frame["tips_10y"], errors="coerce")
    frame["real_yield_10y"] = frame["tips_10y"]
    frame = frame.dropna(subset=["real_yield_10y"]).reset_index(drop=True)

    if cache_file is not None:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(cache_file, index=False)
    return frame


def build_real_yield_signal(frame: pd.DataFrame | None, as_of: datetime, lookback_days: int) -> RealYieldSignal | None:
    if frame is None or frame.empty:
        return None

    now_utc = as_of.astimezone(timezone.utc)
    window = frame[frame["time"] <= now_utc]
    if window.empty:
        return None

    current = window.iloc[-1]
    reference_cutoff = now_utc - timedelta(days=lookback_days)
    reference_window = window[window["time"] <= reference_cutoff]
    reference = reference_window.iloc[-1] if not reference_window.empty else (window.iloc[0] if len(window) > 1 else None)
    change_bps = None
    if reference is not None and pd.notna(reference["real_yield_10y"]):
        change_bps = (float(current["real_yield_10y"]) - float(reference["real_yield_10y"])) * 100.0

    return RealYieldSignal(
        as_of=_ensure_utc_timestamp(current["time"]),
        nominal_10y=_coerce_optional_float(current.get("nominal_10y")),
        tips_10y=_coerce_optional_float(current.get("tips_10y")),
        real_yield_10y=_coerce_optional_float(current.get("real_yield_10y")),
        real_yield_change_bps=change_bps,
    )


def load_real_yield_signal_from_macro_state(
    file_path: str,
    now: datetime,
    *,
    max_age_hours: int,
) -> RealYieldSignal | None:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    raw = payload.get("real_yields")
    if not isinstance(raw, dict):
        return None
    signal = _signal_from_dict(raw)
    if signal is None:
        return None

    if max_age_hours >= 0 and (now.astimezone(timezone.utc) - signal.as_of) > timedelta(hours=max_age_hours):
        return None
    return signal


def apply_real_yield_overlay(settings: Settings, opportunity: Opportunity, signal: RealYieldSignal | None) -> Opportunity | None:
    if not settings.real_yield_filter_enabled or signal is None or signal.real_yield_change_bps is None:
        return opportunity

    change_bps = float(signal.real_yield_change_bps)
    veto = float(settings.real_yield_veto_bps)
    reduce = float(settings.real_yield_reduce_risk_bps)
    metadata = dict(opportunity.metadata)
    metadata.update(
        {
            "real_yield_10y": round(signal.real_yield_10y, 3) if signal.real_yield_10y is not None else None,
            "real_yield_change_bps": round(change_bps, 2),
            "nominal_10y": round(signal.nominal_10y, 3) if signal.nominal_10y is not None else None,
            "tips_10y": round(signal.tips_10y, 3) if signal.tips_10y is not None else None,
            "real_yield_as_of": signal.as_of.isoformat(),
            "risk_multiplier": 1.0,
        }
    )

    if opportunity.direction == "LONG":
        if change_bps >= veto:
            return None
        if change_bps >= reduce:
            metadata["risk_multiplier"] = settings.real_yield_adverse_risk_multiplier
            metadata["macro_filter"] = "real_yield_long_reduced"
    else:
        if change_bps <= -veto:
            return None
        if change_bps <= -reduce:
            metadata["risk_multiplier"] = settings.real_yield_adverse_risk_multiplier
            metadata["macro_filter"] = "real_yield_short_reduced"

    opportunity.metadata = metadata
    return opportunity


def signal_to_payload(signal: RealYieldSignal | None) -> dict | None:
    if signal is None:
        return None
    payload = asdict(signal)
    payload["as_of"] = signal.as_of.isoformat()
    return payload


def _fetch_fred_series(series_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    response = requests.get(
        FRED_GRAPH_CSV_URL,
        params={
            "id": series_id,
            "cosd": start.astimezone(timezone.utc).strftime("%Y-%m-%d"),
            "coed": end.astimezone(timezone.utc).strftime("%Y-%m-%d"),
        },
        timeout=20,
    )
    response.raise_for_status()
    frame = pd.read_csv(StringIO(response.text))
    if frame.empty:
        return pd.DataFrame(columns=["time", "value"])
    frame = frame.rename(columns={frame.columns[0]: "time", frame.columns[1]: "value"})
    return _normalize_series_frame(frame)


def _normalize_series_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["time"] = pd.to_datetime(normalized["time"], utc=True, errors="coerce")
    normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce")
    normalized = normalized.dropna(subset=["time"]).reset_index(drop=True)
    return normalized


def _normalize_real_yield_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["time"] = pd.to_datetime(normalized["time"], utc=True, errors="coerce")
    for column in ["nominal_10y", "tips_10y", "real_yield_10y"]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized.dropna(subset=["time", "real_yield_10y"]).sort_values("time").reset_index(drop=True)


def _signal_from_dict(payload: dict) -> RealYieldSignal | None:
    as_of_raw = payload.get("as_of")
    if not as_of_raw:
        return None
    try:
        as_of = datetime.fromisoformat(str(as_of_raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    else:
        as_of = as_of.astimezone(timezone.utc)
    return RealYieldSignal(
        as_of=as_of,
        nominal_10y=_coerce_optional_float(payload.get("nominal_10y")),
        tips_10y=_coerce_optional_float(payload.get("tips_10y")),
        real_yield_10y=_coerce_optional_float(payload.get("real_yield_10y")),
        real_yield_change_bps=_coerce_optional_float(payload.get("real_yield_change_bps")),
    )


def _cache_file(cache_dir: str | None, start: datetime, end: datetime) -> Path | None:
    if not cache_dir:
        return None
    return Path(cache_dir) / f"real_yields_{start:%Y%m%d}_{end:%Y%m%d}.csv"


def _coerce_optional_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_utc_timestamp(value: object) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.to_pydatetime().astimezone(timezone.utc)