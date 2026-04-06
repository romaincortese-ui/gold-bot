from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pandas as pd

from goldbot.backtest_config import GoldBacktestConfig, parse_utc_datetime
from goldbot.marketdata import OandaClient
from goldbot.models import CalendarEvent


class GoldHistoricalDataProvider:
    def __init__(self, client: OandaClient, *, cache_dir: str) -> None:
        self.client = client
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load_frames(self, config: GoldBacktestConfig, instrument: str) -> dict[str, pd.DataFrame]:
        warmup_start = config.start - timedelta(days=config.warmup_days)
        return {
            "M15": self._load_frame(instrument, "M15", warmup_start, config.end),
            "H1": self._load_frame(instrument, "H1", warmup_start, config.end),
            "H4": self._load_frame(instrument, "H4", warmup_start, config.end),
            "D": self._load_frame(instrument, "D", warmup_start, config.end),
        }

    def load_events(self, event_file: str) -> list[CalendarEvent]:
        if not event_file:
            return []
        path = Path(event_file)
        if not path.exists():
            return []
        raw = json.loads(path.read_text(encoding="utf-8"))
        events: list[CalendarEvent] = []
        for item in raw:
            try:
                events.append(
                    CalendarEvent(
                        title=str(item["title"]),
                        currency=str(item.get("currency", "USD")),
                        impact=str(item.get("impact", "high")),
                        occurs_at=parse_utc_datetime(str(item["occurs_at"])),
                        source=str(item.get("source", "file")),
                    )
                )
            except Exception:
                continue
        return sorted(events, key=lambda event: event.occurs_at)

    def _load_frame(self, instrument: str, granularity: str, start, end) -> pd.DataFrame:
        cache_file = self.cache_dir / f"{instrument}_{granularity}_{start:%Y%m%d%H%M}_{end:%Y%m%d%H%M}.json"
        if cache_file.exists():
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            frame = pd.DataFrame(cached)
            frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
            frame = frame.dropna(subset=["time", "close"])
            return frame.sort_values("time").reset_index(drop=True)

        frame = self.client.fetch_candles_range(instrument, granularity, start, end)
        if frame is None or frame.empty:
            raise RuntimeError(f"No historical {granularity} candles returned for {instrument}")

        serializable = frame.copy()
        serializable["time"] = serializable["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        cache_file.write_text(json.dumps(serializable.to_dict(orient="records"), indent=2), encoding="utf-8")
        return frame.reset_index(drop=True)