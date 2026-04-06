from datetime import datetime, timezone
import json

from goldbot.backtest_data import GoldHistoricalDataProvider
from goldbot.marketdata import OandaClient
from tests.test_strategies import build_settings


def test_load_frame_restores_cached_timestamps(tmp_path) -> None:
    provider = GoldHistoricalDataProvider(OandaClient(build_settings()), cache_dir=str(tmp_path))
    cache_file = tmp_path / "XAU_USD_M15_202601010000_202601020000.json"
    cache_file.write_text(
        json.dumps(
            [
                {
                    "time": "2026-01-01T00:00:00Z",
                    "open": 3000.0,
                    "high": 3001.0,
                    "low": 2999.0,
                    "close": 3000.5,
                    "volume": 100,
                }
            ]
        ),
        encoding="utf-8",
    )

    frame = provider._load_frame(
        "XAU_USD",
        "M15",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert str(frame["time"].dtype).startswith("datetime64")
    assert frame["time"].iloc[0].tzinfo is not None


def test_load_events_supports_csv(tmp_path) -> None:
    provider = GoldHistoricalDataProvider(OandaClient(build_settings()), cache_dir=str(tmp_path))
    event_file = tmp_path / "events.csv"
    event_file.write_text(
        "title,currency,impact,occurs_at,source\nUS CPI,USD,high,2026-03-11T13:30:00+00:00,test\n",
        encoding="utf-8",
    )

    events = provider.load_events(str(event_file))

    assert len(events) == 1
    assert events[0].title == "US CPI"