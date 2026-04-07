import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from goldbot.config import load_settings
from goldbot.news import fetch_calendar_events, filter_gold_events
from goldbot.real_yields import build_real_yield_signal, fetch_real_yield_history, signal_to_payload


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    settings = load_settings()
    now = datetime.now(timezone.utc)
    events = fetch_calendar_events(settings.news_urls, settings.news_cache_file)
    relevant = filter_gold_events(
        events,
        now=now,
        lookback_hours=settings.breakout_news_lookback_hours,
        lookahead_hours=settings.breakout_news_lookahead_hours,
    )
    real_yields = None
    if settings.real_yield_filter_enabled:
        try:
            history = fetch_real_yield_history(now - timedelta(days=max(20, settings.real_yield_lookback_days + 10)), now)
            real_yields = signal_to_payload(build_real_yield_signal(history, now, settings.real_yield_lookback_days))
        except Exception as exc:
            log.warning("Failed to refresh real-yield snapshot: %s", exc)
    payload = {
        "generated_at": now.isoformat(),
        "instrument": settings.instrument,
        "event_count": len(relevant),
        "events": [asdict(event) | {"occurs_at": event.occurs_at.isoformat()} for event in relevant],
        "real_yields": real_yields,
    }
    output_path = Path(settings.macro_state_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log.info("Wrote %s relevant gold events to %s", len(relevant), output_path)


if __name__ == "__main__":
    main()