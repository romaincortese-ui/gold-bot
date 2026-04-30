import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from goldbot.config import load_settings
from goldbot.news import fetch_calendar_events, filter_gold_events
from goldbot.real_yields import build_real_yield_signal, fetch_real_yield_history, signal_to_payload
from goldbot.cftc import signal_to_payload as cftc_signal_to_payload  # re-exported for downstream pipelines  # noqa: F401
from goldbot.shared_backend import save_json_payload


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
        "cftc": _read_existing_cftc(settings.macro_state_file),
    }
    output_path = Path(settings.macro_state_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if settings.gold_event_state_file != settings.macro_state_file:
        event_path = Path(settings.gold_event_state_file)
        event_path.parent.mkdir(parents=True, exist_ok=True)
        event_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    save_json_payload(settings.gold_event_state_file, payload, settings.gold_event_redis_key)
    log.info("Wrote %s relevant gold events to %s", len(relevant), output_path)


def _read_existing_cftc(macro_state_file: str) -> dict | None:
    """Preserve any externally-published CFTC payload across macro refreshes.

    The CoT report only refreshes weekly; the macro engine runs frequently.
    A separate pipeline (manual CSV import or scheduled fetch) is responsible
    for writing the ``cftc`` key. Without this passthrough, every macro
    refresh would clobber the positioning data with ``None``.
    """
    try:
        path = Path(macro_state_file)
        if not path.exists():
            return None
        existing = json.loads(path.read_text(encoding="utf-8"))
        cftc = existing.get("cftc")
        return cftc if isinstance(cftc, dict) else None
    except Exception:
        return None


if __name__ == "__main__":
    main()