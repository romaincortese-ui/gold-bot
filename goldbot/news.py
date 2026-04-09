import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

try:
    from defusedxml.ElementTree import fromstring as safe_fromstring
except ImportError:
    safe_fromstring = None

from goldbot.models import CalendarEvent


log = logging.getLogger(__name__)

GOLD_NEWS_KEYWORDS = (
    "nfp",
    "non-farm",
    "cpi",
    "core cpi",
    "fomc",
    "powell",
    "fed",
    "interest rate",
    "pce",
    "inflation",
)


def fetch_calendar_events(urls: list[str], cache_file: str, timeout: int = 20) -> list[CalendarEvent]:
    for url in urls:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            events = parse_calendar_events(response.text, url)
            if events:
                _write_cache(cache_file, events)
                return events
        except Exception as exc:
            log.warning("Calendar fetch failed for %s: %s", url, exc)
    return _read_cache(cache_file)


def parse_calendar_events(xml_text: str, source: str) -> list[CalendarEvent]:
    if safe_fromstring is not None:
        root = safe_fromstring(xml_text)
    else:
        parser = ET.XMLParser()
        root = ET.fromstring(xml_text, parser=parser)  # noqa: S314
    nodes = list(root.findall(".//item")) + list(root.findall(".//event"))
    events: list[CalendarEvent] = []
    for node in nodes:
        title = _find_text(node, "title", "event", "name")
        currency = (_find_text(node, "currency", "country") or "").upper()
        impact = (_find_text(node, "impact", "impact_title") or "").lower()
        occurs_at = _parse_occurs_at(node)
        if not title or occurs_at is None:
            continue
        events.append(
            CalendarEvent(
                title=title.strip(),
                currency=currency.strip(),
                impact=impact.strip() or "unknown",
                occurs_at=occurs_at,
                source=source,
            )
        )
    events.sort(key=lambda event: event.occurs_at)
    return events


def filter_gold_events(
    events: list[CalendarEvent],
    *,
    now: datetime,
    lookback_hours: int,
    lookahead_hours: int,
) -> list[CalendarEvent]:
    start = now - timedelta(hours=lookback_hours)
    end = now + timedelta(hours=lookahead_hours)
    filtered: list[CalendarEvent] = []
    for event in events:
        if event.occurs_at < start or event.occurs_at > end:
            continue
        impact_text = event.impact.lower()
        title_text = event.title.lower()
        is_high_impact = any(word in impact_text for word in ("high", "red"))
        is_gold_relevant = event.currency in {"USD", "US"} and any(keyword in title_text for keyword in GOLD_NEWS_KEYWORDS)
        if is_high_impact and is_gold_relevant:
            filtered.append(event)
    return filtered


def _find_text(node: ET.Element, *names: str) -> str | None:
    lowered = {child.tag.lower().split("}")[-1]: (child.text or "").strip() for child in node.iter() if child is not node}
    for name in names:
        value = lowered.get(name.lower())
        if value:
            return value
    return None


def _parse_occurs_at(node: ET.Element) -> datetime | None:
    date_text = _find_text(node, "date")
    time_text = _find_text(node, "time")
    if date_text and time_text:
        for fmt in (
            "%m-%d-%Y %I:%M%p",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d %H:%M",
            "%a%b %d %I:%M%p",
        ):
            try:
                parsed = datetime.strptime(f"{date_text} {time_text}", fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    pub_date = _find_text(node, "pubdate", "pubDate")
    if pub_date:
        try:
            parsed = parsedate_to_datetime(pub_date)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _write_cache(cache_file: str, events: list[CalendarEvent]) -> None:
    path = Path(cache_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "title": event.title,
            "currency": event.currency,
            "impact": event.impact,
            "occurs_at": event.occurs_at.isoformat(),
            "source": event.source,
        }
        for event in events
    ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_cache(cache_file: str) -> list[CalendarEvent]:
    path = Path(cache_file)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    events: list[CalendarEvent] = []
    for item in payload:
        try:
            events.append(
                CalendarEvent(
                    title=str(item["title"]),
                    currency=str(item["currency"]),
                    impact=str(item["impact"]),
                    occurs_at=datetime.fromisoformat(str(item["occurs_at"])).astimezone(timezone.utc),
                    source=str(item.get("source", "cache")),
                )
            )
        except Exception:
            continue
    return events