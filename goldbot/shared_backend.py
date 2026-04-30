import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import redis
except ImportError:
    redis = None  # type: ignore


log = logging.getLogger(__name__)


_redis_client = None
_redis_url = None
_redis_failed_until: dict[str, float] = {}


def _invalidate_redis_client() -> None:
    global _redis_client, _redis_url
    _redis_client = None
    _redis_url = None


def get_redis_client(*, _retries: int = 1, _delay: float = 0.5):
    global _redis_client, _redis_url
    urls_to_try = _redis_urls_to_try()
    if not urls_to_try or redis is None:
        return None
    if _redis_client is not None and _redis_url in urls_to_try:
        return _redis_client

    now = time.monotonic()
    cooldown = max(0.0, float(os.getenv("GOLD_REDIS_FAILED_URL_COOLDOWN_SECONDS", "900") or 900))
    connect_timeout = max(0.2, float(os.getenv("GOLD_REDIS_CONNECT_TIMEOUT_SECONDS", "2") or 2))
    retries = max(1, int(os.getenv("GOLD_REDIS_CONNECT_RETRIES", str(_retries)) or _retries))
    for redis_url in urls_to_try:
        if _redis_failed_until.get(redis_url, 0.0) > now:
            continue
        for attempt in range(1, retries + 1):
            try:
                _redis_client = redis.from_url(
                    redis_url,
                    socket_connect_timeout=connect_timeout,
                    socket_timeout=connect_timeout,
                    health_check_interval=30,
                    retry_on_timeout=True,
                )
                _redis_client.ping()
                _redis_url = redis_url
                _redis_failed_until.pop(redis_url, None)
                log.info("Redis connected via %s", _redis_url_label(redis_url))
                return _redis_client
            except Exception as exc:
                _invalidate_redis_client()
                if attempt >= retries:
                    _redis_failed_until[redis_url] = time.monotonic() + cooldown
                    log.warning(
                        "Redis connection failed (%s); suppressing retries for %.0fs: %s",
                        _redis_url_label(redis_url),
                        cooldown,
                        exc,
                    )
                elif _delay > 0:
                    time.sleep(_delay * attempt)
    return None


def _redis_urls_to_try() -> list[str]:
    urls: list[str] = []
    preferred = "REDIS_PUBLIC_URL" if env_bool("GOLD_REDIS_PREFER_PUBLIC", True) else "REDIS_URL"
    for var in (preferred, "REDIS_URL", "REDIS_PUBLIC_URL"):
        url = os.getenv(var, "").strip()
        if url and url not in urls:
            urls.append(url)
    if _redis_url in urls:
        urls.remove(_redis_url)
        urls.insert(0, _redis_url)
    return urls


def _redis_url_label(redis_url: str) -> str:
    return redis_url.split("@")[-1] if "@" in redis_url else redis_url.split("//")[-1]


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_json_payload(file_path: str, redis_key: str | None = None, default: dict | None = None) -> dict:
    payload_default = dict(default or {})
    client = get_redis_client()
    if client is not None and redis_key:
        try:
            raw = client.get(redis_key)
            if raw:
                return json.loads(raw)
        except Exception:
            _invalidate_redis_client()
    path = Path(file_path)
    if not path.exists():
        return payload_default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        log.critical("State file corrupted: %s — returning default to avoid data loss", path)
        backup = path.with_suffix(path.suffix + ".corrupt")
        try:
            path.rename(backup)
            log.critical("Corrupted file preserved as %s", backup)
        except OSError:
            pass
        return payload_default


def save_json_payload(file_path: str, payload: dict, redis_key: str | None = None) -> None:
    client = get_redis_client()
    if client is not None and redis_key:
        try:
            client.set(redis_key, json.dumps(payload))
        except Exception:
            _invalidate_redis_client()
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)  # atomic on most OS


def merge_bot_budget_slot(file_path: str, redis_key: str | None, bot_name: str, slot_data: dict, *, _max_attempts: int = 3) -> bool:
    """Atomically update only the given bot's slot in the shared budget payload.

    Uses Redis WATCH/MULTI for optimistic locking when Redis is available,
    so concurrent writes from the sibling bot don't clobber each other.
    Falls back to a simple read-modify-write on file when Redis is unavailable.
    """
    client = get_redis_client()

    # ── Redis path: optimistic lock via WATCH ──
    if client is not None and redis_key and redis is not None:
        for attempt in range(_max_attempts):
            try:
                pipe = client.pipeline(True)  # MULTI/EXEC pipeline
                pipe.watch(redis_key)
                raw = pipe.get(redis_key)
                payload = json.loads(raw) if raw else {"bots": {}}
                if not isinstance(payload, dict):
                    payload = {"bots": {}}
                bots = payload.setdefault("bots", {})
                bots[bot_name] = slot_data
                pipe.multi()
                pipe.set(redis_key, json.dumps(payload))
                pipe.execute()
                # Also persist to file for local reads
                _write_file_atomic(file_path, payload)
                return True
            except redis.WatchError:
                log.debug("Budget WATCH conflict (attempt %d/%d)", attempt + 1, _max_attempts)
                continue
            except Exception as exc:
                log.warning("Redis merge_bot_budget_slot failed: %s", exc)
                _invalidate_redis_client()
                break  # fall through to file path

    # ── File-only path ──
    path = Path(file_path)
    payload: dict = {"bots": {}}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {"bots": {}}
        except (json.JSONDecodeError, OSError):
            payload = {"bots": {}}
    bots = payload.setdefault("bots", {})
    bots[bot_name] = slot_data
    _write_file_atomic(file_path, payload)
    return True


def _write_file_atomic(file_path: str, payload: dict) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def publish_runtime_status(service: str, state: str, *, redis_key: str | None, ttl_seconds: int, file_path: str | None = None, **fields) -> bool:
    client = get_redis_client()
    payload = {
        "service": service,
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload.update(fields)
    published = False
    if client is not None and redis_key and ttl_seconds > 0:
        try:
            client.set(redis_key, json.dumps(payload), ex=ttl_seconds)
            published = True
        except Exception:
            _invalidate_redis_client()
    if file_path:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        published = True
    return published


def load_runtime_status(redis_key: str | None, file_path: str | None = None, max_age_seconds: int | None = None) -> dict | None:
    client = get_redis_client()
    if client is not None and redis_key:
        try:
            raw = client.get(redis_key)
            if raw:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    return payload
        except Exception:
            _invalidate_redis_client()
    if not file_path:
        return None
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        if max_age_seconds is not None and max_age_seconds > 0:
            generated_at = payload.get("generated_at")
            if not generated_at:
                return None
            try:
                parsed = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - parsed).total_seconds()
            if age_seconds > max_age_seconds:
                return None
        return payload
    except Exception:
        return None