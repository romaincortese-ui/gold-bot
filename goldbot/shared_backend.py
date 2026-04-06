import json
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    import redis
except ImportError:
    redis = None  # type: ignore


_redis_client = None
_redis_url = None


def get_redis_client():
    global _redis_client, _redis_url
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url or redis is None:
        return None
    if _redis_client is not None and _redis_url == redis_url:
        return _redis_client
    try:
        _redis_client = redis.from_url(redis_url)
        _redis_url = redis_url
        return _redis_client
    except Exception:
        _redis_client = None
        _redis_url = None
        return None


def load_json_payload(file_path: str, redis_key: str | None = None, default: dict | None = None) -> dict:
    payload_default = dict(default or {})
    client = get_redis_client()
    if client is not None and redis_key:
        try:
            raw = client.get(redis_key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    path = Path(file_path)
    if not path.exists():
        return payload_default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return payload_default


def save_json_payload(file_path: str, payload: dict, redis_key: str | None = None) -> None:
    client = get_redis_client()
    if client is not None and redis_key:
        client.set(redis_key, json.dumps(payload))
        return
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def publish_runtime_status(service: str, state: str, *, redis_key: str | None, ttl_seconds: int, **fields) -> bool:
    client = get_redis_client()
    if client is None or not redis_key or ttl_seconds <= 0:
        return False
    payload = {
        "service": service,
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload.update(fields)
    client.set(redis_key, json.dumps(payload), ex=ttl_seconds)
    return True