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


def _invalidate_redis_client() -> None:
    global _redis_client, _redis_url
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
        _redis_client = redis.from_url(
            redis_url,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30,
            retry_on_timeout=True,
        )
        _redis_client.ping()
        _redis_url = redis_url
        return _redis_client
    except Exception:
        _invalidate_redis_client()
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
            _invalidate_redis_client()
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
        try:
            client.set(redis_key, json.dumps(payload))
        except Exception:
            _invalidate_redis_client()
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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