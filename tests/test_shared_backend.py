import json
from datetime import datetime, timedelta, timezone

from goldbot import shared_backend


class BrokenRedisClient:
    def get(self, key):
        raise RuntimeError("redis get failed")

    def set(self, key, value, ex=None):
        raise RuntimeError("redis set failed")


def test_save_json_payload_falls_back_to_file_when_redis_set_fails(tmp_path, monkeypatch) -> None:
    target = tmp_path / "state.json"
    monkeypatch.setattr(shared_backend, "get_redis_client", lambda: BrokenRedisClient())

    shared_backend.save_json_payload(str(target), {"ok": True}, "test_key")

    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}


def test_publish_runtime_status_falls_back_to_file_when_redis_set_fails(tmp_path, monkeypatch) -> None:
    target = tmp_path / "status.json"
    monkeypatch.setattr(shared_backend, "get_redis_client", lambda: BrokenRedisClient())

    published = shared_backend.publish_runtime_status(
        service="gold-bot",
        state="idle",
        redis_key="gold_bot_runtime_status",
        ttl_seconds=1800,
        file_path=str(target),
        last_run_at="2026-04-07T07:00:00+00:00",
    )

    assert published is True
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["state"] == "idle"
    assert payload["last_run_at"] == "2026-04-07T07:00:00+00:00"


def test_load_runtime_status_falls_back_to_file_when_redis_get_fails(tmp_path, monkeypatch) -> None:
    target = tmp_path / "status.json"
    target.write_text('{"state": "idle", "generated_at": "2026-04-07T07:00:00+00:00"}', encoding="utf-8")
    monkeypatch.setattr(shared_backend, "get_redis_client", lambda: BrokenRedisClient())

    payload = shared_backend.load_runtime_status("gold_bot_runtime_status", str(target))

    assert payload is not None
    assert payload["state"] == "idle"


def test_load_runtime_status_rejects_stale_file_payload(tmp_path, monkeypatch) -> None:
    target = tmp_path / "status.json"
    stale_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    target.write_text(json.dumps({"state": "idle", "generated_at": stale_time}), encoding="utf-8")
    monkeypatch.setattr(shared_backend, "get_redis_client", lambda: None)

    payload = shared_backend.load_runtime_status(None, str(target), max_age_seconds=60)

    assert payload is None


class WorkingRedisClient:
    def ping(self):
        return True


class FakeRedisModule:
    class WatchError(Exception):
        pass

    def __init__(self) -> None:
        self.urls: list[str] = []

    def from_url(self, url, **kwargs):
        self.urls.append(url)
        if "private" in url:
            raise TimeoutError("private endpoint timeout")
        return WorkingRedisClient()


def test_get_redis_client_prefers_public_proxy_and_caches_client(monkeypatch) -> None:
    fake_redis = FakeRedisModule()
    monkeypatch.setattr(shared_backend, "redis", fake_redis)
    monkeypatch.setattr(shared_backend, "_redis_client", None)
    monkeypatch.setattr(shared_backend, "_redis_url", None)
    shared_backend._redis_failed_until.clear()
    monkeypatch.setenv("REDIS_URL", "redis://private:6379")
    monkeypatch.setenv("REDIS_PUBLIC_URL", "redis://public:42277")
    monkeypatch.setenv("GOLD_REDIS_PREFER_PUBLIC", "true")

    client = shared_backend.get_redis_client()
    cached = shared_backend.get_redis_client()

    assert isinstance(client, WorkingRedisClient)
    assert cached is client
    assert fake_redis.urls == ["redis://public:42277"]


def test_get_redis_client_cools_down_failed_private_endpoint(monkeypatch) -> None:
    fake_redis = FakeRedisModule()
    monkeypatch.setattr(shared_backend, "redis", fake_redis)
    monkeypatch.setattr(shared_backend, "_redis_client", None)
    monkeypatch.setattr(shared_backend, "_redis_url", None)
    shared_backend._redis_failed_until.clear()
    monkeypatch.setenv("REDIS_URL", "redis://private:6379")
    monkeypatch.delenv("REDIS_PUBLIC_URL", raising=False)
    monkeypatch.setenv("GOLD_REDIS_FAILED_URL_COOLDOWN_SECONDS", "60")

    assert shared_backend.get_redis_client(_retries=1, _delay=0) is None
    assert shared_backend.get_redis_client(_retries=1, _delay=0) is None

    assert fake_redis.urls == ["redis://private:6379"]