"""Checkpoint stores: in-process (tests/single worker) and Redis (multi-worker).

The store is intentionally small: one JSON document per run, a lease key, an
idempotency index, and an append-only event list. That keeps the durable
semantics inspectable and lets CI run against ``fakeredis`` with no server.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Protocol

from .models import WorkflowRun


class RunNotFound(KeyError):
    pass


class RunLocked(RuntimeError):
    pass


class WorkflowStore(Protocol):
    def create(self, run: WorkflowRun) -> WorkflowRun: ...
    def load(self, run_id: str) -> WorkflowRun: ...
    def save(self, run: WorkflowRun, *, owner: str | None = None) -> WorkflowRun: ...
    def find_by_idempotency(self, key: str) -> str | None: ...
    def acquire_lease(self, run_id: str, owner: str, ttl_seconds: int) -> bool: ...
    def lease_holder(self, run_id: str) -> tuple[str, float] | None: ...
    def release_lease(self, run_id: str, owner: str) -> None: ...
    def append_event(self, run_id: str, event: dict[str, Any]) -> None: ...
    def events(self, run_id: str) -> list[dict[str, Any]]: ...


class InMemoryStore:
    """Thread-safe store for one process. Not durable across restarts."""

    def __init__(self) -> None:
        self._runs: dict[str, WorkflowRun] = {}
        self._idem: dict[str, str] = {}
        self._leases: dict[str, tuple[str, float]] = {}
        self._events: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.RLock()

    def create(self, run: WorkflowRun) -> WorkflowRun:
        with self._lock:
            if run.idempotency_key and run.idempotency_key in self._idem:
                return self.load(self._idem[run.idempotency_key])
            self._runs[run.run_id] = run
            if run.idempotency_key:
                self._idem[run.idempotency_key] = run.run_id
            self._events.setdefault(run.run_id, [])
            return run

    def load(self, run_id: str) -> WorkflowRun:
        with self._lock:
            if run_id not in self._runs:
                raise RunNotFound(run_id)
            data = json.loads(json.dumps(self._runs[run_id].to_dict()))
            return WorkflowRun.from_dict(data)

    def save(self, run: WorkflowRun, *, owner: str | None = None) -> WorkflowRun:
        with self._lock:
            if run.run_id not in self._runs:
                raise RunNotFound(run.run_id)
            held = self._leases.get(run.run_id)
            if owner is not None and (held is None or held[0] != owner):
                raise RunLocked(f"lease for {run.run_id} is not held by {owner}")
            run.version += 1
            run.updated_at = time.time()
            self._runs[run.run_id] = WorkflowRun.from_dict(run.to_dict())
            return run

    def find_by_idempotency(self, key: str) -> str | None:
        with self._lock:
            return self._idem.get(key)

    def acquire_lease(self, run_id: str, owner: str, ttl_seconds: int) -> bool:
        with self._lock:
            if run_id not in self._runs:
                raise RunNotFound(run_id)
            now = time.time()
            held = self._leases.get(run_id)
            if held and held[1] > now and held[0] != owner:
                return False
            self._leases[run_id] = (owner, now + ttl_seconds)
            return True

    def lease_holder(self, run_id: str) -> tuple[str, float] | None:
        with self._lock:
            return self._leases.get(run_id)

    def release_lease(self, run_id: str, owner: str) -> None:
        with self._lock:
            held = self._leases.get(run_id)
            if held and held[0] == owner:
                self._leases.pop(run_id, None)

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        with self._lock:
            self._events.setdefault(run_id, []).append(event)

    def events(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events.get(run_id, []))


class RedisStore:
    """Redis-backed checkpoints, idempotency index and leases.

    ``client`` is any redis-py compatible client (``redis.Redis`` in
    production, ``fakeredis.FakeRedis`` in tests). Values are JSON strings so
    the on-disk format stays readable and versionable.
    """

    def __init__(self, client: Any, prefix: str = "expenseiq:wf") -> None:
        self.client = client
        self.prefix = prefix

    def _key(self, run_id: str) -> str:
        return f"{self.prefix}:run:{run_id}"

    def _idem_key(self, key: str) -> str:
        return f"{self.prefix}:idem:{key}"

    def _lease_key(self, run_id: str) -> str:
        return f"{self.prefix}:lease:{run_id}"

    def _events_key(self, run_id: str) -> str:
        return f"{self.prefix}:events:{run_id}"

    def create(self, run: WorkflowRun) -> WorkflowRun:
        if run.idempotency_key:
            claimed = self.client.set(self._idem_key(run.idempotency_key), run.run_id, nx=True)
            if not claimed:
                existing = self.find_by_idempotency(run.idempotency_key)
                if existing:
                    return self.load(existing)
        self.client.set(self._key(run.run_id), json.dumps(run.to_dict()))
        return run

    def load(self, run_id: str) -> WorkflowRun:
        raw = self.client.get(self._key(run_id))
        if raw is None:
            raise RunNotFound(run_id)
        return WorkflowRun.from_dict(json.loads(raw))

    def save(self, run: WorkflowRun, *, owner: str | None = None) -> WorkflowRun:
        if owner is not None and not self._holds_lease(run.run_id, owner):
            raise RunLocked(f"lease for {run.run_id} is not held by {owner}")
        run.version += 1
        run.updated_at = time.time()
        self.client.set(self._key(run.run_id), json.dumps(run.to_dict()))
        return run

    def _holds_lease(self, run_id: str, owner: str) -> bool:
        current = self.client.get(self._lease_key(run_id))
        if current is None:
            return False
        if isinstance(current, bytes):
            current = current.decode()
        return str(current) == owner

    def find_by_idempotency(self, key: str) -> str | None:
        value = self.client.get(self._idem_key(key))
        if value is None:
            return None
        return value.decode() if isinstance(value, bytes) else str(value)

    def acquire_lease(self, run_id: str, owner: str, ttl_seconds: int) -> bool:
        if self.client.get(self._key(run_id)) is None:
            raise RunNotFound(run_id)
        return bool(self.client.set(self._lease_key(run_id), owner, nx=True, ex=ttl_seconds))

    def lease_holder(self, run_id: str) -> tuple[str, float] | None:
        owner = self.client.get(self._lease_key(run_id))
        if owner is None:
            return None
        if isinstance(owner, bytes):
            owner = owner.decode()
        ttl = self.client.ttl(self._lease_key(run_id))
        expires_at = time.time() + max(0, int(ttl)) if int(ttl) > 0 else time.time()
        return str(owner), expires_at

    def release_lease(self, run_id: str, owner: str) -> None:
        if self._holds_lease(run_id, owner):
            self.client.delete(self._lease_key(run_id))

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        self.client.rpush(self._events_key(run_id), json.dumps(event))

    def events(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.client.lrange(self._events_key(run_id), 0, -1)
        return [json.loads(row) for row in rows]


def build_store(redis_url: str | None = None) -> WorkflowStore:
    if redis_url:
        import redis

        return RedisStore(redis.from_url(redis_url))
    return InMemoryStore()
