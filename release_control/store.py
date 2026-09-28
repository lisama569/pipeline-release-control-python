"""内存存储：发布记录、审计事件与流水线级并发锁。"""
from __future__ import annotations

import threading

from .models import AuditEvent, Environment, ReleaseRecord, ReleaseStatus


class InMemoryStore:
    """进程内存储，所有方法在内部锁下执行，保证原子性。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._releases: dict[str, ReleaseRecord] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self._audits: list[AuditEvent] = []
        self._pipeline_locks: dict[str, threading.Lock] = {}

    def pipeline_lock(self, pipeline_id: str) -> threading.Lock:
        with self._lock:
            lock = self._pipeline_locks.get(pipeline_id)
            if lock is None:
                lock = threading.Lock()
                self._pipeline_locks[pipeline_id] = lock
            return lock

    def get(self, release_id: str) -> ReleaseRecord | None:
        with self._lock:
            return self._releases.get(release_id)

    def find_by_idempotency_key(self, pipeline_id: str, key: str) -> ReleaseRecord | None:
        with self._lock:
            release_id = self._idempotency.get((pipeline_id, key))
            return self._releases.get(release_id) if release_id else None

    def save(self, record: ReleaseRecord) -> None:
        with self._lock:
            self._releases[record.release_id] = record

    def bind_idempotency_key(self, pipeline_id: str, key: str, release_id: str) -> None:
        with self._lock:
            self._idempotency[(pipeline_id, key)] = release_id

    def current_release(self, pipeline_id: str, env: Environment) -> ReleaseRecord | None:
        with self._lock:
            candidates = [
                r
                for r in self._releases.values()
                if r.pipeline_id == pipeline_id
                and r.environment == env
                and r.status == ReleaseStatus.PUBLISHED
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda r: r.created_at)

    def releases_for(self, pipeline_id: str, env: Environment) -> list[ReleaseRecord]:
        with self._lock:
            return [
                r
                for r in self._releases.values()
                if r.pipeline_id == pipeline_id and r.environment == env
            ]

    def append_audit(self, event: AuditEvent) -> None:
        with self._lock:
            self._audits.append(event)

    def audits_for(self, release_id: str) -> list[AuditEvent]:
        with self._lock:
            return [a for a in self._audits if a.release_id == release_id]
