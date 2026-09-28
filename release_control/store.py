"""内存存储：发布记录、审计事件与流水线级并发锁。"""
from __future__ import annotations

import threading

from .models import AuditEvent, Environment, ReleaseRecord


class InMemoryStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._releases: dict[str, ReleaseRecord] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self._audits: list[AuditEvent] = []
        self._pipeline_locks: dict[str, threading.Lock] = {}

    def pipeline_lock(self, pipeline_id: str) -> threading.Lock:
        raise NotImplementedError

    def get(self, release_id: str) -> ReleaseRecord | None:
        raise NotImplementedError

    def find_by_idempotency_key(self, pipeline_id: str, key: str) -> ReleaseRecord | None:
        raise NotImplementedError

    def save(self, record: ReleaseRecord) -> None:
        raise NotImplementedError

    def bind_idempotency_key(self, pipeline_id: str, key: str, release_id: str) -> None:
        raise NotImplementedError

    def current_release(self, pipeline_id: str, env: Environment) -> ReleaseRecord | None:
        raise NotImplementedError

    def append_audit(self, event: AuditEvent) -> None:
        raise NotImplementedError

    def audits_for(self, release_id: str) -> list[AuditEvent]:
        raise NotImplementedError
