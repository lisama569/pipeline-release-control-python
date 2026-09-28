"""发布 / 回滚 / 取消的核心业务逻辑。"""
from __future__ import annotations

from .models import Build, Caller, Environment, ReleaseRecord
from .store import InMemoryStore


class ReleaseService:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    def publish(
        self,
        caller: Caller,
        pipeline_id: str,
        env: Environment,
        build: Build,
        idempotency_key: str,
        request_id: str,
    ) -> ReleaseRecord:
        raise NotImplementedError

    def rollback(
        self,
        caller: Caller,
        pipeline_id: str,
        env: Environment,
        target_release_id: str,
        idempotency_key: str,
        request_id: str,
    ) -> ReleaseRecord:
        raise NotImplementedError

    def cancel(
        self,
        caller: Caller,
        release_id: str,
        request_id: str,
    ) -> ReleaseRecord:
        raise NotImplementedError

    def get_release(self, caller: Caller, release_id: str) -> ReleaseRecord:
        raise NotImplementedError
