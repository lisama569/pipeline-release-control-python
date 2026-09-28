"""发布 / 回滚 / 取消的核心业务逻辑。

一致性约定：
- 每条流水线一把锁，非阻塞获取；拿不到即并发冲突，明确拒绝。
- 同一 (pipeline_id, idempotency_key) 重复提交直接返回首次结果，不产生新记录。
- 所有状态迁移走 _transition，校验合法性并写审计，状态与审计始终一一对应。
- 回滚先完成全部校验再一次性变更，不会留下改了一半的状态。
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from . import auth
from .errors import ReasonCode, ReleaseError
from .models import (
    AuditEvent,
    Build,
    Caller,
    Environment,
    ReleaseRecord,
    ReleaseStatus,
)
from .store import InMemoryStore

logger = logging.getLogger("release_control")

# 当前系统兼容的产物 schema 版本
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})

_ALLOWED_TRANSITIONS: dict[ReleaseStatus, frozenset[ReleaseStatus]] = {
    ReleaseStatus.PENDING: frozenset(
        {ReleaseStatus.PUBLISHED, ReleaseStatus.FAILED, ReleaseStatus.CANCELLED}
    ),
    ReleaseStatus.PUBLISHED: frozenset({ReleaseStatus.ROLLED_BACK}),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ReleaseService:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    # ---- 内部工具 ----

    def _transition(
        self,
        record: ReleaseRecord,
        to_status: ReleaseStatus,
        request_id: str,
        actor: str,
        reason: str,
    ) -> None:
        allowed = _ALLOWED_TRANSITIONS.get(record.status, frozenset())
        if to_status not in allowed:
            raise ReleaseError(
                ReasonCode.INVALID_STATE_TRANSITION,
                f"release={record.release_id} 不允许 {record.status.value} -> {to_status.value}",
            )
        from_status = record.status
        record.status = to_status
        record.updated_at = _now()
        record.history.append((from_status.value, to_status.value, reason))
        self._store.save(record)
        self._store.append_audit(
            AuditEvent(
                request_id=request_id,
                release_id=record.release_id,
                from_status=from_status.value,
                to_status=to_status.value,
                reason=reason,
                actor=actor,
                at=record.updated_at,
            )
        )
        logger.info(
            "request_id=%s release=%s transition %s -> %s reason=%s",
            request_id, record.release_id, from_status.value, to_status.value, reason,
        )

    def _acquire_pipeline_lock(self, pipeline_id: str, request_id: str):
        lock = self._store.pipeline_lock(pipeline_id)
        if not lock.acquire(blocking=False):
            logger.info(
                "request_id=%s pipeline=%s 判定依据: 流水线锁被占用 -> concurrent_conflict",
                request_id, pipeline_id,
            )
            raise ReleaseError(
                ReasonCode.CONCURRENT_CONFLICT,
                f"pipeline={pipeline_id} 正被其他请求操作",
            )
        return lock

    def _idempotent_replay(
        self, pipeline_id: str, idempotency_key: str, request_id: str
    ) -> ReleaseRecord | None:
        existing = self._store.find_by_idempotency_key(pipeline_id, idempotency_key)
        if existing is not None:
            logger.info(
                "request_id=%s pipeline=%s 判定依据: 幂等键 %s 命中 release=%s，直接返回首次结果",
                request_id, pipeline_id, idempotency_key, existing.release_id,
            )
        return existing

    # ---- 发布 ----

    def publish(
        self,
        caller: Caller,
        pipeline_id: str,
        env: Environment,
        build: Build,
        idempotency_key: str,
        request_id: str,
    ) -> ReleaseRecord:
        auth.require(caller, "publish")
        if build.pipeline_id != pipeline_id:
            raise ReleaseError(
                ReasonCode.VERSION_INCOMPATIBLE,
                f"build={build.build_id} 属于 pipeline={build.pipeline_id}，不能发布到 {pipeline_id}",
            )
        if build.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ReleaseError(
                ReasonCode.VERSION_INCOMPATIBLE,
                f"build={build.build_id} schema_version={build.schema_version} 不受支持",
            )
        lock = self._acquire_pipeline_lock(pipeline_id, request_id)
        try:
            existing = self._idempotent_replay(pipeline_id, idempotency_key, request_id)
            if existing is not None:
                if existing.environment != env or existing.build.build_id != build.build_id:
                    raise ReleaseError(
                        ReasonCode.DUPLICATE_REQUEST,
                        f"幂等键 {idempotency_key} 已被不同参数的发布占用",
                    )
                return existing
            record = ReleaseRecord(
                release_id=uuid.uuid4().hex,
                pipeline_id=pipeline_id,
                environment=env,
                build=build,
                status=ReleaseStatus.PENDING,
                idempotency_key=idempotency_key,
                created_by=caller.caller_id,
                created_at=_now(),
                updated_at=_now(),
            )
            self._store.save(record)
            self._store.bind_idempotency_key(pipeline_id, idempotency_key, record.release_id)
            self._store.append_audit(
                AuditEvent(
                    request_id=request_id,
                    release_id=record.release_id,
                    from_status="",
                    to_status=ReleaseStatus.PENDING.value,
                    reason="创建发布记录",
                    actor=caller.caller_id,
                    at=record.created_at,
                )
            )
            self._transition(
                record, ReleaseStatus.PUBLISHED, request_id, caller.caller_id,
                f"发布 build={build.build_id} 到 {env.value}",
            )
            return record
        finally:
            lock.release()

    # ---- 回滚 ----

    def rollback(
        self,
        caller: Caller,
        pipeline_id: str,
        env: Environment,
        target_release_id: str,
        idempotency_key: str,
        request_id: str,
    ) -> ReleaseRecord:
        auth.require(caller, "rollback")
        lock = self._acquire_pipeline_lock(pipeline_id, request_id)
        try:
            existing = self._idempotent_replay(pipeline_id, idempotency_key, request_id)
            if existing is not None:
                return existing
            # 先校验，后变更：任何一步失败都不改动现有状态
            target = self._store.get(target_release_id)
            if target is None or target.pipeline_id != pipeline_id or target.environment != env:
                raise ReleaseError(
                    ReasonCode.VERSION_NOT_FOUND,
                    f"目标版本 release={target_release_id} 在 pipeline={pipeline_id} env={env.value} 下不存在",
                )
            if target.status != ReleaseStatus.PUBLISHED:
                raise ReleaseError(
                    ReasonCode.VERSION_INCOMPATIBLE,
                    f"目标版本 release={target_release_id} 状态为 {target.status.value}，不是可回滚的已发布版本",
                )
            if target.build.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
                raise ReleaseError(
                    ReasonCode.VERSION_INCOMPATIBLE,
                    f"目标版本 schema_version={target.build.schema_version} 与当前系统不兼容",
                )
            current = self._store.current_release(pipeline_id, env)
            record = ReleaseRecord(
                release_id=uuid.uuid4().hex,
                pipeline_id=pipeline_id,
                environment=env,
                build=target.build,  # 产物与配置跟当时那次发布完全一致
                status=ReleaseStatus.PENDING,
                idempotency_key=idempotency_key,
                created_by=caller.caller_id,
                created_at=_now(),
                updated_at=_now(),
            )
            self._store.save(record)
            self._store.bind_idempotency_key(pipeline_id, idempotency_key, record.release_id)
            self._store.append_audit(
                AuditEvent(
                    request_id=request_id,
                    release_id=record.release_id,
                    from_status="",
                    to_status=ReleaseStatus.PENDING.value,
                    reason=f"创建回滚记录，源版本={target_release_id}",
                    actor=caller.caller_id,
                    at=record.created_at,
                )
            )
            self._transition(
                record, ReleaseStatus.PUBLISHED, request_id, caller.caller_id,
                f"回滚到 release={target_release_id} 的 build={target.build.build_id}",
            )
            if current is not None and current.release_id != target_release_id:
                self._transition(
                    current, ReleaseStatus.ROLLED_BACK, request_id, caller.caller_id,
                    f"被回滚取代，回滚记录={record.release_id}",
                )
            return record
        finally:
            lock.release()

    # ---- 取消 / 失败 ----

    def cancel(self, caller: Caller, release_id: str, request_id: str) -> ReleaseRecord:
        auth.require(caller, "cancel")
        record = self._store.get(release_id)
        if record is None:
            raise ReleaseError(ReasonCode.NOT_FOUND, f"release={release_id} 不存在")
        lock = self._acquire_pipeline_lock(record.pipeline_id, request_id)
        try:
            self._transition(record, ReleaseStatus.CANCELLED, request_id, caller.caller_id, "调用方取消")
            return record
        finally:
            lock.release()

    def fail(self, caller: Caller, release_id: str, reason: str, request_id: str) -> ReleaseRecord:
        auth.require(caller, "cancel")
        record = self._store.get(release_id)
        if record is None:
            raise ReleaseError(ReasonCode.NOT_FOUND, f"release={release_id} 不存在")
        lock = self._acquire_pipeline_lock(record.pipeline_id, request_id)
        try:
            self._transition(record, ReleaseStatus.FAILED, request_id, caller.caller_id, reason)
            return record
        finally:
            lock.release()

    # ---- 查询 ----

    def get_release(self, caller: Caller, release_id: str) -> ReleaseRecord:
        auth.require(caller, "read")
        record = self._store.get(release_id)
        if record is None:
            raise ReleaseError(ReasonCode.NOT_FOUND, f"release={release_id} 不存在")
        return record
