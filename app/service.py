"""发布 / 回滚控制服务。

职责（全部在流水线排他锁内完成，保证不留中间态）：

1. 权限前置校验（越权直接拒绝，见 auth.require_permission）。
2. 幂等：相同 ``Idempotency-Key`` 的重复提交直接重放原结果；同键但
   请求体不同 -> ``IDEMPOTENCY_CONFLICT``，不产生第二条记录。
3. 并发：同一流水线存在在途（pending）变更时，另一个变更请求
   -> ``CONCURRENT_MODIFICATION``，并指明挡路的 release_id。
4. 状态机：只允许 models.ALLOWED_TRANSITIONS 中的迁移，非法迁移拒绝。
5. 回滚：目标版本必须在该环境成功发布过、与当前配置代际兼容、且不
   是当前生效版本；校验全部通过后才落盘，失败不改任何状态。

每次受理或拒绝都写审计并打日志，携带 request_id、状态迁移和判定依据。
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from . import auth
from .auth import Principal
from .errors import ErrorCode, ServiceError
from .models import (
    ALLOWED_TRANSITIONS,
    Artifact,
    AuditEntry,
    Release,
    ReleaseStatus,
)
from .store import IdempotencyRecord, Store

logger = logging.getLogger("release_control")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class ReleaseService:
    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()
        # 故障注入钩子：publish 进入 pending 后调用，抛 ServiceError 模拟失败。
        # key 为 pipeline_id，测试用；默认不注入。
        self.publish_hooks: dict[str, Callable[[Release], None]] = {}
        # 持锁延迟（秒）：进入 pending 后、提交前短暂持锁，
        # 用于测试确定性地制造并发窗口。生产保持 0。
        self.mutation_delay: dict[str, float] = {}

    # -----------------------------------------------------------------
    # 内部工具
    # -----------------------------------------------------------------
    @staticmethod
    def _fingerprint(environment: str, artifact: Artifact) -> str:
        body = {
            "environment": environment,
            "artifact_id": artifact.artifact_id,
            "version": artifact.version,
            "checksum": artifact.checksum,
            "config_snapshot": artifact.config_snapshot,
            "compatible_generations": list(artifact.compatible_generations),
        }
        raw = json.dumps(body, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _rollback_fingerprint(environment: str, target_version: str) -> str:
        raw = json.dumps(
            {"environment": environment, "target_version": target_version},
            sort_keys=True,
        )
        return "rollback:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _audit(
        self,
        *,
        request_id: str,
        actor: str,
        action: str,
        pipeline_id: str,
        environment: str,
        release_id: str,
        from_status: str,
        to_status: str,
        reason: str = "",
        idempotency_key: str = "",
    ) -> None:
        entry = AuditEntry(
            audit_id=new_id("aud"),
            request_id=request_id,
            timestamp=utc_now(),
            actor=actor,
            action=action,
            pipeline_id=pipeline_id,
            environment=environment,
            release_id=release_id,
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            idempotency_key=idempotency_key,
        )
        self.store.append_audit(entry)
        logger.info(
            "audit request_id=%s actor=%s action=%s pipeline=%s env=%s "
            "release=%s transition=%s->%s reason=%s idempotency_key=%s",
            request_id, actor, action, pipeline_id, environment,
            release_id, from_status, to_status, reason or "-", idempotency_key or "-",
        )

    def _deny(
        self,
        *,
        request_id: str,
        actor: str,
        action: str,
        pipeline_id: str,
        environment: str,
        err: ServiceError,
        release_id: str = "",
        idempotency_key: str = "",
    ) -> None:
        """拒绝也要留痕：审计 + 日志，再把原错误抛出。"""
        self._audit(
            request_id=request_id, actor=actor, action=f"{action}.denied",
            pipeline_id=pipeline_id, environment=environment,
            release_id=release_id or err.conflict_with,
            from_status="", to_status="", reason=f"{err.code}: {err.message}",
            idempotency_key=idempotency_key,
        )
        logger.warning(
            "denied request_id=%s action=%s pipeline=%s env=%s code=%s detail=%s",
            request_id, action, pipeline_id, environment, err.code, err.message,
        )
        raise err

    def _authorize(
        self, principal: Principal, action: str, *, request_id: str,
        pipeline_id: str, environment: str = "", idempotency_key: str = "",
    ) -> None:
        """权限前置校验；越权也要留审计再拒绝。"""
        try:
            auth.require_permission(principal, action)
        except ServiceError as err:
            self._deny(
                request_id=request_id, actor=principal.username, action=action,
                pipeline_id=pipeline_id, environment=environment, err=err,
                idempotency_key=idempotency_key,
            )

    def _transition(self, release: Release, target: ReleaseStatus) -> None:
        allowed = ALLOWED_TRANSITIONS.get(release.status, frozenset())
        if target not in allowed:
            raise ServiceError(
                ErrorCode.INVALID_STATUS_TRANSITION,
                f"release '{release.release_id}' cannot move from "
                f"{release.status.value} to {target.value}",
                conflict_with=release.release_id,
            )
        release.status = target
        release.updated_at = utc_now()

    # -----------------------------------------------------------------
    # 流水线注册（admin）
    # -----------------------------------------------------------------
    def register_pipeline(
        self, principal: Principal, pipeline_id: str,
        config_generation: str, request_id: str = "",
    ) -> None:
        self._authorize(principal, auth.REGISTER_PIPELINE, request_id=request_id,
                        pipeline_id=pipeline_id)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.register_pipeline(pipeline_id, config_generation)
        self._audit(
            request_id=request_id, actor=principal.username,
            action="register_pipeline", pipeline_id=pipeline_id,
            environment="*", release_id="", from_status="",
            to_status="registered", reason=f"generation={state.config_generation}",
        )

    def set_config_generation(
        self, principal: Principal, pipeline_id: str,
        generation: str, request_id: str = "",
    ) -> None:
        """更新流水线当前配置代际（admin），用于驱动回滚兼容性判定。"""
        self._authorize(principal, auth.REGISTER_PIPELINE, request_id=request_id,
                        pipeline_id=pipeline_id)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.require_pipeline(pipeline_id)
            old = state.config_generation
            state.config_generation = generation
        self._audit(
            request_id=request_id, actor=principal.username,
            action="config_generation_changed", pipeline_id=pipeline_id,
            environment="*", release_id="", from_status=old,
            to_status=generation, reason="",
        )

    # -----------------------------------------------------------------
    # 发布
    # -----------------------------------------------------------------
    def publish(
        self,
        principal: Principal,
        pipeline_id: str,
        environment: str,
        artifact: Artifact,
        idempotency_key: str = "",
        request_id: str = "",
    ) -> Release:
        action = auth.PUBLISH
        self._authorize(principal, action, request_id=request_id,
                        pipeline_id=pipeline_id, environment=environment,
                        idempotency_key=idempotency_key)
        rid = ""

        # 非阻塞抢锁：抢不到说明此刻有另一个变更正在同一条流水线上进行。
        lock = self._get_lock(pipeline_id)
        if not lock.acquire(blocking=False):
            blocker = self.store.peek_pending_release_id(pipeline_id)
            err = ServiceError(
                ErrorCode.CONCURRENT_MODIFICATION,
                f"another mutation on pipeline '{pipeline_id}' is in progress",
                conflict_with=blocker,
            )
            self._deny(request_id=request_id, actor=principal.username,
                       action=action, pipeline_id=pipeline_id,
                       environment=environment, err=err,
                       idempotency_key=idempotency_key)
        try:
            state = self.store.require_pipeline(pipeline_id)

            # 幂等重放 / 冲突
            if idempotency_key:
                rec = state.idempotency.get((environment, idempotency_key))
                if rec is not None:
                    existing = state.releases[rec.release_id]
                    fingerprint = self._fingerprint(environment, artifact)
                    if rec.request_fingerprint != fingerprint:
                        err = ServiceError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            f"idempotency key '{idempotency_key}' was already used "
                            f"with a different request body",
                            conflict_with=rec.release_id,
                        )
                        self._deny(request_id=request_id, actor=principal.username,
                                   action=action, pipeline_id=pipeline_id,
                                   environment=environment, err=err,
                                   release_id=rec.release_id,
                                   idempotency_key=idempotency_key)
                    logger.info(
                        "idempotent replay request_id=%s key=%s release=%s",
                        request_id, idempotency_key, existing.release_id,
                    )
                    return existing

            # 在途操作互斥
            in_flight = self.store.pending_release(state)
            if in_flight is not None:
                err = ServiceError(
                    ErrorCode.CONCURRENT_MODIFICATION,
                    f"release '{in_flight.release_id}' on pipeline "
                    f"'{pipeline_id}' (env={in_flight.environment}) is still "
                    f"pending; reject concurrent mutation",
                    conflict_with=in_flight.release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err,
                           release_id=in_flight.release_id,
                           idempotency_key=idempotency_key)

            # 落盘 pending（此时尚未改变 current 指针，失败也无残留中间态）
            now = utc_now()
            release = Release(
                release_id=new_id("rel"),
                pipeline_id=pipeline_id,
                environment=environment,
                artifact=artifact,
                kind="publish",
                status=ReleaseStatus.PENDING,
                config_generation=state.config_generation,
                idempotency_key=idempotency_key,
                requested_by=principal.username,
                created_at=now,
                updated_at=now,
            )
            self.store.add_release(state, release)
            if idempotency_key:
                state.idempotency[(environment, idempotency_key)] = IdempotencyRecord(
                    key=idempotency_key, environment=environment,
                    request_fingerprint=self._fingerprint(environment, artifact),
                    release_id=release.release_id, created_at=now,
                )
            rid = release.release_id
            self._audit(
                request_id=request_id, actor=principal.username, action=action,
                pipeline_id=pipeline_id, environment=environment, release_id=rid,
                from_status="", to_status=ReleaseStatus.PENDING.value,
                reason=f"accepted version={artifact.version}",
                idempotency_key=idempotency_key,
            )

            delay = self.mutation_delay.get(pipeline_id, 0.0)
            if delay:
                time.sleep(delay)

            # 执行发布（钩子可注入失败，模拟部署阶段出错）
            try:
                hook = self.publish_hooks.get(pipeline_id)
                if hook is not None:
                    hook(release)
            except ServiceError as hook_err:
                self._transition(release, ReleaseStatus.FAILED)
                release.failure_reason = f"{hook_err.code}: {hook_err.message}"
                state.pending_by_env.pop(environment, None)
                self._audit(
                    request_id=request_id, actor=principal.username,
                    action=f"{action}.failed", pipeline_id=pipeline_id,
                    environment=environment, release_id=rid,
                    from_status=ReleaseStatus.PENDING.value,
                    to_status=ReleaseStatus.FAILED.value,
                    reason=release.failure_reason,
                    idempotency_key=idempotency_key,
                )
                raise

            # 成功：旧 current -> rolled_back，新 release -> published，
            # 更新 current 指针。同一把锁内连续完成，对外原子。
            previous = self.store.current_release(state, environment)
            self._transition(release, ReleaseStatus.PUBLISHED)
            state.current_by_env[environment] = release.release_id
            state.pending_by_env.pop(environment, None)
            if previous is not None:
                prev_status = previous.status.value
                self._transition(previous, ReleaseStatus.ROLLED_BACK)
                previous.superseded_by = release.release_id
                self._audit(
                    request_id=request_id, actor=principal.username,
                    action="superseded", pipeline_id=pipeline_id,
                    environment=environment, release_id=previous.release_id,
                    from_status=prev_status,
                    to_status=ReleaseStatus.ROLLED_BACK.value,
                    reason=f"superseded by {release.release_id}",
                )
            self._audit(
                request_id=request_id, actor=principal.username,
                action=f"{action}.succeeded", pipeline_id=pipeline_id,
                environment=environment, release_id=rid,
                from_status=ReleaseStatus.PENDING.value,
                to_status=ReleaseStatus.PUBLISHED.value,
                reason=(f"version={artifact.version} "
                        f"generation={state.config_generation}"),
                idempotency_key=idempotency_key,
            )
            return release
        finally:
            lock.release()

    # -----------------------------------------------------------------
    # 回滚
    # -----------------------------------------------------------------
    def rollback(
        self,
        principal: Principal,
        pipeline_id: str,
        environment: str,
        target_version: str,
        idempotency_key: str = "",
        request_id: str = "",
    ) -> Release:
        action = auth.ROLLBACK
        self._authorize(principal, action, request_id=request_id,
                        pipeline_id=pipeline_id, environment=environment,
                        idempotency_key=idempotency_key)

        lock = self._get_lock(pipeline_id)
        if not lock.acquire(blocking=False):
            blocker = self.store.peek_pending_release_id(pipeline_id)
            err = ServiceError(
                ErrorCode.CONCURRENT_MODIFICATION,
                f"another mutation on pipeline '{pipeline_id}' is in progress",
                conflict_with=blocker,
            )
            self._deny(request_id=request_id, actor=principal.username,
                       action=action, pipeline_id=pipeline_id,
                       environment=environment, err=err,
                       idempotency_key=idempotency_key)
        try:
            state = self.store.require_pipeline(pipeline_id)

            # 幂等
            if idempotency_key:
                rec = state.idempotency.get((environment, idempotency_key))
                if rec is not None:
                    fingerprint = self._rollback_fingerprint(environment, target_version)
                    if rec.request_fingerprint != fingerprint:
                        err = ServiceError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            f"idempotency key '{idempotency_key}' was already used "
                            f"with a different request body",
                            conflict_with=rec.release_id,
                        )
                        self._deny(request_id=request_id, actor=principal.username,
                                   action=action, pipeline_id=pipeline_id,
                                   environment=environment, err=err,
                                   release_id=rec.release_id,
                                   idempotency_key=idempotency_key)
                    return state.releases[rec.release_id]

            # 前置校验全部完成前不动任何状态。
            in_flight = self.store.pending_release(state)
            if in_flight is not None:
                err = ServiceError(
                    ErrorCode.CONCURRENT_MODIFICATION,
                    f"release '{in_flight.release_id}' is pending on pipeline "
                    f"'{pipeline_id}'; reject concurrent rollback",
                    conflict_with=in_flight.release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err,
                           release_id=in_flight.release_id,
                           idempotency_key=idempotency_key)

            target = self.store.find_historical_published(
                state, environment, target_version
            )

            current = self.store.current_release(state, environment)
            if current is not None and current.artifact.version == target_version:
                err = ServiceError(
                    ErrorCode.VERSION_ALREADY_ACTIVE,
                    f"version '{target_version}' is already the active version "
                    f"on environment '{environment}'",
                    conflict_with=current.release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err,
                           release_id=current.release_id,
                           idempotency_key=idempotency_key)

            if state.config_generation not in target.artifact.compatible_generations:
                err = ServiceError(
                    ErrorCode.VERSION_INCOMPATIBLE,
                    f"historical version '{target_version}' "
                    f"(generations={list(target.artifact.compatible_generations)}) "
                    f"is incompatible with current generation "
                    f"'{state.config_generation}'",
                    conflict_with=target.release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err,
                           release_id=target.release_id,
                           idempotency_key=idempotency_key)

            # 用当时那次发布冻结的产物与配置快照重建，保证与当时一致。
            restored = Artifact(
                artifact_id=target.artifact.artifact_id,
                version=target.artifact.version,
                checksum=target.artifact.checksum,
                config_snapshot=dict(target.artifact.config_snapshot),
                compatible_generations=target.artifact.compatible_generations,
            )
            now = utc_now()
            rollback_release = Release(
                release_id=new_id("rel"),
                pipeline_id=pipeline_id,
                environment=environment,
                artifact=restored,
                kind="rollback",
                status=ReleaseStatus.PENDING,
                config_generation=state.config_generation,
                idempotency_key=idempotency_key,
                requested_by=principal.username,
                rolled_back_from=target.release_id,
                created_at=now,
                updated_at=now,
            )
            self.store.add_release(state, rollback_release)
            if idempotency_key:
                state.idempotency[(environment, idempotency_key)] = IdempotencyRecord(
                    key=idempotency_key, environment=environment,
                    request_fingerprint=self._rollback_fingerprint(
                        environment, target_version),
                    release_id=rollback_release.release_id, created_at=now,
                )
            self._audit(
                request_id=request_id, actor=principal.username, action=action,
                pipeline_id=pipeline_id, environment=environment,
                release_id=rollback_release.release_id,
                from_status="", to_status=ReleaseStatus.PENDING.value,
                reason=f"accepted rollback to version={target_version} "
                       f"restored_from={target.release_id}",
                idempotency_key=idempotency_key,
            )

            delay = self.mutation_delay.get(pipeline_id, 0.0)
            if delay:
                time.sleep(delay)

            # 提交事务：pending -> published；当前版本 -> rolled_back；指针切换。
            self._transition(rollback_release, ReleaseStatus.PUBLISHED)
            state.current_by_env[environment] = rollback_release.release_id
            state.pending_by_env.pop(environment, None)
            if current is not None:
                prev_status = current.status.value
                self._transition(current, ReleaseStatus.ROLLED_BACK)
                current.superseded_by = rollback_release.release_id
                self._audit(
                    request_id=request_id, actor=principal.username,
                    action="superseded", pipeline_id=pipeline_id,
                    environment=environment, release_id=current.release_id,
                    from_status=prev_status,
                    to_status=ReleaseStatus.ROLLED_BACK.value,
                    reason=f"superseded by rollback {rollback_release.release_id}",
                )
            self._audit(
                request_id=request_id, actor=principal.username,
                action=f"{action}.succeeded", pipeline_id=pipeline_id,
                environment=environment, release_id=rollback_release.release_id,
                from_status=ReleaseStatus.PENDING.value,
                to_status=ReleaseStatus.PUBLISHED.value,
                reason=(f"restored version={target_version} "
                        f"artifact_checksum={restored.checksum} "
                        f"generation={state.config_generation}"),
                idempotency_key=idempotency_key,
            )
            return rollback_release
        finally:
            lock.release()

    # -----------------------------------------------------------------
    # 取消 / 失败上报
    # -----------------------------------------------------------------
    def cancel(
        self, principal: Principal, pipeline_id: str, environment: str,
        release_id: str, request_id: str = "",
    ) -> Release:
        action = auth.CANCEL
        self._authorize(principal, action, request_id=request_id,
                        pipeline_id=pipeline_id, environment=environment)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.require_pipeline(pipeline_id)
            release = self.store.get_release(state, release_id)
            if release.status is not ReleaseStatus.PENDING:
                err = ServiceError(
                    ErrorCode.RELEASE_NOT_CANCELLABLE,
                    f"release '{release_id}' is {release.status.value}, "
                    f"only pending releases can be cancelled",
                    conflict_with=release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err, release_id=release_id)
            prev = release.status.value
            self._transition(release, ReleaseStatus.CANCELLED)
            state.pending_by_env.pop(release.environment, None)
            self._audit(
                request_id=request_id, actor=principal.username,
                action=f"{action}.succeeded", pipeline_id=pipeline_id,
                environment=environment, release_id=release_id,
                from_status=prev, to_status=ReleaseStatus.CANCELLED.value,
                reason="cancelled by operator",
            )
            return release

    def mark_failed(
        self, principal: Principal, pipeline_id: str, environment: str,
        release_id: str, reason: str, request_id: str = "",
    ) -> Release:
        action = auth.REPORT_FAILURE
        self._authorize(principal, action, request_id=request_id,
                        pipeline_id=pipeline_id, environment=environment)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.require_pipeline(pipeline_id)
            release = self.store.get_release(state, release_id)
            if release.status is not ReleaseStatus.PENDING:
                err = ServiceError(
                    ErrorCode.INVALID_STATUS_TRANSITION,
                    f"release '{release_id}' is {release.status.value}, "
                    f"failure can only be reported for pending releases",
                    conflict_with=release_id,
                )
                self._deny(request_id=request_id, actor=principal.username,
                           action=action, pipeline_id=pipeline_id,
                           environment=environment, err=err, release_id=release_id)
            prev = release.status.value
            self._transition(release, ReleaseStatus.FAILED)
            release.failure_reason = reason
            state.pending_by_env.pop(release.environment, None)
            self._audit(
                request_id=request_id, actor=principal.username,
                action=f"{action}.recorded", pipeline_id=pipeline_id,
                environment=environment, release_id=release_id,
                from_status=prev, to_status=ReleaseStatus.FAILED.value,
                reason=reason,
            )
            return release

    # -----------------------------------------------------------------
    # 查询
    # -----------------------------------------------------------------
    def get_current(
        self, principal: Principal, pipeline_id: str, environment: str
    ) -> Release:
        auth.require_permission(principal, auth.READ)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.require_pipeline(pipeline_id)
            release = self.store.current_release(state, environment)
            if release is None:
                raise ServiceError(
                    ErrorCode.NOT_FOUND,
                    f"no published release on pipeline '{pipeline_id}' "
                    f"environment '{environment}'",
                )
            return release

    def history(
        self, principal: Principal, pipeline_id: str
    ) -> list[Release]:
        auth.require_permission(principal, auth.READ)
        with self.store.lock_pipeline(pipeline_id):
            state = self.store.require_pipeline(pipeline_id)
            return self.store.list_history(state)

    def audits(self, principal: Principal, pipeline_id: str | None = None) -> list:
        auth.require_permission(principal, auth.READ)
        return self.store.list_audits(pipeline_id)

    # -----------------------------------------------------------------
    def _get_lock(self, pipeline_id: str) -> threading.RLock:
        # 与 Store 内部同一把锁（通过注册时创建的锁表访问）。
        with self.store._locks_guard:  # noqa: SLF001
            return self.store._pipeline_locks.setdefault(  # noqa: SLF001
                pipeline_id, threading.RLock())
