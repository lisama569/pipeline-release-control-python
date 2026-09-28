"""线程安全的内存存储。

并发模型：

- 每条 ``pipeline_id`` 一把可重入排他锁，任何对该流水线的状态变更
  （发布 / 回滚 / 取消 / 上报失败）都必须持锁完成；因此同一时刻只有
  一个变更请求能进入临界区，其余请求要么被状态检查挡下
  （``CONCURRENT_MODIFICATION``），要么在锁外等待后看到最新状态。
- 幂等记录按 (环境, 幂等键) 存于流水线状态上。
- 审计表全局有序，只追加。

生产环境应替换为数据库 + 行锁/乐观版本号，接口语义保持不变。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator

from .errors import ErrorCode, ServiceError
from .models import AuditEntry, Release, ReleaseStatus


@dataclass
class IdempotencyRecord:
    key: str
    environment: str
    request_fingerprint: str
    release_id: str
    created_at: str = ""


@dataclass
class PipelineState:
    pipeline_id: str
    config_generation: str
    releases: dict[str, Release] = field(default_factory=dict)
    history: list[str] = field(default_factory=list)          # release_id，按创建顺序
    current_by_env: dict[str, str] = field(default_factory=dict)
    pending_by_env: dict[str, str] = field(default_factory=dict)
    idempotency: dict[tuple[str, str], IdempotencyRecord] = field(default_factory=dict)


class Store:
    def __init__(self) -> None:
        self._locks_guard = threading.Lock()
        self._pipeline_locks: dict[str, threading.RLock] = {}
        self._pipelines: dict[str, PipelineState] = {}
        self._audit_lock = threading.Lock()
        self.audits: list[AuditEntry] = []

    # ---- 锁 ----------------------------------------------------------
    @contextmanager
    def lock_pipeline(self, pipeline_id: str) -> Iterator[None]:
        with self._locks_guard:
            lock = self._pipeline_locks.setdefault(pipeline_id, threading.RLock())
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    # ---- 流水线注册 --------------------------------------------------
    def register_pipeline(self, pipeline_id: str, config_generation: str) -> PipelineState:
        with self._locks_guard:
            state = self._pipelines.get(pipeline_id)
            if state is None:
                state = PipelineState(pipeline_id=pipeline_id,
                                      config_generation=config_generation)
                self._pipelines[pipeline_id] = state
            return state

    def set_config_generation(self, pipeline_id: str, generation: str) -> None:
        state = self.require_pipeline(pipeline_id)
        state.config_generation = generation

    def get_pipeline(self, pipeline_id: str) -> PipelineState | None:
        return self._pipelines.get(pipeline_id)

    def require_pipeline(self, pipeline_id: str) -> PipelineState:
        state = self._pipelines.get(pipeline_id)
        if state is None:
            raise ServiceError(
                ErrorCode.NOT_FOUND,
                f"pipeline '{pipeline_id}' is not registered",
            )
        return state

    # ---- release -----------------------------------------------------
    def add_release(self, state: PipelineState, release: Release) -> None:
        state.releases[release.release_id] = release
        state.history.append(release.release_id)
        if release.status is ReleaseStatus.PENDING:
            state.pending_by_env[release.environment] = release.release_id

    def get_release(self, state: PipelineState, release_id: str) -> Release:
        release = state.releases.get(release_id)
        if release is None:
            raise ServiceError(
                ErrorCode.RELEASE_NOT_FOUND,
                f"release '{release_id}' not found on pipeline "
                f"'{state.pipeline_id}'",
            )
        return release

    def pending_release(self, state: PipelineState) -> Release | None:
        """任一环境上处于 pending 的发布（整条流水线维度的在途操作）。"""
        for env_name, rid in list(state.pending_by_env.items()):
            release = state.releases.get(rid)
            if release is not None and release.status is ReleaseStatus.PENDING:
                return release
            state.pending_by_env.pop(env_name, None)
        return None

    def peek_pending_release_id(self, pipeline_id: str) -> str:
        """抢锁失败时的诊断信息：尽力返回在途 release_id，不加锁。"""
        state = self._pipelines.get(pipeline_id)
        if state is None:
            return ""
        for rid in list(state.pending_by_env.values()):
            release = state.releases.get(rid)
            if release is not None and release.status is ReleaseStatus.PENDING:
                return rid
        return ""

    def current_release(self, state: PipelineState, environment: str) -> Release | None:
        rid = state.current_by_env.get(environment)
        if rid is None:
            return None
        release = state.releases.get(rid)
        # 指针只可能指向 published；失败/取消的发布从未更新过指针，
        # 但防御性地在这里再校验一次，避免把非生效记录当作当前版本。
        if release is None or release.status is not ReleaseStatus.PUBLISHED:
            return None
        return release

    def list_history(self, state: PipelineState) -> list[Release]:
        return [state.releases[rid] for rid in state.history if rid in state.releases]

    def find_historical_published(
        self, state: PipelineState, environment: str, version: str
    ) -> Release:
        """在某环境历史中查找指定版本、且曾经成功发布的最近一次记录。

        找不到版本          -> VERSION_NOT_FOUND
        版本只提交失败/取消 -> VERSION_NOT_PUBLISHED
        """
        matched: Release | None = None
        for rid in reversed(state.history):
            release = state.releases[rid]
            if (release.environment == environment
                    and release.artifact.version == version):
                matched = release
                break
        if matched is None:
            raise ServiceError(
                ErrorCode.VERSION_NOT_FOUND,
                f"version '{version}' was never submitted to environment "
                f"'{environment}' on pipeline '{state.pipeline_id}'",
            )
        if matched.status not in (ReleaseStatus.PUBLISHED, ReleaseStatus.ROLLED_BACK):
            raise ServiceError(
                ErrorCode.VERSION_NOT_PUBLISHED,
                f"version '{version}' exists on pipeline "
                f"'{state.pipeline_id}' but never reached published "
                f"(last status: {matched.status.value})",
                conflict_with=matched.release_id,
            )
        return matched

    # ---- 审计 --------------------------------------------------------
    def append_audit(self, entry: AuditEntry) -> None:
        with self._audit_lock:
            self.audits.append(entry)

    def list_audits(self, pipeline_id: str | None = None) -> list[AuditEntry]:
        with self._audit_lock:
            if pipeline_id is None:
                return list(self.audits)
            return [a for a in self.audits if a.pipeline_id == pipeline_id]
