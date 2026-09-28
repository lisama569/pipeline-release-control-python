"""枚举与数据模型。

状态流转见 README，核心实体：

- ``Artifact``：一次构建的产物（版本号、校验和、配置快照、兼容的配置代际）。
- ``Release``：发布记录，产物在某条流水线某个环境上的一次状态机实例。
- ``AuditEntry``：状态迁移审计，与 Release 的每次状态变化一一对应。
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class Environment(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class Role(str, Enum):
    """调用方角色，权限模型见 auth.py。"""

    VIEWER = "viewer"      # 只读
    OPERATOR = "operator"  # 发布 / 回滚 / 取消 / 上报失败
    ADMIN = "admin"        # 全部操作，含流水线注册


class ReleaseStatus(str, Enum):
    PENDING = "pending"        # 已受理，发布进行中
    PUBLISHED = "published"    # 发布成功，当前生效
    FAILED = "failed"          # 发布失败（终态，未生效）
    CANCELLED = "cancelled"    # 被取消（仅可从 pending）
    ROLLED_BACK = "rolled_back"  # 已被回滚替换（曾经生效的发布）


# 合法的状态迁移表：键为当前状态，值为允许迁移到的状态。
# pending -> published / failed / cancelled
# published -> rolled_back
# failed / cancelled / rolled_back 均为终态
ALLOWED_TRANSITIONS: dict[ReleaseStatus, frozenset[ReleaseStatus]] = {
    ReleaseStatus.PENDING: frozenset({
        ReleaseStatus.PUBLISHED,
        ReleaseStatus.FAILED,
        ReleaseStatus.CANCELLED,
    }),
    ReleaseStatus.PUBLISHED: frozenset({
        ReleaseStatus.ROLLED_BACK,
    }),
    ReleaseStatus.FAILED: frozenset(),
    ReleaseStatus.CANCELLED: frozenset(),
    ReleaseStatus.ROLLED_BACK: frozenset(),
}


@dataclass(frozen=True)
class Artifact:
    """构建产物。

    compatible_generations 声明该产物兼容哪些流水线“配置代际”；
    回滚时据此判定历史版本与当前配置是否兼容。
    """

    artifact_id: str
    version: str
    checksum: str
    config_snapshot: dict[str, Any] = field(default_factory=dict)
    compatible_generations: tuple[str, ...] = ()


@dataclass
class Release:
    release_id: str
    pipeline_id: str
    environment: str
    artifact: Artifact
    kind: str = "publish"  # publish | rollback
    status: ReleaseStatus = ReleaseStatus.PENDING
    config_generation: str = ""
    idempotency_key: str = ""
    requested_by: str = ""
    failure_reason: str = ""
    rolled_back_from: str = ""  # rollback 类型记录被恢复的历史 release_id
    superseded_by: str = ""     # 被哪次 rollback 替换
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data


@dataclass
class AuditEntry:
    """一次状态迁移的审计记录。"""

    audit_id: str
    request_id: str
    timestamp: str
    actor: str
    action: str
    pipeline_id: str
    environment: str
    release_id: str
    from_status: str
    to_status: str
    reason: str = ""        # 判定依据 / 失败原因 / 冲突原因
    idempotency_key: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
