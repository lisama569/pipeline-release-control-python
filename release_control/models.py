"""领域模型：环境、状态、角色与核心数据结构。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class Environment(str, enum.Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class ReleaseStatus(str, enum.Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLED_BACK = "rolled_back"


class Role(str, enum.Enum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


@dataclass(frozen=True)
class Caller:
    caller_id: str
    role: Role


@dataclass(frozen=True)
class Build:
    build_id: str
    pipeline_id: str
    artifact_uri: str
    config: dict
    schema_version: int = 1


@dataclass
class ReleaseRecord:
    release_id: str
    pipeline_id: str
    environment: Environment
    build: Build
    status: ReleaseStatus
    idempotency_key: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    history: list = field(default_factory=list)


@dataclass(frozen=True)
class AuditEvent:
    request_id: str
    release_id: str
    from_status: str
    to_status: str
    reason: str
    actor: str
    at: datetime
