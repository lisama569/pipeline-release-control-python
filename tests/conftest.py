"""pytest 公共夹具：独立服务实例 + TestClient，并在日志中打印
请求标识、状态迁移与判定依据（caplog 断言用）。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.auth import Principal
from app.models import Artifact, Role
from app.service import ReleaseService


def make_artifact(version: str, *, generations=("gen-1",), snapshot=None,
                  checksum: str | None = None) -> Artifact:
    return Artifact(
        artifact_id=f"art-{version}",
        version=version,
        checksum=checksum or f"sha-{version}",
        config_snapshot=snapshot if snapshot is not None else {"version": version},
        compatible_generations=tuple(generations),
    )


@pytest.fixture
def service() -> ReleaseService:
    return ReleaseService()


@pytest.fixture
def client(service: ReleaseService) -> TestClient:
    return TestClient(create_app(service))


@pytest.fixture
def admin() -> Principal:
    return Principal("root", Role.ADMIN)


@pytest.fixture
def operator() -> Principal:
    return Principal("ops", Role.OPERATOR)


@pytest.fixture
def viewer() -> Principal:
    return Principal("viewer", Role.VIEWER)


@pytest.fixture
def registered(service: ReleaseService, admin: Principal) -> str:
    service.register_pipeline(admin, "p1", "gen-1", request_id="req-setup")
    return "p1"


@pytest.fixture
def caplog_audit(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger="release_control")
    return caplog


def headers(role: str = "operator", user: str = "ops",
            request_id: str | None = None, idempotency_key: str | None = None) -> dict:
    h = {"X-Role": role, "X-Username": user}
    if request_id:
        h["X-Request-Id"] = request_id
    if idempotency_key:
        h["Idempotency-Key"] = idempotency_key
    return h


def artifact_json(version: str, *, generations=("gen-1",), snapshot=None) -> dict:
    return {
        "artifact_id": f"art-{version}",
        "version": version,
        "checksum": f"sha-{version}",
        "config_snapshot": snapshot if snapshot is not None else {"version": version},
        "compatible_generations": list(generations),
    }
