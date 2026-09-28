"""API 层测试：错误码到 HTTP 状态的映射与端到端发布/回滚。"""
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from release_control.api import STATUS_BY_CODE, create_router
from release_control.errors import ReleaseError
from release_control.service import ReleaseService
from release_control.store import InMemoryStore


@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(create_router(ReleaseService(InMemoryStore())))

    @app.exception_handler(ReleaseError)
    async def handler(request: Request, exc: ReleaseError):
        return JSONResponse(
            status_code=STATUS_BY_CODE[exc.code],
            content={"error": {"code": exc.code.value, "message": exc.message}},
        )

    return TestClient(app)


def headers(role="operator", caller="op-1", key="k1", req="req-1"):
    return {
        "x-caller-id": caller,
        "x-role": role,
        "idempotency-key": key,
        "x-request-id": req,
    }


def publish(client, build_id="b1", key="k1", role="operator", env="prod"):
    return client.post(
        "/pipelines/p1/releases",
        json={
            "environment": env,
            "build": {"build_id": build_id, "artifact_uri": f"s3://a/{build_id}", "config": {"v": 1}},
        },
        headers=headers(role=role, key=key),
    )


def test_publish_and_get(client):
    resp = publish(client)
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "published"
    got = client.get(f"/releases/{body['release_id']}", headers=headers())
    assert got.status_code == 200
    assert got.json()["release_id"] == body["release_id"]


def test_idempotent_replay_over_http(client):
    first = publish(client, key="k-dup").json()
    second = publish(client, key="k-dup").json()
    assert first["release_id"] == second["release_id"]


def test_permission_denied_maps_to_403(client):
    resp = publish(client, role="viewer")
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_rollback_missing_version_maps_to_404(client):
    publish(client)
    resp = client.post(
        "/pipelines/p1/rollbacks",
        json={"environment": "prod", "target_release_id": "nope"},
        headers=headers(role="admin", key="k-rb"),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "version_not_found"


def test_rollback_as_operator_denied(client):
    target = publish(client).json()
    resp = client.post(
        "/pipelines/p1/rollbacks",
        json={"environment": "prod", "target_release_id": target["release_id"]},
        headers=headers(role="operator", key="k-rb"),
    )
    assert resp.status_code == 403


def test_rollback_over_http_restores_build(client):
    first = publish(client, build_id="b1", key="k1").json()
    publish(client, build_id="b2", key="k2")
    resp = client.post(
        "/pipelines/p1/rollbacks",
        json={"environment": "prod", "target_release_id": first["release_id"]},
        headers=headers(role="admin", key="k-rb"),
    )
    assert resp.status_code == 201
    assert resp.json()["build_id"] == "b1"
    assert resp.json()["config"] == first["config"]
