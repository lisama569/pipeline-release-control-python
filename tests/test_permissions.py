"""不同角色的权限隔离：越权操作直接拒绝（403 + PERMISSION_DENIED）。"""
from __future__ import annotations

from tests.conftest import artifact_json, headers, make_artifact


def test_viewer_cannot_publish(client, registered):
    r = client.post(
        "/pipelines/p1/environments/dev/releases",
        json={"artifact": artifact_json("1.0.0")},
        headers=headers("viewer", "v", "req-1", "k1"),
    )
    assert r.status_code == 403
    assert r.json()["code"] == "PERMISSION_DENIED"


def test_viewer_can_read(client, registered):
    client.post("/pipelines/p1/environments/dev/releases",
                json={"artifact": artifact_json("1.0.0")},
                headers=headers("operator", "ops", "req-1", "k1"))
    r = client.get("/pipelines/p1/releases", headers=headers("viewer", "v"))
    assert r.status_code == 200
    assert len(r.json()["releases"]) == 1


def test_operator_cannot_register_pipeline(client, registered):
    r = client.post("/pipelines/p1/config-generation",
                    json={"generation": "gen-2"},
                    headers=headers("operator", "ops", "req-2"))
    assert r.status_code == 403
    assert r.json()["code"] == "PERMISSION_DENIED"


def test_admin_can_register_new_pipeline(client):
    r = client.post("/pipelines/p2/register", json={"config_generation": "g"},
                    headers=headers("admin", "root", "req-3"))
    assert r.status_code == 200


def test_unknown_role_is_rejected(client, registered):
    r = client.post(
        "/pipelines/p1/environments/dev/releases",
        json={"artifact": artifact_json("1.0.0")},
        headers={"X-Role": "superuser", "X-Username": "x", "X-Request-Id": "req-4"},
    )
    assert r.status_code == 403
    assert r.json()["code"] == "PERMISSION_DENIED"


def test_viewer_cannot_rollback(service, registered, operator, viewer):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-1")
    service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                    idempotency_key="p2", request_id="req-2")
    try:
        service.rollback(viewer, "p1", "dev", "1.0.0", request_id="req-3")
        assert False, "viewer rollback must be denied"
    except Exception as exc:  # noqa: BLE001
        assert exc.code == "PERMISSION_DENIED"


def test_permission_check_happens_before_resource_lookup(client):
    # 未注册流水线 + viewer 写操作：返回 403 而不是 404，证明先鉴权
    r = client.post(
        "/pipelines/ghost/environments/dev/releases",
        json={"artifact": artifact_json("1.0.0")},
        headers=headers("viewer", "v", "req-5"),
    )
    assert r.status_code == 403


def test_denied_action_is_audited_with_actor_and_reason(
        service, registered, viewer, caplog_audit):
    try:
        service.publish(viewer, "p1", "dev", make_artifact("1.0.0"),
                        request_id="req-deny")
    except Exception:  # noqa: BLE001
        pass
    audits = service.audits(viewer, "p1")
    denied = [a for a in audits if a.action == "publish.denied"]
    assert len(denied) == 1
    entry = denied[0]
    assert entry.actor == "viewer"
    assert entry.request_id == "req-deny"
    assert "PERMISSION_DENIED" in entry.reason
    assert "req-deny" in caplog_audit.text
