"""HTTP 层：错误码可区分、请求标识贯穿响应头/日志、审计可查。"""
from __future__ import annotations

from tests.conftest import artifact_json, headers


def _register(client):
    r = client.post("/pipelines/p1/register",
                    json={"config_generation": "gen-1"},
                    headers=headers("admin", "root", "req-setup"))
    assert r.status_code == 200


def _publish(client, version, key, request_id, role="operator"):
    return client.post(
        "/pipelines/p1/environments/dev/releases",
        json={"artifact": artifact_json(version)},
        headers=headers(role, "ops", request_id, key),
    )


def test_request_id_generated_and_echoed(client, registered, caplog):
    import logging
    caplog.set_level(logging.INFO, logger="release_control")
    r = _publish(client, "1.0.0", "k1", None)
    assert r.status_code == 200
    rid = r.headers["x-request-id"]
    assert rid.startswith("req-")
    assert rid in caplog.text
    assert "request_start" in caplog.text and "request_end" in caplog.text


def test_client_supplied_request_id_is_preserved(client, registered):
    r = _publish(client, "1.0.0", "k1", "my-trace-id")
    assert r.headers["x-request-id"] == "my-trace-id"


def test_error_codes_are_distinguishable_over_http(client, registered):
    _publish(client, "1.0.0", "k1", "req-1")
    _publish(client, "2.0.0", "k2", "req-2")

    # 幂等冲突
    conflict = _publish(client, "2.0.0", "k1", "req-3")
    body_conflict = dict(artifact_json("2.0.0"))
    # 同键不同体
    r1 = client.post(
        "/pipelines/p1/environments/dev/releases",
        json={"artifact": {**artifact_json("2.0.0"), "checksum": "other"}},
        headers=headers("operator", "ops", "req-idem", "k1"),
    )
    assert r1.status_code == 409
    assert r1.json()["code"] == "IDEMPOTENCY_CONFLICT"

    # 回滚不存在
    r2 = client.post("/pipelines/p1/environments/dev/rollbacks",
                     json={"target_version": "9.9.9"},
                     headers=headers("operator", "ops", "req-rb"))
    assert r2.status_code == 404
    assert r2.json()["code"] == "VERSION_NOT_FOUND"

    # 越权
    r3 = _publish(client, "3.0.0", "k3", "req-403", role="viewer")
    assert r3.status_code == 403
    assert r3.json()["code"] == "PERMISSION_DENIED"

    # 非法状态：取消已发布的记录
    rid = _publish(client, "4.0.0", "k4", "req-4").json()["release_id"]
    r4 = client.post(f"/pipelines/p1/releases/{rid}/cancel",
                     headers=headers("operator", "ops", "req-cancel"))
    assert r4.status_code == 409
    assert r4.json()["code"] == "RELEASE_NOT_CANCELLABLE"

    # 四种失败原因两两不同
    codes = {r1.json()["code"], r2.json()["code"], r3.json()["code"],
             r4.json()["code"]}
    assert codes == {"IDEMPOTENCY_CONFLICT", "VERSION_NOT_FOUND",
                     "PERMISSION_DENIED", "RELEASE_NOT_CANCELLABLE"}


def test_rollback_http_flow_and_audit_log(client, registered, caplog):
    import logging
    caplog.set_level(logging.INFO, logger="release_control")
    _publish(client, "1.0.0", "k1", "req-a")
    _publish(client, "2.0.0", "k2", "req-b")
    r = client.post("/pipelines/p1/environments/dev/rollbacks",
                    json={"target_version": "1.0.0"},
                    headers=headers("operator", "ops", "req-c", "krb"))
    assert r.status_code == 200
    assert r.json()["kind"] == "rollback"

    audits = client.get("/pipelines/p1/audits",
                        headers=headers("viewer", "v")).json()["audits"]
    rollback_audits = [a for a in audits if a["request_id"] == "req-c"]
    assert any(a["to_status"] == "pending" for a in rollback_audits)
    assert any(a["to_status"] == "published" for a in rollback_audits)
    assert any("restored version=1.0.0" in a["reason"] for a in rollback_audits)
    assert "transition=pending->published" in caplog.text


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200 and r.json() == {"status": "ok"}
