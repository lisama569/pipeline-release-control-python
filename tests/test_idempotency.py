"""重复提交（重试 / 超时重发）不得产生重复发布记录。"""
from __future__ import annotations

from app.errors import ErrorCode
from app.models import ReleaseStatus
from tests.conftest import artifact_json, headers, make_artifact


def publish(client, version, key, request_id, **kw):
    return client.post(
        "/pipelines/p1/environments/dev/releases",
        json={"artifact": artifact_json(version, **kw)},
        headers=headers("operator", "ops", request_id, key),
    )


def test_same_key_same_body_returns_same_release(client, registered):
    r1 = publish(client, "1.0.0", "dup-1", "req-a")
    assert r1.status_code == 200
    r2 = publish(client, "1.0.0", "dup-1", "req-b")  # 超时重发
    r3 = publish(client, "1.0.0", "dup-1", "req-c")  # 再次重试
    assert r2.status_code == 200 and r3.status_code == 200
    ids = {r1.json()["release_id"], r2.json()["release_id"], r3.json()["release_id"]}
    assert ids == {r1.json()["release_id"]}

    history = client.get("/pipelines/p1/releases",
                         headers=headers("viewer", "v")).json()["releases"]
    assert len(history) == 1
    assert history[0]["idempotency_key"] == "dup-1"


def test_same_key_different_body_is_rejected_with_distinguishable_code(
        client, registered):
    r1 = publish(client, "1.0.0", "dup-2", "req-a")
    assert r1.status_code == 200
    # 同键但请求体不同（不同 checksum）
    body = {"artifact": artifact_json("1.0.0")}
    body["artifact"]["checksum"] = "sha-TAMPERED"
    r2 = client.post("/pipelines/p1/environments/dev/releases", json=body,
                     headers=headers("operator", "ops", "req-b", "dup-2"))
    assert r2.status_code == 409
    assert r2.json()["code"] == ErrorCode.IDEMPOTENCY_CONFLICT
    assert r2.json()["conflict_with"] == r1.json()["release_id"]

    history = client.get("/pipelines/p1/releases",
                         headers=headers("viewer", "v")).json()["releases"]
    assert len(history) == 1  # 冲突请求未产生任何记录


def test_replay_after_failed_publish_returns_same_failed_record(
        service, registered, operator, caplog_audit):
    from app.errors import ServiceError

    def fail_hook(release):
        raise ServiceError(ErrorCode.RELEASE_FAILED, "deploy step blew up")

    service.publish_hooks["p1"] = fail_hook
    art = make_artifact("1.0.0")
    try:
        service.publish(operator, "p1", "dev", art, idempotency_key="fk1",
                        request_id="req-f1")
        assert False, "should fail"
    except Exception as e:  # noqa: BLE001
        assert getattr(e, "code", "") == ErrorCode.RELEASE_FAILED

    # 同一幂等键重试：重放原失败记录，不会再走一遍 hook，也不产生新记录
    replay = service.publish(operator, "p1", "dev", art, idempotency_key="fk1",
                             request_id="req-f2")
    assert replay.status is ReleaseStatus.FAILED
    assert replay.failure_reason
    releases = service.history(operator, "p1")
    assert len(releases) == 1
    assert "idempotent replay" in caplog_audit.text


def test_idempotency_key_is_scoped_per_environment(service, registered, operator):
    art = make_artifact("1.0.0")
    r_dev = service.publish(operator, "p1", "dev", art, idempotency_key="shared",
                            request_id="req-dev")
    r_stg = service.publish(operator, "p1", "staging", art, idempotency_key="shared",
                            request_id="req-stg")
    assert r_dev.release_id != r_stg.release_id
    assert r_dev.environment == "dev" and r_stg.environment == "staging"


def test_rollback_idempotency(service, registered, operator):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-1")
    service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                    idempotency_key="p2", request_id="req-2")
    rb1 = service.rollback(operator, "p1", "dev", "1.0.0",
                           idempotency_key="rb1", request_id="req-3")
    rb2 = service.rollback(operator, "p1", "dev", "1.0.0",
                           idempotency_key="rb1", request_id="req-4")
    assert rb1.release_id == rb2.release_id
    releases = service.history(operator, "p1")
    # 1.0.0, 2.0.0, rollback(1.0.0) 共三条
    assert len(releases) == 3
