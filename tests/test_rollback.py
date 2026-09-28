"""回滚：历史版本校验、产物/配置一致性、兼容性拒绝与原子性。"""
from __future__ import annotations

import pytest

from app.errors import ErrorCode, ServiceError
from app.models import ReleaseStatus
from tests.conftest import make_artifact


def _publish_chain(service, operator):
    r1 = service.publish(operator, "p1", "dev", make_artifact(
        "1.0.0", snapshot={"cfg": "v1"}), idempotency_key="p1", request_id="req-1")
    r2 = service.publish(operator, "p1", "dev", make_artifact(
        "2.0.0", snapshot={"cfg": "v2"}), idempotency_key="p2", request_id="req-2")
    return r1, r2


def test_rollback_to_unknown_version_is_rejected(client, registered):
    client.post("/pipelines/p1/environments/dev/releases",
                json={"artifact": {
                    "artifact_id": "a1", "version": "1.0.0", "checksum": "sha1",
                    "config_snapshot": {}, "compatible_generations": ["gen-1"]}},
                headers={"X-Role": "operator", "X-Username": "ops",
                         "X-Request-Id": "req-1", "Idempotency-Key": "k1"})
    r = client.post("/pipelines/p1/environments/dev/rollbacks",
                    json={"target_version": "4.0.4"},
                    headers={"X-Role": "operator", "X-Username": "ops",
                             "X-Request-Id": "req-2"})
    assert r.status_code == 404
    assert r.json()["code"] == ErrorCode.VERSION_NOT_FOUND
    # 当前版本不受影响
    cur = client.get("/pipelines/p1/environments/dev/current",
                     headers={"X-Role": "viewer", "X-Username": "v"})
    assert cur.json()["artifact"]["version"] == "1.0.0"


def test_rollback_to_version_that_never_published(service, registered, operator):
    # 一次失败发布：版本存在，但从未成功
    service.publish_hooks["p1"] = lambda rel: (_ for _ in ()).throw(
        ServiceError(ErrorCode.RELEASE_FAILED, "boom"))
    try:
        service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                        idempotency_key="p1", request_id="req-1")
    except ServiceError:
        pass
    del service.publish_hooks["p1"]
    service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                    idempotency_key="p2", request_id="req-2")
    with pytest.raises(ServiceError) as ei:
        service.rollback(operator, "p1", "dev", "1.0.0", request_id="req-3")
    assert ei.value.code == ErrorCode.VERSION_NOT_PUBLISHED
    # 仍是 2.0.0 生效
    assert service.get_current(operator, "p1", "dev").artifact.version == "2.0.0"


def test_rollback_restores_exact_artifact_and_config(service, registered, operator):
    r1, r2 = _publish_chain(service, operator)
    rb = service.rollback(operator, "p1", "dev", "1.0.0",
                          idempotency_key="rb1", request_id="req-3")
    assert rb.kind == "rollback"
    assert rb.rolled_back_from == r1.release_id
    # 产物与配置必须与当时那次发布一致
    assert rb.artifact.artifact_id == r1.artifact.artifact_id
    assert rb.artifact.checksum == r1.artifact.checksum
    assert rb.artifact.config_snapshot == {"cfg": "v1"}
    # 修改回滚记录的快照不影响原始历史记录（拷贝而非引用）
    rb.artifact.config_snapshot["cfg"] = "MUTATED"
    fresh = [r for r in service.history(operator, "p1")
             if r.release_id == r1.release_id][0]
    assert fresh.artifact.config_snapshot == {"cfg": "v1"}

    current = service.get_current(operator, "p1", "dev")
    assert current.release_id == rb.release_id
    # 2.0.0 被标记为 rolled_back 且记录了被谁替换
    assert r2.status is ReleaseStatus.ROLLED_BACK
    assert r2.superseded_by == rb.release_id


def test_rollback_to_current_version_rejected(service, registered, operator):
    _publish_chain(service, operator)
    with pytest.raises(ServiceError) as ei:
        service.rollback(operator, "p1", "dev", "2.0.0", request_id="req-3")
    assert ei.value.code == ErrorCode.VERSION_ALREADY_ACTIVE
    assert ei.value.conflict_with  # 指向当前生效记录


def test_rollback_incompatible_generation_rejected_without_partial_state(
        service, registered, operator, admin):
    r1, r2 = _publish_chain(service, operator)
    service.set_config_generation(admin, "p1", "gen-2", request_id="req-gen")
    before = [r.to_dict() for r in service.history(operator, "p1")]
    with pytest.raises(ServiceError) as ei:
        service.rollback(operator, "p1", "dev", "1.0.0", request_id="req-rb")
    assert ei.value.code == ErrorCode.VERSION_INCOMPATIBLE
    assert ei.value.conflict_with == r1.release_id
    # 无新增记录，当前指针仍是 2.0.0，无 pending 残留
    after = [r.to_dict() for r in service.history(operator, "p1")]
    assert before == after
    current = service.get_current(operator, "p1", "dev")
    assert current.artifact.version == "2.0.0"
    assert current.status is ReleaseStatus.PUBLISHED


def test_rollback_succeeds_when_generation_explicitly_compatible(
        service, registered, operator, admin):
    service.publish(operator, "p1", "dev",
                    make_artifact("1.0.0", generations=("gen-1", "gen-2")),
                    idempotency_key="p1", request_id="req-1")
    service.publish(operator, "p1", "dev",
                    make_artifact("2.0.0", generations=("gen-2",)),
                    idempotency_key="p2", request_id="req-2")
    service.set_config_generation(admin, "p1", "gen-2", request_id="req-gen")
    rb = service.rollback(operator, "p1", "dev", "1.0.0", request_id="req-rb")
    assert rb.status is ReleaseStatus.PUBLISHED


def test_rollback_to_other_environment_version_not_found(service, registered, operator):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-1")
    with pytest.raises(ServiceError) as ei:
        service.rollback(operator, "p1", "prod", "1.0.0", request_id="req-2")
    assert ei.value.code == ErrorCode.VERSION_NOT_FOUND


def test_rollback_chain_is_reproducible(service, registered, operator):
    # 1 -> 2 -> 回滚1 -> 3 -> 回滚1，结果始终与 1.0.0 那次一致
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="r1")
    service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                    idempotency_key="p2", request_id="r2")
    service.rollback(operator, "p1", "dev", "1.0.0",
                     idempotency_key="rb1", request_id="r3")
    service.publish(operator, "p1", "dev", make_artifact("3.0.0"),
                    idempotency_key="p3", request_id="r4")
    rb = service.rollback(operator, "p1", "dev", "1.0.0",
                          idempotency_key="rb2", request_id="r5")
    assert rb.artifact.checksum == "sha-1.0.0"
    assert service.get_current(operator, "p1", "dev").artifact.version == "1.0.0"
    # 审计中每次发布都有 accepted + succeeded，状态对得上
    audits = service.audits(operator, "p1")
    transitions = [(a.from_status, a.to_status) for a in audits
                   if a.to_status in {"published", "rolled_back"}]
    assert transitions.count(("pending", "published")) == 5
    assert transitions.count(("published", "rolled_back")) == 4
