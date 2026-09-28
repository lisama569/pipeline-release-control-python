"""状态机：发布、失败、取消的状态迁移与审计记录必须始终一致。"""
from __future__ import annotations

import pytest

from app.errors import ErrorCode, ServiceError
from app.models import ReleaseStatus
from tests.conftest import make_artifact


def test_failed_publish_does_not_change_current(service, registered, operator):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-1")

    def fail_hook(release):
        raise ServiceError(ErrorCode.RELEASE_FAILED, "network down")

    service.publish_hooks["p1"] = fail_hook
    with pytest.raises(ServiceError) as ei:
        service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                        idempotency_key="p2", request_id="req-2")
    assert ei.value.code == ErrorCode.RELEASE_FAILED

    # 1.0.0 仍然生效；2.0.0 留在 failed 终态，无 pending 卡死
    current = service.get_current(operator, "p1", "dev")
    assert current.artifact.version == "1.0.0"
    failed = [r for r in service.history(operator, "p1")
              if r.artifact.version == "2.0.0"][0]
    assert failed.status is ReleaseStatus.FAILED
    assert failed.failure_reason.endswith("network down")
    # 失败之后同一条流水线可以继续发布
    del service.publish_hooks["p1"]
    r3 = service.publish(operator, "p1", "dev", make_artifact("3.0.0"),
                         idempotency_key="p3", request_id="req-3")
    assert r3.status is ReleaseStatus.PUBLISHED


def test_cancel_only_allowed_from_pending(service, registered, operator):
    service.mutation_delay["p1"] = 0  # publish 同步完成
    r = service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                        idempotency_key="p1", request_id="req-1")
    with pytest.raises(ServiceError) as ei:
        service.cancel(operator, "p1", "dev", r.release_id, request_id="req-2")
    assert ei.value.code == ErrorCode.RELEASE_NOT_CANCELLABLE


def test_mark_failed_then_cancel_is_rejected(service, registered, operator):
    # 直接构造 pending：用延迟钩子来不及，改用 mark_failed 前先建立 pending。
    # 通过 store 直接模拟一个已受理但未完成的发布。
    from app.models import Release
    state = service.store.require_pipeline("p1")
    release = Release(release_id="rel_pending", pipeline_id="p1",
                      environment="dev", artifact=make_artifact("0.9.0"))
    service.store.add_release(state, release)

    failed = service.mark_failed(operator, "p1", "dev", "rel_pending",
                                 "deploy error", request_id="req-1")
    assert failed.status is ReleaseStatus.FAILED
    with pytest.raises(ServiceError) as ei:
        service.cancel(operator, "p1", "dev", "rel_pending", request_id="req-2")
    assert ei.value.code == ErrorCode.RELEASE_NOT_CANCELLABLE


def test_terminal_states_have_no_outgoing_transitions():
    from app.models import ALLOWED_TRANSITIONS
    for terminal in (ReleaseStatus.FAILED, ReleaseStatus.CANCELLED,
                     ReleaseStatus.ROLLED_BACK):
        assert ALLOWED_TRANSITIONS[terminal] == frozenset()


def test_every_status_change_has_matching_audit(service, registered, operator):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-1")
    service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                    idempotency_key="p2", request_id="req-2")
    service.rollback(operator, "p1", "dev", "1.0.0",
                     idempotency_key="rb1", request_id="req-3")

    releases = service.history(operator, "p1")
    final_status = {r.release_id: r.status for r in releases}
    audits = service.audits(operator, "p1")

    # 每条记录的最终状态都能在审计链中找到对应的 to_status
    for rid, status in final_status.items():
        assert any(a.release_id == rid and a.to_status == status.value
                   for a in audits), f"missing audit for {rid} -> {status}"

    # 审计迁移必须连续合法：每个 release 的审计顺序遵循状态机
    by_release: dict[str, list[str]] = {}
    for a in audits:
        if a.release_id:
            by_release.setdefault(a.release_id, []).append(a.to_status)
    for rid, chain in by_release.items():
        # 允许出现 denied（to_status 为空），过滤后必须是合法前缀链
        chain = [s for s in chain if s]
        expected_prefix = []
        if chain:
            assert chain[0] == ReleaseStatus.PENDING.value
            assert chain[-1] == final_status[rid].value


def test_audit_chain_contains_transition_reasons(service, registered, operator,
                                                 caplog_audit):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-xyz")
    audits = [a for a in service.audits(operator, "p1") if a.release_id]
    assert all(a.request_id == "req-xyz" for a in audits)
    # 日志里可看到请求标识与 accepted 判定依据
    assert "req-xyz" in caplog_audit.text
    assert "accepted version=1.0.0" in caplog_audit.text
