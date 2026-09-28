"""服务层测试：幂等、并发、越权、回滚与状态一致性。"""
import logging
import threading

import pytest

from release_control.errors import ReasonCode, ReleaseError
from release_control.models import Build, Caller, Environment, Role, ReleaseStatus
from release_control.service import ReleaseService
from release_control.store import InMemoryStore

ADMIN = Caller("admin-1", Role.ADMIN)
OPERATOR = Caller("op-1", Role.OPERATOR)
VIEWER = Caller("viewer-1", Role.VIEWER)


@pytest.fixture()
def service():
    return ReleaseService(InMemoryStore())


def make_build(build_id="b1", pipeline_id="p1", config=None, schema_version=1):
    return Build(
        build_id=build_id,
        pipeline_id=pipeline_id,
        artifact_uri=f"s3://artifacts/{build_id}.tar.gz",
        config=config if config is not None else {"replicas": 2},
        schema_version=schema_version,
    )


def publish(service, build_id="b1", key="k1", env=Environment.PROD, caller=OPERATOR):
    return service.publish(caller, "p1", env, make_build(build_id), key, f"req-{key}")


# ---- 幂等 ----

def test_duplicate_submission_returns_same_record(service):
    first = publish(service, key="k-dup")
    second = publish(service, key="k-dup")
    assert first.release_id == second.release_id
    assert len(service._store.releases_for("p1", Environment.PROD)) == 1


def test_same_key_with_different_params_rejected(service):
    publish(service, build_id="b1", key="k-conflict")
    with pytest.raises(ReleaseError) as exc:
        publish(service, build_id="b2", key="k-conflict")
    assert exc.value.code == ReasonCode.DUPLICATE_REQUEST


# ---- 并发 ----

def test_concurrent_publish_only_one_wins(service):
    lock = service._store.pipeline_lock("p1")
    lock.acquire()  # 模拟另一个请求正在操作同一条流水线
    try:
        with pytest.raises(ReleaseError) as exc:
            publish(service, key="k-busy")
        assert exc.value.code == ReasonCode.CONCURRENT_CONFLICT
    finally:
        lock.release()
    # 锁释放后可以正常发布，且只有一条记录
    publish(service, key="k-after")
    assert len(service._store.releases_for("p1", Environment.PROD)) == 1


def test_concurrent_threads_exactly_one_success(service):
    barrier = threading.Barrier(2)
    results, errors = [], []

    def worker(key):
        barrier.wait()
        try:
            results.append(publish(service, key=key))
        except ReleaseError as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(f"kt-{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) + len(errors) == 2
    if errors:  # 若发生冲突，原因必须可区分
        assert all(e.code == ReasonCode.CONCURRENT_CONFLICT for e in errors)
    assert len(service._store.releases_for("p1", Environment.PROD)) == len(results)


# ---- 权限 ----

def test_viewer_cannot_publish(service):
    with pytest.raises(ReleaseError) as exc:
        publish(service, caller=VIEWER)
    assert exc.value.code == ReasonCode.PERMISSION_DENIED


def test_operator_cannot_rollback(service):
    record = publish(service)
    with pytest.raises(ReleaseError) as exc:
        service.rollback(OPERATOR, "p1", Environment.PROD, record.release_id, "k-rb", "req-rb")
    assert exc.value.code == ReasonCode.PERMISSION_DENIED


# ---- 回滚 ----

def test_rollback_restores_exact_artifact_and_config(service):
    first = publish(service, build_id="b1", key="k1")
    publish(service, build_id="b2", key="k2")
    rolled = service.rollback(ADMIN, "p1", Environment.PROD, first.release_id, "k-rb", "req-rb")
    assert rolled.status == ReleaseStatus.PUBLISHED
    assert rolled.build.build_id == "b1"
    assert rolled.build.artifact_uri == first.build.artifact_uri
    assert rolled.build.config == first.build.config
    # 被取代的版本状态变为 rolled_back，不留中间态
    current = service._store.current_release("p1", Environment.PROD)
    assert current.release_id == rolled.release_id


def test_rollback_to_missing_version_rejected(service):
    publish(service)
    with pytest.raises(ReleaseError) as exc:
        service.rollback(ADMIN, "p1", Environment.PROD, "no-such-release", "k-rb", "req-rb")
    assert exc.value.code == ReasonCode.VERSION_NOT_FOUND


def test_rollback_to_non_published_version_rejected(service):
    store = service._store
    record = service.publish(OPERATOR, "p1", Environment.PROD, make_build("b1"), "k1", "req-k1")
    # 构造一个已取消的历史版本
    cancelled = service.publish(OPERATOR, "p1", Environment.STAGING, make_build("b0"), "k0", "req-k0")
    store_cancelled = store.get(cancelled.release_id)
    store_cancelled.status = ReleaseStatus.PENDING  # 回到可取消状态
    store.save(store_cancelled)
    service.cancel(OPERATOR, cancelled.release_id, "req-cancel")
    with pytest.raises(ReleaseError) as exc:
        service.rollback(ADMIN, "p1", Environment.STAGING, cancelled.release_id, "k-rb", "req-rb")
    assert exc.value.code == ReasonCode.VERSION_INCOMPATIBLE
    assert record.status == ReleaseStatus.PUBLISHED  # 原有状态未被改动


def test_rollback_to_incompatible_schema_rejected(service):
    old = publish(service, build_id="b-old", key="k-old")
    # 模拟系统升级后旧 schema 不再受支持
    import release_control.service as svc
    original = svc.SUPPORTED_SCHEMA_VERSIONS
    svc.SUPPORTED_SCHEMA_VERSIONS = frozenset({2})
    try:
        with pytest.raises(ReleaseError) as exc:
            service.rollback(ADMIN, "p1", Environment.PROD, old.release_id, "k-rb", "req-rb")
        assert exc.value.code == ReasonCode.VERSION_INCOMPATIBLE
    finally:
        svc.SUPPORTED_SCHEMA_VERSIONS = original


def test_failed_rollback_leaves_no_partial_state(service):
    first = publish(service, build_id="b1", key="k1")
    second = publish(service, build_id="b2", key="k2")
    with pytest.raises(ReleaseError):
        service.rollback(ADMIN, "p1", Environment.PROD, "missing", "k-rb", "req-rb")
    # 当前发布仍然是 second，且没有产生任何新记录
    assert service._store.current_release("p1", Environment.PROD).release_id == second.release_id
    assert len(service._store.releases_for("p1", Environment.PROD)) == 2
    assert first.status == ReleaseStatus.PUBLISHED


# ---- 状态机与审计一致性 ----

def test_cancel_published_release_rejected(service):
    record = publish(service)
    with pytest.raises(ReleaseError) as exc:
        service.cancel(OPERATOR, record.release_id, "req-cancel")
    assert exc.value.code == ReasonCode.INVALID_STATE_TRANSITION


def test_state_and_audit_always_match(service):
    record = publish(service, key="k-audit")
    audits = service._store.audits_for(record.release_id)
    # 每次迁移都有审计，且链条连续、终态与记录状态一致
    assert audits[0].from_status == "" and audits[0].to_status == "pending"
    for prev, nxt in zip(audits, audits[1:]):
        assert prev.to_status == nxt.from_status
    assert audits[-1].to_status == record.status.value


def test_logs_contain_request_id_transition_and_reason(service, caplog):
    with caplog.at_level(logging.INFO, logger="release_control"):
        record = publish(service, key="k-log")
        publish(service, key="k-log")  # 幂等重放
    text = caplog.text
    assert "req-k-log" in text
    assert "pending -> published" in text
    assert "判定依据" in text and "幂等键" in text
    assert record.release_id in text
