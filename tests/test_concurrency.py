"""同一流水线并发变更：只能有一个成功，其余被明确挡下且原因可区分。"""
from __future__ import annotations

import threading

import pytest

from app.errors import ErrorCode
from tests.conftest import make_artifact


def run_concurrently(func, calls, start_gap: float = 0.15):
    """依次启动线程（间隔 start_gap 秒），等全部结束，收集结果/异常。"""
    outcomes: dict[str, object] = {}

    def worker(name, *args):
        try:
            outcomes[name] = ("ok", func(*args))
        except Exception as exc:  # noqa: BLE001
            outcomes[name] = ("error", exc)

    threads = []
    for i, (name, args) in enumerate(calls):
        t = threading.Thread(target=worker, args=(name, *args))
        threads.append(t)
        t.start()
        if i < len(calls) - 1:
            # 让第一个请求先进入临界区
            if i == 0:
                threading.Event().wait(start_gap)
    for t in threads:
        t.join()
    return outcomes


def test_parallel_publishes_only_one_succeeds(service, registered, operator):
    service.mutation_delay["p1"] = 0.6
    calls = [
        ("req-a", (operator, "p1", "dev", make_artifact("1.0.0"), "ka")),
        ("req-b", (operator, "p1", "dev", make_artifact("2.0.0"), "kb")),
        ("req-c", (operator, "p1", "prod", make_artifact("3.0.0"), "kc")),
    ]
    outcomes = run_concurrently(
        lambda p, pid, env, art, key: service.publish(
            p, pid, env, art, idempotency_key=key, request_id="req"),
        calls,
    )
    ok = [n for n, v in outcomes.items() if v[0] == "ok"]
    denied = [v[1] for n, v in outcomes.items() if v[0] == "error"]
    assert ok == ["req-a"]
    assert len(denied) == 2
    for exc in denied:
        assert exc.code == ErrorCode.CONCURRENT_MODIFICATION
        # 挡路原因必须能定位到正在执行的那条发布
        assert exc.conflict_with == outcomes["req-a"][1].release_id

    # 只有一个生效版本，且不存在卡在 pending 的记录
    current = service.get_current(operator, "p1", "dev")
    assert current.artifact.version == "1.0.0"
    pending = [r for r in service.history(operator, "p1")
               if r.status.value == "pending"]
    assert pending == []


def test_rollback_blocked_while_publish_in_flight(service, registered, operator):
    service.publish(operator, "p1", "dev", make_artifact("1.0.0"),
                    idempotency_key="p1", request_id="req-0")
    service.mutation_delay["p1"] = 0.6

    def publish_v2():
        return service.publish(operator, "p1", "dev", make_artifact("2.0.0"),
                               idempotency_key="p2", request_id="req-pub")

    def rollback_v1():
        return service.rollback(operator, "p1", "dev", "1.0.0",
                                idempotency_key="rb", request_id="req-rb")

    outcomes: dict[str, object] = {}

    def run(name, fn):
        try:
            outcomes[name] = ("ok", fn())
        except Exception as exc:  # noqa: BLE001
            outcomes[name] = ("error", exc)

    t1 = threading.Thread(target=run, args=("pub", publish_v2))
    t1.start()
    threading.Event().wait(0.2)
    t2 = threading.Thread(target=run, args=("rb", rollback_v1))
    t2.start()
    t1.join(); t2.join()

    assert outcomes["pub"][0] == "ok"
    assert outcomes["rb"][0] == "error"
    exc = outcomes["rb"][1]
    assert exc.code == ErrorCode.CONCURRENT_MODIFICATION
    assert exc.conflict_with == outcomes["pub"][1].release_id
    # 发布完成后回滚即可成功，状态没有被半截修改
    rb = service.rollback(operator, "p1", "dev", "1.0.0",
                          idempotency_key="rb2", request_id="req-rb2")
    assert rb.status.value == "published"


def test_concurrent_denial_distinguished_from_idempotent_conflict(
        service, registered, operator):
    """并发冲突码与幂等冲突码必须不同，调用方可以区分重试策略。"""
    service.mutation_delay["p1"] = 0.6
    art = make_artifact("1.0.0")

    outcomes: dict[str, object] = {}

    def run(name, key, version):
        try:
            outcomes[name] = ("ok", service.publish(
                operator, "p1", "dev", make_artifact(version),
                idempotency_key=key, request_id=name))
        except Exception as exc:  # noqa: BLE001
            outcomes[name] = ("error", exc)

    t1 = threading.Thread(target=run, args=("req-a", "K", "1.0.0"))
    t2 = threading.Thread(target=run, args=("req-b", "K-OTHER", "2.0.0"))
    t1.start()
    threading.Event().wait(0.2)
    t2.start()
    t1.join(); t2.join()

    assert outcomes["req-a"][0] == "ok"
    assert outcomes["req-b"][1].code == ErrorCode.CONCURRENT_MODIFICATION
    assert outcomes["req-b"][1].code != ErrorCode.IDEMPOTENCY_CONFLICT


def test_denied_requests_leave_no_partial_state(service, registered, operator):
    service.mutation_delay["p1"] = 0.5
    outcomes = run_concurrently(
        lambda key, ver: service.publish(
            operator, "p1", "dev", make_artifact(ver),
            idempotency_key=key, request_id="req"),
        [("a", ("k1", "1.0.0")), ("b", ("k2", "2.0.0"))],
    )
    # 被挡下的一方不应留下 pending 记录或幂等残留导致后续被误判
    service.mutation_delay["p1"] = 0
    art = make_artifact("2.0.0")
    later = service.publish(operator, "p1", "dev", art,
                            idempotency_key="k2", request_id="req-later")
    assert later.status.value == "published"


@pytest.mark.parametrize("role", ["viewer", "operator"])
def test_http_concurrent_publishes(client, registered, role):
    # HTTP 层通过故障注入钩子也能观测并发：这里直接串行验证在途语义。
    svc = client.app.state.service
    svc.mutation_delay["p1"] = 0.6
    results: dict[str, tuple] = {}

    def call(name):
        r = client.post(
            "/pipelines/p1/environments/dev/releases",
            json={"artifact": {
                "artifact_id": f"art-{name}", "version": f"{name}.0.0",
                "checksum": f"sha-{name}", "config_snapshot": {},
                "compatible_generations": ["gen-1"]}},
            headers={"X-Role": "operator", "X-Username": "ops",
                     "X-Request-Id": f"req-{name}",
                     "Idempotency-Key": f"key-{name}"},
        )
        results[name] = (r.status_code, r.json())

    t1 = threading.Thread(target=call, args=("1",))
    t1.start()
    threading.Event().wait(0.2)
    t2 = threading.Thread(target=call, args=("2",))
    t2.start()
    t1.join(); t2.join()

    statuses = sorted(v[0] for v in results.values())
    assert statuses == [200, 409]
    conflict = next(v[1] for v in results.values() if v[0] == 409)
    assert conflict["code"] == ErrorCode.CONCURRENT_MODIFICATION
    assert conflict["conflict_with"]
