# Pipeline Release Control

数据管道构建的发布与回滚控制服务。同一条流水线在不同环境、不同调用方下的状态变化可预期、可复现，失败时能给出可区分的原因。

## 状态流转

```
                publish / rollback
                      │
                      ▼
        (创建) ──► PENDING ──► PUBLISHED ──► ROLLED_BACK
                      │  │
                      │  └────► FAILED
                      └───────► CANCELLED
```

- 允许迁移：`PENDING → PUBLISHED / FAILED / CANCELLED`，`PUBLISHED → ROLLED_BACK`。
- 终态（`FAILED` / `CANCELLED` / `ROLLED_BACK`）不可再迁移；非法迁移返回 `invalid_state_transition`。
- 每次迁移都写入一条审计记录（`from_status`、`to_status`、`reason`、`actor`、`request_id`），审计链连续且终态与记录状态一致，不会出现卡在中间或互相矛盾的状态。

## 发布与回滚

- **发布**：`POST /pipelines/{pipeline_id}/releases`，把某个 build 发布到 `dev / staging / prod`。
- **回滚**：`POST /pipelines/{pipeline_id}/rollbacks`，指定历史 `target_release_id`。回滚产生一条新记录，其产物 URI 与配置**完整复制**自目标版本，保证与当时那次发布一致；被取代的当前版本置为 `ROLLED_BACK`。
- 目标版本不存在 → `version_not_found`；目标版本不是已发布状态或 schema 版本与当前系统不兼容 → `version_incompatible`。
- 回滚先完成全部校验再一次性变更，失败不会留下改了一半的状态。

## 幂等与并发规则

- 每个写操作必须携带 `Idempotency-Key` 请求头。同一 `(pipeline_id, idempotency_key)` 重复提交（重试、超时重发）直接返回首次结果，不产生重复记录。
- 同一幂等键被不同参数复用 → `duplicate_request`（409）。
- 每条流水线一把锁，非阻塞获取：多个请求同时操作同一条流水线时只有一个成功，其余立即得到 `concurrent_conflict`（409），不会排队等待或互相覆盖。

## 权限模型

| 动作              | viewer | operator | admin |
|-------------------|--------|----------|-------|
| 查询发布记录       | ✓      | ✓        | ✓     |
| 发布 / 取消        | ✗      | ✓        | ✓     |
| 回滚              | ✗      | ✗        | ✓     |

调用方通过请求头标识身份：`X-Caller-Id`、`X-Role`（`viewer` / `operator` / `admin`）。越权操作直接拒绝，返回 `permission_denied`（403）。

## 错误码

| code                       | HTTP | 含义                           |
|----------------------------|------|--------------------------------|
| `permission_denied`        | 403  | 越权操作                        |
| `duplicate_request`        | 409  | 幂等键被不同参数复用             |
| `concurrent_conflict`      | 409  | 同一流水线被其他请求占用          |
| `invalid_state_transition` | 409  | 非法状态迁移                    |
| `not_found`                | 404  | 发布记录不存在                  |
| `version_not_found`        | 404  | 回滚目标版本不存在              |
| `version_incompatible`     | 422  | 目标版本不可回滚或 schema 不兼容 |

## 日志

关键路径打印 `request_id`、状态迁移和判定依据，例如：

```
request_id=req-1 release=abc123 transition pending -> published reason=发布 build=b1 到 prod
request_id=req-2 pipeline=p1 判定依据: 幂等键 k1 命中 release=abc123，直接返回首次结果
request_id=req-3 pipeline=p1 判定依据: 流水线锁被占用 -> concurrent_conflict
```

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 运行全部测试（幂等、并发、越权、回滚到不存在版本等场景）
uv run python main.py   # 本地启动服务
```
