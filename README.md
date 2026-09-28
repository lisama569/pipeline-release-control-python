# 数据管道发布与回滚控制（pipeline-release-control）

让同一条流水线在不同环境、不同调用方下的状态变化 **可预期、可复现、出错可解释**：

- 发布 / 回滚走显式状态机，只有合法迁移能发生；
- 重复提交靠幂等键去重，绝不产生第二条发布记录；
- 同一流水线的并发变更只允许一个成功，其余被明确挡下并给出可区分原因；
- 角色权限前置校验，越权直接 403；
- 每次状态变化（包括被拒绝的请求）都有审计与结构化日志，携带请求标识、迁移路径和判定依据；
- 回滚只恢复“当时那次发布冻结的产物与配置”，版本不存在、未成功发布过或与当前配置不兼容时拒绝，且不留中间态。

## 1. 架构与模块

| 模块 | 职责 |
|---|---|
| `app/models.py` | 枚举（环境/角色/状态）、状态迁移表 `ALLOWED_TRANSITIONS`、`Artifact` / `Release` / `AuditEntry` |
| `app/errors.py` | 稳定错误码、HTTP 状态映射、`ServiceError`（含 `conflict_with` 定位冲突来源） |
| `app/auth.py` | 角色与权限矩阵、请求头身份解析、越权拒绝 |
| `app/store.py` | 线程安全内存存储：**每条流水线一把可重入排他锁**、发布历史、幂等记录、只追加审计表 |
| `app/service.py` | 核心：状态机、幂等重放、并发拦截、回滚事务、审计与日志 |
| `app/api.py` | FastAPI 路由：请求头鉴权、`X-Request-Id`、`Idempotency-Key`、统一错误体 |
| `tests/` | 重复提交、并发冲突、越权、回滚不存在版本等 38 个用例 |

存储为内存实现（演示/测试用）。生产替换为数据库时，用行锁或乐观版本号
实现同等互斥语义即可，service 层接口不变。

## 2. 状态流转

```
                发布受理
                  │
                  ▼
              ┌────────┐  发布成功   ┌───────────┐ 被新版本/回滚替换 ┌─────────────┐
              │ pending │ ─────────▶ │ published │ ────────────────▶ │ rolled_back │
              └────────┘             └───────────┘                    └─────────────┘
                  │  ▲                     ▲
      上报失败/钩子│  │仅 pending 可取消      │回滚成功后新记录也是 published
                  ▼  │                     （回滚产生一条 kind=rollback 的新记录）
              ┌────────┐   ┌───────────┐
              │ failed │   │ cancelled │
              └────────┘   └───────────┘
```

合法迁移（`ALLOWED_TRANSITIONS`，终态无出边）：

- `pending → published | failed | cancelled`
- `published → rolled_back`
- `failed / cancelled / rolled_back` 为终态

关键一致性保证：

- 只有 **pending → published 成功后** 才更新环境的 current 指针；失败/取消
  从不触碰指针，因此旧版本始终在线，不会出现“半截上线”。
- 替换旧版本（发布或回滚成功）与旧版本 `published → rolled_back`、
  current 指针切换在**同一把流水线锁内连续完成**，对外原子。
- 回滚在**所有前置校验通过后才写任何状态**；校验失败时历史与指针原样不变。
- 每次迁移写一条 `AuditEntry`，审计链与 Release 最终状态一一对应。

## 3. 幂等规则

- 发布/回滚请求携带 `Idempotency-Key`（重试、超时重发务必复用同一个键）。
- 幂等键作用域为 **(流水线, 环境, 键)**；不同环境可复用同名键。
- 同键 + 同请求体（对 artifact 与环境做指纹哈希）→ **重放原 Release**，
  不重复执行、不产生新记录（原记录是 failed 也原样重放）。
- 同键 + 不同请求体 → `409 IDEMPOTENCY_CONFLICT`，响应带
  `conflict_with`（原 release_id）。
- 不带幂等键的每次请求都会产生新记录（调用方自行保证不要这样重试）。

## 4. 并发规则

- 每条 `pipeline_id` 一把排他锁，覆盖该流水线**所有环境**的变更
  （发布/回滚/取消/失败上报）。
- 变更请求使用**非阻塞抢锁 + 在途记录检查**双重判定：
  - 锁被占用：`409 CONCURRENT_MODIFICATION`，`conflict_with` 尽力指向
    挡路的 pending release；
  - 已持锁但发现存在 pending 发布：同样拒绝，`conflict_with` 必为该 pending
    release_id。
- 因此并发 N 个变更，恰好 1 个成功，其余全部失败；被拒绝的请求不创建
  Release、不写幂等记录、不留 pending。
- 在途操作到达终态后，后续变更正常进行（串行放行，不丢业务）。
- `CONCURRENT_MODIFICATION` 与 `IDEMPOTENCY_CONFLICT` 是两个独立错误码，
  调用方可据此区分“稍后重试”与“请求体自相矛盾”。

## 5. 权限模型

简易演示鉴权：请求头 `X-Username`、`X-Role`（`viewer|operator|admin`，
缺省 viewer，未知角色直接 403）。

| 动作 | viewer | operator | admin |
|---|:---:|:---:|:---:|
| 查询 current / 历史 / 审计（read） | ✓ | ✓ | ✓ |
| publish 发布 | | ✓ | ✓ |
| rollback 回滚 | | ✓ | ✓ |
| cancel 取消 pending | | ✓ | ✓ |
| report failure 上报失败 | | ✓ | ✓ |
| register pipeline / 改配置代际 | | | ✓ |

- 权限检查 **先于** 资源查找：对不存在的流水线越权写操作返回 403 而不是 404，
  不泄露资源是否存在。
- 越权拒绝也写一条 `<action>.denied` 审计，记录 actor 与原因。

## 6. 回滚规则

`POST .../rollbacks {"target_version": "x.y.z"}`：

1. 该流水线存在在途操作 → `CONCURRENT_MODIFICATION`；
2. 在该**环境**历史中找该版本：
   - 从未提交过 → `404 VERSION_NOT_FOUND`；
   - 存在但只到过 failed/cancelled（从未 published）→ `409 VERSION_NOT_PUBLISHED`；
3. 该版本就是当前生效版本 → `409 VERSION_ALREADY_ACTIVE`；
4. 兼容性：当前流水线 `config_generation` 必须包含在目标历史发布时产物声明的
   `compatible_generations` 中，否则 `422 VERSION_INCOMPATIBLE`
   （防止把旧产物配到不兼容的新配置上）；
5. 全部通过 → 用历史记录中**冻结的 artifact（版本、checksum、config_snapshot
   深拷贝）** 新建一条 `kind=rollback` 的发布并上线，原 current 标记
   `rolled_back`。

修改配置代际（仅 admin）：`POST /pipelines/{id}/config-generation
{"generation":"gen-2"}`，用于驱动上面的兼容性判定。

## 7. HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/pipelines/{id}/register` | 注册流水线（admin），body: `{"config_generation":"gen-1"}` |
| POST | `/pipelines/{id}/config-generation` | 更新配置代际（admin） |
| POST | `/pipelines/{id}/environments/{env}/releases` | 发布（env: dev/staging/prod） |
| POST | `/pipelines/{id}/environments/{env}/rollbacks` | 回滚到历史版本 |
| POST | `/pipelines/{id}/releases/{rid}/cancel` | 取消 pending 发布 |
| POST | `/pipelines/{id}/releases/{rid}/failures` | 上报失败（仅 pending 有效） |
| GET | `/pipelines/{id}/environments/{env}/current` | 当前生效版本 |
| GET | `/pipelines/{id}/releases` | 发布历史 |
| GET | `/pipelines/{id}/audits` | 审计记录 |
| GET | `/healthz` | 健康检查 |

请求头：`X-Username`、`X-Role`、`X-Request-Id`（缺省自动生成并通过响应头
原样回传）、`Idempotency-Key`。

错误响应统一为：

```json
{"code": "CONCURRENT_MODIFICATION",
 "message": "release 'rel_xxx' ... is still pending; reject concurrent mutation",
 "conflict_with": "rel_xxx"}
```

| code | HTTP | 含义 / 建议 |
|---|---:|---|
| `PERMISSION_DENIED` | 403 | 越权或未知角色，不要重试 |
| `NOT_FOUND` / `RELEASE_NOT_FOUND` / `VERSION_NOT_FOUND` | 404 | 流水线/发布/历史版本不存在 |
| `IDEMPOTENCY_CONFLICT` | 409 | 幂等键被不同请求体占用，修正请求或换键 |
| `CONCURRENT_MODIFICATION` | 409 | 有在途变更，带退避重试 |
| `VERSION_NOT_PUBLISHED` | 409 | 目标版本从未成功发布 |
| `VERSION_ALREADY_ACTIVE` | 409 | 目标版本已在生效 |
| `RELEASE_NOT_CANCELLABLE` / `INVALID_STATUS_TRANSITION` | 409 | 当前状态不允许该操作 |
| `VERSION_INCOMPATIBLE` | 422 | 历史版本与当前配置代际不兼容 |
| `VALIDATION_ERROR` | 400 | 请求体不合法 |

## 8. 日志与审计

- 日志 logger 前缀 `release_control`：
  - `request_start/end`：request_id、方法、路径、角色、调用方、幂等键、结果状态码；
  - `audit ... transition=<from>-><to> reason=<判定依据>`；
  - `denied ... code=<错误码> detail=<原因>`；
  - `idempotent replay ...`。
- 审计可通过 `GET /pipelines/{id}/audits` 查询，字段含 request_id、actor、
  action、from/to 状态、release_id、reason、idempotency_key。
- 被拒绝的越权/冲突/兼容性请求同样留审计，保证“为什么没做成”可追溯。

## 9. 本地验证

需要 Python 3.14+（项目用 [uv](https://docs.astral.sh/uv/) 管理）。

```bash
# 安装依赖
uv sync

# 跑全部测试（重复提交 / 并发 / 越权 / 回滚不存在版本等场景）
uv run pytest -q

# 本地启动（端口自选）
uv run uvicorn main:app --port 8000
# 交互式文档
open http://127.0.0.1:8000/docs
```

快速手工验证：

```bash
# 注册流水线（admin）
curl -s -X POST localhost:8000/pipelines/demo/register \
  -H 'X-Role: admin' -H 'X-Username: root' -H 'X-Request-Id: t1' \
  -H 'Content-Type: application/json' \
  -d '{"config_generation":"gen-1"}'

# 发布 1.0.0（operator + 幂等键）
curl -s -X POST localhost:8000/pipelines/demo/environments/prod/releases \
  -H 'X-Role: operator' -H 'X-Username: alice' -H 'X-Request-Id: t2' \
  -H 'Idempotency-Key: key-1' -H 'Content-Type: application/json' \
  -d '{"artifact":{"artifact_id":"a1","version":"1.0.0","checksum":"sha1",
       "config_snapshot":{"feature_x":true},
       "compatible_generations":["gen-1"]}}'

# 用同一个幂等键重发 -> 返回同一条 release，历史里仍只有一条
# 回滚（先发布 2.0.0 再回滚到 1.0.0）
curl -s -X POST localhost:8000/pipelines/demo/environments/prod/rollbacks \
  -H 'X-Role: operator' -H 'X-Username: alice' -H 'X-Request-Id: t3' \
  -H 'Idempotency-Key: rb-1' -H 'Content-Type: application/json' \
  -d '{"target_version":"1.0.0"}'

# 审计链
curl -s localhost:8000/pipelines/demo/audits -H 'X-Role: viewer' -H 'X-Username: bob'
```

### 测试覆盖说明

- `tests/test_idempotency.py`：同键重放不重复、同键异体 409、失败后重放、
  幂等键按环境隔离、回滚幂等；
- `tests/test_concurrency.py`：多线程并发发布恰好一个成功、回滚撞在途发布被挡、
  并发码与幂等码可区分、被挡请求无残留、HTTP 层 409；
  （通过 `service.mutation_delay` 确定性制造并发窗口）
- `tests/test_permissions.py`：viewer 只读、operator 不能改配置、admin 注册、
  未知角色拒绝、鉴权先于资源查找、拒绝留审计；
- `tests/test_rollback.py`：版本不存在、从未发布成功、产物与配置逐字节一致
  （深拷贝）、当前版本拒绝、代际不兼容拒绝且无中间态、跨环境隔离、可重复复现；
- `tests/test_state_machine.py`：失败不切换指针、取消仅限 pending、终态无出边、
  审计与最终状态一一对应；
- `tests/test_api_logging.py`：request_id 生成/回传/入日志、四类错误码可区分、
  回滚审计含 pending→published 与判定依据。

## 10. 设计取舍备忘

- 互斥粒度选了“整条流水线”而非“流水线+环境”，换取更强的可解释性（任何时候
  只有一个在途变更，`conflict_with` 唯一）；如需跨环境并行，可把锁收窄到
  二元组，同时保留在途检查。
- 回滚采用“追加新记录”而不是把历史记录改回 published，审计链只追加、
  不可变，任何一次生效都能独立追溯。
- 鉴权为演示用请求头方案；生产应替换为不透明会话令牌 / mTLS，权限矩阵与
  “先鉴权后查资源”的顺序不变。
