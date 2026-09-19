# 航班中断公告收敛中心

航班系统故障期间，机场、航司、地服会同时向多个下游发布取消/恢复公告。本服务是运营席的**收敛中心**：它不替代航班状态表，而是证明**每个订阅方最终收到了哪一版公告**。

纯后端服务（Go + Chi + PostgreSQL），核心能力：

- **事务性发件箱**：公告版本与全部订阅方投递在同一事务内落库，不存在"公告已发但投递丢失"的中间态。
- **幂等发布**：`idempotency_key` 唯一，重复请求（含并发重复）返回首次结果；`expected_version`（前版号）做乐观并发控制，并发发布只有一个成功。
- **撤销即更高版本**：撤销只能以更高版本发布，不能重复撤销；更高版本出现后，未被回执确认的旧投递立即被取代（superseded）。
- **租约投递器**：后台投递器凭租约批量领取发件箱任务（`FOR UPDATE SKIP LOCKED`），失败按层级退避重试、超限转死信；进程重启后，过期租约被重新领取，未完成的投递继续执行。
- **严格回执**：回执是订阅记录"生效"的唯一路径——未知投递、过期（已被取代）版本、未送达投递、订阅方不符一律拒绝；重复回执按幂等键返回首次结果，不产生二次生效。
- **可核对性**：公告详情接口给出每个订阅方在每个版本上的投递状态；Prometheus 指标同时以计数器（事件）和 gauge（状态）两个维度暴露，二者可互相印证。

## 快速开始

```bash
docker compose up --build -d        # 拉起 postgres + app + subscriber-sim
curl localhost:8080/healthz         # {"status":"ok",...}
```

发布一条取消公告（前版号 0 表示新业务键）：

```bash
curl -X POST localhost:8080/api/v1/announcements -d '{
  "idempotency_key": "pub-001",
  "business_key": "CA1234:2026-09-19",
  "expected_version": 0,
  "kind": "cancellation",
  "payload": {"flight": "CA1234", "reason": "weather"}
}'
# 201 → {"version":1,"deliveries":[{"delivery_id":...,"subscriber":"airport:PEK"},...]}
# 再次发送同一请求 → 200，返回与首次完全相同的结果
```

撤销（只能以更高版本）：

```bash
curl -X POST localhost:8080/api/v1/announcements/CA1234:2026-09-19/revocations -d '{
  "idempotency_key": "rev-001",
  "expected_version": 1,
  "note": "issued by mistake"
}'
```

查看收敛详情（每个订阅方收敛于哪个版本）：

```bash
curl localhost:8080/api/v1/announcements/CA1234:2026-09-19
curl localhost:8080/metrics   # Prometheus 文本格式
```

## 运行集成测试

集成测试以黑盒方式运行（只通过 HTTP API 与指标断言），覆盖：并发发布、投递器进程中断与租约恢复、重复回执、撤销后迟到确认、分级重试与死信、终态一致性核对：

```bash
docker compose -f docker-compose.yml -f docker-compose.test.yml \
  up --build --exit-code-from tester --abort-on-container-exit tester
docker compose -f docker-compose.yml -f docker-compose.test.yml down -v
```

`tester` 容器挂载 `docker.sock`，在"投递器中断"用例中执行 `docker restart bulletins-app` 制造真实的进程重启。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/announcements` | 发布取消/恢复公告（201 新建 / 200 幂等命中 / 409 版本或幂等冲突） |
| POST | `/api/v1/announcements/{key}/revocations` | 撤销公告（更高版本；404 不存在 / 409 已撤销或版本冲突） |
| GET | `/api/v1/announcements/{key}` | 收敛详情：版本历史 + 每订阅方投递状态 + 汇总 |
| POST | `/api/v1/receipts` | 订阅方回执（201 受理 / 200 幂等命中 / 404 未知投递 / 409 过期版本等） |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/healthz` | 健康检查 |

错误统一为 `{"error":{"code","message","details?}}`。关键错误码：`version_conflict`、`idempotency_conflict`、`already_revoked`、`unknown_delivery`、`stale_version`、`not_delivered`、`already_acknowledged`、`subscriber_mismatch`、`version_mismatch`、`receipt_key_conflict`。

## 状态机与不变量

投递（delivery）：`pending → leased → sent → acked`；`leased → pending`（退避重试）；`leased → dead`（次数耗尽）；`pending/leased/sent → superseded`（更高版本出现）。

- 只有 `sent` 状态的投递接受回执 → 不会**提前完成**。
- 回执与投递 1:1（`receipt_key` 幂等 + 已确认拒绝二次回执）→ 不会**重复生效**。
- 发件箱与公告同事务、租约到期可重领 → 不会**丢失**。
- 核对方式：`bulletin_receipts_total{result="accepted"}` 的增量必然等于 `bulletin_deliveries_status{status="acked"}` gauge 的增量；终态时 `pending/leased/sent` gauge 必须为 0。

## 主要配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_URL` | 本地 postgres | 连接串 |
| `SUBSCRIBERS` | 空 | `名称=回调URL;...` 种子订阅方 |
| `DISPATCHER_TICK` / `DISPATCHER_BATCH_SIZE` | 500ms / 50 | 领取节奏 |
| `DISPATCHER_LEASE_TTL` | 30s | 租约时长；进程死亡后任务在此时间内被重新领取 |
| `RETRY_TIER_DELAYS` | 1s,5s,15s,1m,5m | 分级退避层级，用尽后保持末级 |
| `DELIVERY_MAX_ATTEMPTS` | 8 | 超过后转死信 |
| `DELIVERY_HTTP_TIMEOUT` | 5s | webhook 发送超时 |

## 工程结构

```
cmd/server            服务入口（API + 投递器，优雅退出）
cmd/subscriber-sim    模拟订阅方（auto/hold/slow/fail 模式，自动回执）
internal/bulletins    公告发布/撤销/详情（事务性发件箱、乐观并发、幂等）
internal/receipts     回执受理（严格校验、幂等）
internal/dispatcher   租约投递器（SKIP LOCKED、分级重试、重启恢复）
internal/store        连接池与内嵌迁移
internal/metrics      Prometheus 指标（计数器 + 状态 gauge）
tests/integration     黑盒集成测试（compose 中运行）
```
