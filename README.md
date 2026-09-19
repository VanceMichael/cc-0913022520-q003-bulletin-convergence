# 航班中断公告收敛中心

航班系统故障期间，机场、航司和地服会同时向多个下游发布取消 / 恢复公告。
本服务不做又一张航班状态表，而是回答一个问题：**每个订阅方最终收到了哪一版公告？**

纯后端：Go + Chi + PostgreSQL（pgx），无浏览器客户端。

## 核心语义

- **业务键 + 单调版本号**：公告以 `business_key` 标识，每次发布必须携带
  `previous_version`（新键为 `0`）。版本只追加；取消（`cancellation`）与
  恢复（`restoration`）都是新版本。前版号不匹配（并发落败者、以旧版本发布撤销）
  一律 `409 version_conflict`，**撤销只能发布更高版本**。
- **事务性发件箱**：版本写入与「每个活跃订阅方一条投递记录」在同一事务完成，
  唯一约束 `(announcement_id, version, subscriber_id)` 保证每订阅方每版恰好一行。
- **幂等发布**：`Idempotency-Key` + 请求体指纹。占用、业务变更与首次响应缓存
  同事务落盘；重复请求原样返回首次结果（响应头 `Idempotent-Replayed: true`），
  不产生新版本 / 新投递；同键不同体返回 `409`。
- **租约投递**：投递器用 `FOR UPDATE SKIP LOCKED` 领取任务并加盖 worker 租约。
  回调 2xx 才置 `delivered`；失败进入 2s/5s/15s/30s/1m/2m 的分级退避重试，
  超过 6 次进 `dead`。状态回写必须出示租约令牌，租约易主后旧持有者的
  「迟到成功」影响 0 行——HTTP 层面至少一次，状态层面只生效一次。
- **崩溃恢复**：进程被杀死后，`leased` 行在租约过期后自动被回收重做，
  重启后的进程继续未完成投递；不会丢失，也不会提前完成。
- **回执状态机**：订阅方凭 `X-Api-Key` 回执。未知公告 / 未知版本 →
  `unknown_version`；版本低于当前收敛版本（撤销/恢复后迟到的确认）→
  `obsolete_version`；该版本尚未投递成功 → `not_delivered`；未知订阅方 → 401。
  重复回执返回首次结果并标 `duplicate=true`，不产生新的状态迁移。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 存活 + 数据库 ping（db 不可用时 503） |
| GET  | `/metrics` | Prometheus 指标 |
| POST | `/v1/subscribers` | 注册订阅方（name / callback_url / api_key） |
| POST | `/v1/announcements` | 发布公告（需 `Idempotency-Key`） |
| GET  | `/v1/announcements/{businessKey}` | **收敛核对视图**：全部版本 + 每订阅方每版投递状态 |
| POST | `/v1/announcements/{businessKey}/receipts` | 订阅方回执（需 `X-Api-Key`） |

发布请求：

```json
{
  "business_key": "MU5735-2026-09-19",
  "flight_key": "MU5735",
  "kind": "cancellation",
  "status_text": "FLIGHT CANCELLED",
  "body": {"reason": "weather"},
  "publisher": "ops-desk",
  "previous_version": 0
}
```

投递器以 `POST` 调用订阅方 `callback_url`，报文含 `business_key / version /
kind / status_text / body / published_at`，请求头带 `X-Delivery-Id` 与
`X-Bulletin-Version`。订阅方返回 2xx 视为送达。

关键指标：

- `bulletin_announcements_published_total{kind}` — 真实发布（不含幂等重放）
- `bulletin_publish_version_conflicts_total` — 前版号冲突
- `bulletin_idempotent_replays_total` / `bulletin_idempotent_rejected_total{reason}`
- `bulletin_dispatch_claimed_total`、`bulletin_dispatch_attempts_total{result}`
  （`success|failure|dead`）、`bulletin_dispatch_retries_scheduled_total`、
  `bulletin_dispatch_lease_recycled_total`（崩溃回收次数）
- `bulletin_receipts_acked_total{duplicate}`、
  `bulletin_receipts_rejected_total{reason}`
- `bulletin_outbox_deliveries{status}` — **以数据库为事实源**的状态行数 gauge，
  与公告详情双向对账；`bulletin_dispatch_in_flight`

## 工程结构

```
cmd/server            入口：配置、迁移、信号驱动的优雅停机
internal/domain       模型与错误（无传输 / 存储依赖）
internal/store        迁移、发布/回执事务、发件箱租约调度（pgx）
internal/dispatcher   后台投递器：领取、HTTP 回调、分级退避、死信
internal/api          Chi 路由、请求校验、JSON 错误
internal/metrics      Prometheus 指标集 + DB gauge 采集器
test/integration      真实 PostgreSQL 的集成测试（build tag: integration）
```

## 容器运行

```bash
docker compose up --build -d
curl -s http://localhost:8080/health
# 注册订阅方 → 发布 → 查收敛详情 → 回执
curl -s http://localhost:8080/metrics | grep bulletin_
docker compose down            # 加 -v 同时清空数据库卷
```

环境变量：`DATABASE_URL`（必填）、`PORT`、`METRICS_INTERVAL`、
`DISPATCH_POLL_INTERVAL`、`DISPATCH_BATCH_SIZE`、`DISPATCH_LEASE_DURATION`、
`DISPATCH_HTTP_TIMEOUT`、`DISPATCH_CONCURRENCY`。

## 本地检查

```bash
go test ./...                                   # 不依赖数据库的单元测试
go vet ./...

# 集成测试需要真实 PostgreSQL，二选一：
# 1) 指向外部 PostgreSQL 14+
TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable' \
  go test -tags=integration -v ./test/integration
# 2) 指向解压好的便携 PostgreSQL（目录内含 bin/postgres）
TEST_PG_HOME=/path/to/pg go test -tags=integration -v ./test/integration
```

集成测试覆盖：

1. **并发发布**：20 个相同前版号的请求恰好 1 个胜出，其余 409；同幂等键重放
   返回首次结果且不产生新版本 / 新投递；同键不同体被拒；撤销后只能发更高版本。
2. **投递器中断 / 重启**：回调挂起时杀死投递器，迟到的 200 不被记账；
   租约过期后新实例回收同一发件箱行（同一 `X-Delivery-Id`）重新送达，
   `attempts=2`，物理两次、状态只生效一次。
3. **分级重试 / 死信**：前两次 500 第三次成功；持续失败 6 次后进 `dead`。
4. **回执**：未送达 / 未知版本 / 未知公告 / 未知订阅方分别被拒；撤销恢复后
   迟到的旧版确认以 `obsolete_version` 拒绝；重复回执 `duplicate=true`。
5. **收敛对账**：3 版 × 4 订阅方，从公告详情与 Prometheus 指标核对没有丢失、
   重复生效或提前完成的订阅记录。

## 设计注记

- 投递器无状态：进行中状态全部在数据库行上，水平扩容时多实例由
  `SKIP LOCKED` 互斥；worker 令牌防止租约易主后的重复状态迁移。
- 优雅停机先摘 HTTP 流量，再等待在飞回调结束；未及收尾的回调对应行保持
  `leased`，交由租约回收，故停机本身不丢消息。
- 时间统一 UTC / ISO 8601；错误统一 `{"error":{"code","message","reason"}}`。
