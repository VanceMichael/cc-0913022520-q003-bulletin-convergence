-- 0001_init.sql — 公告收敛中心核心模型
--
-- announcements          业务键头表（current_version 为乐观并发控制点）
-- announcement_versions  每次发布的版本行（idempotency_key 唯一 → 重复请求返回首次结果）
-- subscribers            订阅方（机场/航司/地服），携带 webhook 回调地址
-- deliveries             事务性发件箱：与版本行同事务写入，投递器按租约消费
-- receipts               订阅方回执（receipt_key 唯一 → 重复回执返回首次结果）

CREATE TABLE IF NOT EXISTS announcements (
    id              BIGSERIAL PRIMARY KEY,
    business_key    TEXT NOT NULL UNIQUE,
    current_version BIGINT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS announcement_versions (
    id              BIGSERIAL PRIMARY KEY,
    announcement_id BIGINT NOT NULL REFERENCES announcements (id),
    version         BIGINT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('cancellation', 'recovery', 'revocation')),
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    note            TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (announcement_id, version)
);

CREATE TABLE IF NOT EXISTS subscribers (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    callback_url TEXT NOT NULL,
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 投递状态机：
--   pending  → leased      投递器凭租约领取
--   leased   → sent        webhook 成功（等待回执）
--   leased   → pending     发送失败，按层级退避后重试
--   leased   → dead        重试次数耗尽
--   pending/leased/sent → superseded   更高版本发布（含撤销）后取代未完成投递
--   sent     → acked       订阅方回执确认（终态，唯一“生效”路径）
CREATE TABLE IF NOT EXISTS deliveries (
    id                      BIGSERIAL PRIMARY KEY,
    announcement_id         BIGINT NOT NULL REFERENCES announcements (id),
    announcement_version_id BIGINT NOT NULL REFERENCES announcement_versions (id),
    business_key            TEXT NOT NULL,
    subscriber_id           BIGINT NOT NULL REFERENCES subscribers (id),
    version                 BIGINT NOT NULL,
    kind                    TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'sent', 'acked', 'superseded', 'dead')),
    attempt_count           INT NOT NULL DEFAULT 0,
    max_attempts            INT NOT NULL DEFAULT 8,
    next_attempt_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner             TEXT,
    lease_expires_at        TIMESTAMPTZ,
    last_error              TEXT NOT NULL DEFAULT '',
    sent_at                 TIMESTAMPTZ,
    acked_at                TIMESTAMPTZ,
    receipt_id              BIGINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (announcement_version_id, subscriber_id)
);

CREATE INDEX IF NOT EXISTS deliveries_claim_idx
    ON deliveries (next_attempt_at)
    WHERE status IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS deliveries_announcement_idx
    ON deliveries (announcement_id, subscriber_id, version);

CREATE TABLE IF NOT EXISTS receipts (
    id            BIGSERIAL PRIMARY KEY,
    receipt_key   TEXT NOT NULL UNIQUE,
    delivery_id   BIGINT NOT NULL REFERENCES deliveries (id),
    subscriber_id BIGINT NOT NULL REFERENCES subscribers (id),
    version       BIGINT NOT NULL,
    verdict       TEXT NOT NULL DEFAULT 'accepted',
    note          TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS receipts_delivery_idx ON receipts (delivery_id);
