-- 收敛中心的初始模式：公告版本流 + 事务性发件箱 + 幂等请求。

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     integer PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscribers (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name          text NOT NULL,
    callback_url  text NOT NULL,
    api_key       text NOT NULL UNIQUE,
    active        boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- 每个业务键一行，current_version 是已收敛的最新版本号（0 表示尚未发布任何版本）。
CREATE TABLE IF NOT EXISTS announcements (
    business_key    text PRIMARY KEY,
    flight_key      text,
    current_version integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
    latest_kind     text,
    latest_status   text,
    latest_payload  jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (current_version = 0 AND latest_kind IS NULL)
        OR (current_version > 0 AND latest_kind IS NOT NULL)
    )
);

-- 只追加的版本历史。撤销（cancellation）与恢复（restoration）都只是新版本。
CREATE TABLE IF NOT EXISTS announcement_versions (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    announcement_id text NOT NULL REFERENCES announcements(business_key) ON DELETE CASCADE,
    version         integer NOT NULL CHECK (version >= 1),
    kind            text NOT NULL CHECK (kind IN ('cancellation', 'restoration', 'update')),
    status_text     text NOT NULL,
    body            jsonb NOT NULL DEFAULT '{}'::jsonb,
    publisher       text NOT NULL DEFAULT '',
    published_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (announcement_id, version)
);

CREATE TABLE IF NOT EXISTS outbox_deliveries (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    announcement_id text NOT NULL REFERENCES announcements(business_key) ON DELETE CASCADE,
    version         integer NOT NULL CHECK (version >= 1),
    subscriber_id   uuid NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'leased', 'retry_wait', 'delivered', 'acked', 'dead')),
    attempts        integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL DEFAULT 6,
    leased_by       text,
    leased_at       timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    delivered_at    timestamptz,
    acknowledged_at timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    -- 发件箱 fan-out 的唯一性保证：每个订阅方每版恰好一行。
    UNIQUE (announcement_id, version, subscriber_id)
);

-- 调度热路径：待领取 / 等待重试的任务。
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch
    ON outbox_deliveries (next_attempt_at)
    WHERE status IN ('pending', 'retry_wait');

-- 租约回收：投递器崩溃后，leased 行需要被重新领取。
CREATE INDEX IF NOT EXISTS idx_outbox_lease
    ON outbox_deliveries (leased_at)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_outbox_lookup
    ON outbox_deliveries (announcement_id, subscriber_id, version);

-- 幂等请求：同键重放返回首次结果，不同请求体拒绝。
CREATE TABLE IF NOT EXISTS idempotent_requests (
    idempotency_key     text PRIMARY KEY,
    request_fingerprint text NOT NULL,
    state               text NOT NULL DEFAULT 'in_flight'
                        CHECK (state IN ('in_flight', 'completed')),
    response_status     integer,
    response_body       jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz
);

INSERT INTO schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
