package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/disruption-bulletins/internal/domain"
)

// Repo 在 PostgreSQL 上实现全部持久化用例。所有跨表写操作都在单事务内完成。
type Repo struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// Pool 暴露底层连接池（健康检查等场景使用）。
func (r *Repo) Pool() *pgxpool.Pool { return r.pool }

// ErrLeaseLost 表示回写状态时租约已经易主（投递耗时超过租约，被其他进程回收）。
var ErrLeaseLost = errors.New("delivery lease lost")

// ---------------------------------------------------------------------------
// 订阅方
// ---------------------------------------------------------------------------

// CreateSubscriber 注册一个订阅方。apiKey 重复时返回 domain.ErrInvalidInput。
func (r *Repo) CreateSubscriber(ctx context.Context, name, callbackURL, apiKey string) (*domain.Subscriber, error) {
	s := &domain.Subscriber{}
	err := r.pool.QueryRow(ctx, `
		INSERT INTO subscribers(name, callback_url, api_key)
		VALUES ($1, $2, $3)
		RETURNING id, name, callback_url, api_key, active, created_at`,
		name, callbackURL, apiKey,
	).Scan(&s.ID, &s.Name, &s.CallbackURL, &s.APIKey, &s.Active, &s.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, fmt.Errorf("%w: api_key already registered", domain.ErrInvalidInput)
		}
		return nil, err
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// 发布：版本校验 + 发件箱 fan-out，同一事务
// ---------------------------------------------------------------------------

// outboxPayload 是写入发件箱、随后投递给订阅方的报文。
type outboxPayload struct {
	BusinessKey string          `json:"business_key"`
	FlightKey   string          `json:"flight_key,omitempty"`
	Version     int             `json:"version"`
	Kind        domain.Kind     `json:"kind"`
	StatusText  string          `json:"status_text"`
	Body        json.RawMessage `json:"body"`
	Publisher   string          `json:"publisher,omitempty"`
	PublishedAt time.Time       `json:"published_at"`
}

// PublishAnnouncement 在单事务内执行版本校验、版本追加与发件箱 fan-out。
func (r *Repo) PublishAnnouncement(ctx context.Context, in domain.PublishInput) (*domain.PublishResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	res, err := publishAnnouncementTx(ctx, tx, in)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return res, nil
}

// publishAnnouncementTx 是发布用例的事务内实现，供普通发布与幂等发布共用。
// 调用方持有事务并负责提交 / 回滚。
func publishAnnouncementTx(ctx context.Context, tx pgx.Tx, in domain.PublishInput) (*domain.PublishResult, error) {
	// 并发同名发布中落败的 ON CONFLICT 随后在 SELECT ... FOR UPDATE 处串行化。
	if _, err := tx.Exec(ctx, `
		INSERT INTO announcements(business_key, flight_key)
		VALUES ($1, $2)
		ON CONFLICT (business_key) DO NOTHING`,
		in.BusinessKey, nullableString(in.FlightKey)); err != nil {
		return nil, err
	}

	var currentVersion int
	var currentFlight *string
	if err := tx.QueryRow(ctx, `
		SELECT current_version, flight_key
		FROM announcements
		WHERE business_key=$1
		FOR UPDATE`, in.BusinessKey,
	).Scan(&currentVersion, &currentFlight); err != nil {
		return nil, err
	}

	if in.PreviousVersion != currentVersion {
		return nil, fmt.Errorf("%w: current version is %d, request carried previous_version=%d",
			domain.ErrVersionConflict, currentVersion, in.PreviousVersion)
	}

	newVersion := currentVersion + 1
	publishedAt := time.Now().UTC()

	body := in.Body
	if len(body) == 0 {
		body = json.RawMessage(`{}`)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO announcement_versions
			(announcement_id, version, kind, status_text, body, publisher, published_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		in.BusinessKey, newVersion, string(in.Kind), in.StatusText, body, in.Publisher, publishedAt,
	); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(outboxPayload{
		BusinessKey: in.BusinessKey,
		FlightKey:   in.FlightKey,
		Version:     newVersion,
		Kind:        in.Kind,
		StatusText:  in.StatusText,
		Body:        body,
		Publisher:   in.Publisher,
		PublishedAt: publishedAt,
	})
	if err != nil {
		return nil, err
	}

	// fan-out：同一版本 × 每个活跃订阅方恰好一行。
	fanOut, err := tx.Exec(ctx, `
		INSERT INTO outbox_deliveries
			(announcement_id, version, subscriber_id, payload, next_attempt_at)
		SELECT $1, $2, s.id, $3, now()
		FROM subscribers s
		WHERE s.active
		ON CONFLICT (announcement_id, version, subscriber_id) DO NOTHING`,
		in.BusinessKey, newVersion, string(payload))
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE announcements
		SET current_version=$2,
		    flight_key=COALESCE($3, flight_key),
		    latest_kind=$4,
		    latest_status=$5,
		    latest_payload=$6,
		    updated_at=now()
		WHERE business_key=$1`,
		in.BusinessKey, newVersion, nullableString(in.FlightKey),
		string(in.Kind), in.StatusText, body,
	); err != nil {
		return nil, err
	}

	return &domain.PublishResult{
		BusinessKey:     in.BusinessKey,
		Version:         newVersion,
		Kind:            in.Kind,
		FanOut:          int(fanOut.RowsAffected()),
		PreviousVersion: in.PreviousVersion,
	}, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---------------------------------------------------------------------------
// 公告详情（收敛核对视图）
// ---------------------------------------------------------------------------

// GetAnnouncementDetail 返回公告头、全部版本、以及每个订阅方在每个版本上的投递状态。
func (r *Repo) GetAnnouncementDetail(ctx context.Context, businessKey string) (*domain.AnnouncementDetail, error) {
	detail := &domain.AnnouncementDetail{}
	ann := &detail.Announcement
	var flightKey, latestKind, latestStatus *string
	err := r.pool.QueryRow(ctx, `
		SELECT business_key, flight_key, current_version, latest_kind, latest_status,
		       latest_payload, created_at, updated_at
		FROM announcements WHERE business_key=$1`, businessKey,
	).Scan(&ann.BusinessKey, &flightKey, &ann.CurrentVersion, &latestKind, &latestStatus,
		&ann.LatestPayload, &ann.CreatedAt, &ann.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: announcement", domain.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	if flightKey != nil {
		ann.FlightKey = *flightKey
	}
	if latestKind != nil {
		ann.LatestKind = domain.Kind(*latestKind)
		ann.LatestStatus = *latestStatus
	}

	rows, err := r.pool.Query(ctx, `
		SELECT version, kind, status_text, body, publisher, published_at
		FROM announcement_versions
		WHERE announcement_id=$1 ORDER BY version`, businessKey)
	if err != nil {
		return nil, err
	}
	detail.Versions, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Version, error) {
		var v domain.Version
		var kind string
		if err := row.Scan(&v.Version, &kind, &v.StatusText, &v.Body, &v.Publisher, &v.PublishedAt); err != nil {
			return v, err
		}
		v.Kind = domain.Kind(kind)
		return v, nil
	})
	if err != nil {
		return nil, err
	}

	subRows, err := r.pool.Query(ctx, `
		SELECT s.id, s.name, s.callback_url, s.active,
		       d.version, d.status, d.attempts, d.max_attempts, d.last_error,
		       CASE WHEN d.status IN ('delivered','acked','dead')
		            THEN NULL ELSE d.next_attempt_at END,
		       d.delivered_at, d.acknowledged_at
		FROM subscribers s
		LEFT JOIN outbox_deliveries d
		  ON d.subscriber_id = s.id AND d.announcement_id = $1
		ORDER BY s.created_at, s.id, d.version`, businessKey)
	if err != nil {
		return nil, err
	}
	defer subRows.Close()

	var order []string
	byID := map[string]*domain.SubscriberDeliveries{}
	for subRows.Next() {
		var sid, name, cb string
		var active bool
		var version *int
		var status *domain.DeliveryStatus
		var attempts, maxAttempts *int
		var lastError *string
		var nextAttempt, deliveredAt, ackedAt *time.Time

		if err := subRows.Scan(&sid, &name, &cb, &active,
			&version, &status, &attempts, &maxAttempts, &lastError,
			&nextAttempt, &deliveredAt, &ackedAt); err != nil {
			return nil, err
		}
		sd, ok := byID[sid]
		if !ok {
			sd = &domain.SubscriberDeliveries{SubscriberID: sid, Name: name, CallbackURL: cb, Active: active}
			byID[sid] = sd
			order = append(order, sid)
		}
		if version != nil {
			sd.Deliveries = append(sd.Deliveries, domain.DeliveryItem{
				Version:        *version,
				Status:         *status,
				Attempts:       *attempts,
				MaxAttempts:    *maxAttempts,
				LastError:      deref(lastError),
				NextAttemptAt:  nextAttempt,
				DeliveredAt:    deliveredAt,
				AcknowledgedAt: ackedAt,
			})
		}
	}
	if err := subRows.Err(); err != nil {
		return nil, err
	}
	detail.Subscribers = make([]domain.SubscriberDeliveries, 0, len(order))
	for _, id := range order {
		detail.Subscribers = append(detail.Subscribers, *byID[id])
	}
	return detail, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---------------------------------------------------------------------------
// 发件箱调度：租约领取（含崩溃回收）
// ---------------------------------------------------------------------------

// ClaimDue 领取到期任务：
//   - status 为 pending/retry_wait 且 next_attempt_at 已到；或
//   - status=leased 但租约已过期（投递器进程崩溃 / 被 kill -9 / 重启）——回收重做。
//
// SKIP LOCKED 保证同一行不会被两个运行中的投递器同时领取；workerID 是租约令牌，
// 后续所有状态回写都必须出示令牌，租约易主后旧持有者的回写影响 0 行（ErrLeaseLost），
// “送达”状态因此只会生效一次。
func (r *Repo) ClaimDue(ctx context.Context, batch int, workerID string, lease time.Duration) ([]*domain.Delivery, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH picked AS (
			SELECT id, status AS old_status
			FROM outbox_deliveries
			WHERE (status IN ('pending','retry_wait') AND next_attempt_at <= now())
			   OR (status = 'leased'
			       AND leased_at IS NOT NULL
			       AND leased_at < now() - make_interval(secs => $3))
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE OF outbox_deliveries SKIP LOCKED
		)
		UPDATE outbox_deliveries d
		SET status='leased',
		    leased_by=$2,
		    leased_at=now(),
		    attempts=attempts+1,
		    next_attempt_at=now()+make_interval(secs => $3),
		    last_error=NULL,
		    updated_at=now()
		FROM picked, subscribers s
		WHERE d.id = picked.id AND s.id = d.subscriber_id
		RETURNING
			d.id, d.announcement_id, d.version, d.subscriber_id,
			s.name, s.callback_url, d.payload, d.status, d.attempts, d.max_attempts,
			d.leased_by, d.next_attempt_at, COALESCE(d.last_error,''),
			d.delivered_at, d.acknowledged_at, d.created_at, d.updated_at,
			(picked.old_status = 'leased') AS recovered`,
		batch, workerID, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*domain.Delivery
	for rows.Next() {
		d := &domain.Delivery{}
		var leasedBy *string
		if err := rows.Scan(
			&d.ID, &d.BusinessKey, &d.Version, &d.SubscriberID,
			&d.SubscriberName, &d.CallbackURL, &d.Payload, &d.Status, &d.Attempts, &d.MaxAttempts,
			&leasedBy, &d.NextAttemptAt, &d.LastError,
			&d.DeliveredAt, &d.AckedAt, &d.CreatedAt, &d.UpdatedAt, &d.Recovered,
		); err != nil {
			return nil, err
		}
		if leasedBy != nil {
			d.LeasedBy = *leasedBy
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// MarkDelivered 在订阅方回调返回 2xx 后把任务置为 delivered。
// 仅当前租约持有者可写；租约已易主则返回 ErrLeaseLost，调用方不得计数。
func (r *Repo) MarkDelivered(ctx context.Context, id, workerID string) error {
	ct, err := r.pool.Exec(ctx, `
		UPDATE outbox_deliveries
		SET status='delivered',
		    delivered_at=now(),
		    next_attempt_at=now(),
		    updated_at=now()
		WHERE id=$1 AND status='leased' AND leased_by=$2`, id, workerID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// MarkFailure 处理一次失败投递：attempts 已达上限转 dead，否则进入 retry_wait 分级退避。
// 返回最终状态。租约易主时返回 ErrLeaseLost。
func (r *Repo) MarkFailure(ctx context.Context, id, workerID, errMsg string, nextAttemptAt time.Time) (domain.DeliveryStatus, error) {
	var status domain.DeliveryStatus
	err := r.pool.QueryRow(ctx, `
		UPDATE outbox_deliveries
		SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'retry_wait' END,
		    leased_by=NULL,
		    leased_at=NULL,
		    last_error=$3,
		    next_attempt_at=CASE WHEN attempts >= max_attempts THEN now() ELSE $4 END,
		    updated_at=now()
		WHERE id=$1 AND status='leased' AND leased_by=$2
		RETURNING status`,
		id, workerID, truncateErr(errMsg), nextAttemptAt,
	).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", err
	}
	return status, nil
}

func truncateErr(s string) string {
	const max = 1000
	if len(s) > max {
		return s[:max]
	}
	return s
}

// ---------------------------------------------------------------------------
// 回执
// ---------------------------------------------------------------------------

// RecordReceipt 处理订阅方回执。状态机：
//   - 公告或版本不存在            -> rejected / unknown_version
//   - 版本低于当前收敛版本        -> rejected / obsolete_version（撤销后迟到的确认落此）
//   - 该版本尚未投递成功          -> rejected / not_delivered（杜绝提前完成）
//   - delivered + 版本仍为最新    -> acked
//   - 已 acked 的重复回执         -> acked + duplicate=true
func (r *Repo) RecordReceipt(ctx context.Context, apiKey, businessKey string, version int) (*domain.ReceiptOutcome, string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback(ctx)

	var subscriberID string
	err = tx.QueryRow(ctx, `SELECT id FROM subscribers WHERE api_key=$1`, apiKey).Scan(&subscriberID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ReceiptReasonSubscriber, nil
	}
	if err != nil {
		return nil, "", err
	}

	var currentVersion int
	err = tx.QueryRow(ctx, `
		SELECT current_version FROM announcements WHERE business_key=$1`, businessKey,
	).Scan(&currentVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ReceiptReasonUnknown, nil
	}
	if err != nil {
		return nil, "", err
	}

	var status domain.DeliveryStatus
	err = tx.QueryRow(ctx, `
		SELECT status FROM outbox_deliveries
		WHERE announcement_id=$1 AND subscriber_id=$2 AND version=$3
		FOR UPDATE`,
		businessKey, subscriberID, version,
	).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ReceiptReasonUnknown, nil
	}
	if err != nil {
		return nil, "", err
	}

	if version < currentVersion {
		return nil, domain.ReceiptReasonObsolete, nil
	}

	switch status {
	case domain.StatusAcked:
		// 重复回执：首次结果原样返回，不产生新的状态迁移。
		if err := tx.Commit(ctx); err != nil {
			return nil, "", err
		}
		return &domain.ReceiptOutcome{
			BusinessKey: businessKey, Version: version,
			Status: string(domain.StatusAcked), Duplicate: true,
		}, "", nil
	case domain.StatusDelivered:
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_deliveries
			SET status='acked', acknowledged_at=now(), updated_at=now()
			WHERE announcement_id=$1 AND subscriber_id=$2 AND version=$3`,
			businessKey, subscriberID, version); err != nil {
			return nil, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, "", err
		}
		return &domain.ReceiptOutcome{
			BusinessKey: businessKey, Version: version,
			Status: string(domain.StatusAcked), Duplicate: false,
		}, "", nil
	default:
		// pending / leased / retry_wait / dead 都不允许回执生效。
		return nil, domain.ReceiptReasonUndeliv, nil
	}
}

// ---------------------------------------------------------------------------
// 指标采集
// ---------------------------------------------------------------------------

// DeliveryCounts 按状态统计发件箱行数，供 Prometheus gauge 使用。
func (r *Repo) DeliveryCounts(ctx context.Context) (map[domain.DeliveryStatus]int, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT status, count(*)::int FROM outbox_deliveries GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.DeliveryStatus]int{}
	for rows.Next() {
		var s domain.DeliveryStatus
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}
