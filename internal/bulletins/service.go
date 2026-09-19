package bulletins

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/disruption-bulletins/internal/apperr"
	"example.com/disruption-bulletins/internal/metrics"
)

type Service struct {
	pool        *pgxpool.Pool
	maxAttempts int
	metrics     *metrics.Metrics
	logger      *slog.Logger
}

func NewService(pool *pgxpool.Pool, maxAttempts int, m *metrics.Metrics, logger *slog.Logger) *Service {
	return &Service{pool: pool, maxAttempts: maxAttempts, metrics: m, logger: logger}
}

// Publish 发布取消/恢复公告。duplicate=true 表示命中幂等键，返回的是首次发布的结果。
func (s *Service) Publish(ctx context.Context, req PublishRequest) (result *PublishResult, duplicate bool, err *apperr.Error) {
	if verr := validatePublish(&req); verr != nil {
		s.metrics.PublishTotal.WithLabelValues(kindLabel(req.Kind), "invalid").Inc()
		return nil, false, verr
	}
	return s.publishWithKind(ctx, &req, req.Kind)
}

// kindLabel 把非法 kind 归一到 "invalid"，避免指标标签基数爆炸。
func kindLabel(kind string) string {
	switch kind {
	case KindCancellation, KindRecovery, KindRevocation:
		return kind
	default:
		return "invalid"
	}
}

// Revoke 撤销公告：本质是一个 kind=revocation 的更高版本，且目标公告不能已处于撤销态。
func (s *Service) Revoke(ctx context.Context, businessKey string, req RevokeRequest) (result *PublishResult, duplicate bool, err *apperr.Error) {
	if verr := validateRevoke(&req); verr != nil {
		s.metrics.PublishTotal.WithLabelValues(KindRevocation, "invalid").Inc()
		return nil, false, verr
	}
	full := PublishRequest{
		IdempotencyKey:  req.IdempotencyKey,
		BusinessKey:     businessKey,
		ExpectedVersion: req.ExpectedVersion,
		Kind:            KindRevocation,
		Payload:         json.RawMessage(`{}`),
		Note:            req.Note,
	}
	return s.publishWithKind(ctx, &full, KindRevocation)
}

// publishWithKind 带重试的发布主流程。并发下唯一约束冲突时回查幂等键：
// 要么拿到并发事务已提交的首次结果，要么重新尝试。
func (s *Service) publishWithKind(ctx context.Context, req *PublishRequest, kind string) (*PublishResult, bool, *apperr.Error) {
	for attempt := 0; attempt < 10; attempt++ {
		result, found, aerr := s.lookupIdempotent(ctx, req)
		if aerr != nil {
			s.metrics.PublishTotal.WithLabelValues(kind, "conflict_idempotency").Inc()
			return nil, false, aerr
		}
		if found {
			s.metrics.PublishTotal.WithLabelValues(kind, "duplicate").Inc()
			return result, true, nil
		}

		result, aerr = s.publishTx(ctx, req, kind)
		if aerr == nil {
			s.metrics.PublishTotal.WithLabelValues(kind, "created").Inc()
			return result, false, nil
		}
		if !errors.Is(aerr, errRetryPublish) {
			switch aerr.Code {
			case apperr.CodeVersionConflict:
				s.metrics.PublishTotal.WithLabelValues(kind, "conflict_version").Inc()
			case apperr.CodeAlreadyRevoked:
				s.metrics.PublishTotal.WithLabelValues(kind, "already_revoked").Inc()
			}
			return nil, false, aerr
		}
		// 与并发事务在唯一约束上相撞：退避后回查幂等键再决定。
		select {
		case <-ctx.Done():
			return nil, false, apperr.New(apperr.CodeInternal, "request canceled")
		case <-time.After(time.Duration(25*(attempt+1)) * time.Millisecond):
		}
	}
	return nil, false, apperr.New(apperr.CodeInternal, "publish did not converge after retries")
}

var errRetryPublish = errors.New("retry publish after unique violation")

// publishTx 在单事务内完成：头表行锁 → 前版号校验 → 版本行插入 →
// 取代未完成旧投递 → 为全部订阅方写入发件箱 → 推进头表版本。
func (s *Service) publishTx(ctx context.Context, req *PublishRequest, kind string) (*PublishResult, *apperr.Error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, internalErr("begin tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO announcements (business_key) VALUES ($1) ON CONFLICT (business_key) DO NOTHING`,
		req.BusinessKey); err != nil {
		return nil, s.classifyWriteErr(err)
	}

	var announcementID, currentVersion int64
	var status string
	err = tx.QueryRow(ctx,
		`SELECT id, current_version, status FROM announcements WHERE business_key = $1 FOR UPDATE`,
		req.BusinessKey).Scan(&announcementID, &currentVersion, &status)
	if err != nil {
		return nil, internalErr("lock announcement", err)
	}

	// 头表行锁串行化了同键发布者：此处能看到的同幂等键版本行必已提交，
	// 交给外层重查并返回首次结果（并发重复请求不能误判为版本冲突）。
	var dupVersionID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM announcement_versions WHERE idempotency_key = $1`,
		req.IdempotencyKey).Scan(&dupVersionID)
	if err == nil {
		return nil, apperr.Wrap(apperr.CodeInternal, "idempotency key committed concurrently", errRetryPublish)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, internalErr("recheck idempotency key", err)
	}

	if currentVersion != req.ExpectedVersion {
		return nil, apperr.WithDetails(apperr.CodeVersionConflict,
			"expected_version does not match the current version",
			map[string]any{"current_version": currentVersion, "expected_version": req.ExpectedVersion})
	}
	if kind == KindRevocation && currentVersion == 0 {
		// 撤销只能作用于已存在的公告；此时头表行是本次事务刚创建的占位行。
		return nil, apperr.WithDetails(apperr.CodeUnknownAnnouncement,
			"no published announcement for this business key",
			map[string]any{"business_key": req.BusinessKey})
	}
	if kind == KindRevocation && status == StatusRevoked {
		return nil, apperr.WithDetails(apperr.CodeAlreadyRevoked,
			"announcement is already revoked; a revocation cannot follow a revocation",
			map[string]any{"current_version": currentVersion})
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	newVersion := currentVersion + 1
	var versionID int64
	var createdAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO announcement_versions (announcement_id, version, kind, payload, note, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at`,
		announcementID, newVersion, kind, payload, req.Note, req.IdempotencyKey).Scan(&versionID, &createdAt)
	if err != nil {
		return nil, s.classifyWriteErr(err)
	}

	// 更高版本（含撤销）出现后，所有未被回执确认的旧投递即刻失效。
	supersedeTag, err := tx.Exec(ctx,
		`UPDATE deliveries
		 SET status = 'superseded', lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
		 WHERE announcement_id = $1 AND status IN ('pending', 'leased', 'sent')`,
		announcementID)
	if err != nil {
		return nil, internalErr("supersede deliveries", err)
	}

	// 事务性发件箱：与版本行同事务为每个活跃订阅方生成投递。
	insertTag, err := tx.Exec(ctx,
		`INSERT INTO deliveries (announcement_id, announcement_version_id, business_key, subscriber_id, version, kind, max_attempts)
		 SELECT $1, $2, $3, s.id, $4, $5, $6
		 FROM subscribers s WHERE s.active`,
		announcementID, versionID, req.BusinessKey, newVersion, kind, s.maxAttempts)
	if err != nil {
		return nil, internalErr("create outbox deliveries", err)
	}

	newStatus := StatusActive
	if kind == KindRevocation {
		newStatus = StatusRevoked
	}
	if _, err := tx.Exec(ctx,
		`UPDATE announcements SET current_version = $1, status = $2, updated_at = now() WHERE id = $3`,
		newVersion, newStatus, announcementID); err != nil {
		return nil, internalErr("advance announcement head", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, s.classifyWriteErr(err)
	}

	s.metrics.DeliveriesCreated.Add(float64(insertTag.RowsAffected()))
	if n := supersedeTag.RowsAffected(); n > 0 {
		s.metrics.DeliveriesSuperseded.Add(float64(n))
	}

	result, aerr := s.loadPublishResult(ctx, versionID)
	if aerr != nil {
		return nil, aerr
	}
	return result, nil
}

// classifyWriteErr 把唯一约束冲突翻译成可重试信号；其余错误按内部错误处理。
func (s *Service) classifyWriteErr(err error) *apperr.Error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return apperr.Wrap(apperr.CodeInternal, "concurrent publish conflict", errRetryPublish)
	}
	return internalErr("write", err)
}

// lookupIdempotent 按幂等键查找首次发布结果；键相同但请求体不同视为幂等冲突。
func (s *Service) lookupIdempotent(ctx context.Context, req *PublishRequest) (*PublishResult, bool, *apperr.Error) {
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	var versionID int64
	var businessKey, kind string
	var samePayload bool
	err := s.pool.QueryRow(ctx,
		`SELECT av.id, a.business_key, av.kind, (av.payload = $2::jsonb)
		 FROM announcement_versions av
		 JOIN announcements a ON a.id = av.announcement_id
		 WHERE av.idempotency_key = $1`,
		req.IdempotencyKey, payload).Scan(&versionID, &businessKey, &kind, &samePayload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, internalErr("lookup idempotency key", err)
	}
	if businessKey != req.BusinessKey || kind != req.Kind || !samePayload {
		return nil, false, apperr.WithDetails(apperr.CodeIdempotencyConflict,
			"idempotency_key was already used with a different request body",
			map[string]any{"idempotency_key": req.IdempotencyKey})
	}
	result, aerr := s.loadPublishResult(ctx, versionID)
	if aerr != nil {
		return nil, false, aerr
	}
	return result, true, nil
}

func (s *Service) loadPublishResult(ctx context.Context, versionID int64) (*PublishResult, *apperr.Error) {
	var res PublishResult
	err := s.pool.QueryRow(ctx,
		`SELECT a.id, a.business_key, a.status, av.version, av.kind, av.idempotency_key, av.created_at
		 FROM announcement_versions av
		 JOIN announcements a ON a.id = av.announcement_id
		 WHERE av.id = $1`, versionID).
		Scan(&res.AnnouncementID, &res.BusinessKey, &res.AnnouncementStatus, &res.Version, &res.Kind, &res.IdempotencyKey, &res.CreatedAt)
	if err != nil {
		return nil, internalErr("load publish result", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT d.id, s.name, d.status
		 FROM deliveries d JOIN subscribers s ON s.id = d.subscriber_id
		 WHERE d.announcement_version_id = $1
		 ORDER BY d.id`, versionID)
	if err != nil {
		return nil, internalErr("load deliveries", err)
	}
	defer rows.Close()
	res.Deliveries = []DeliveryView{}
	for rows.Next() {
		var dv DeliveryView
		if err := rows.Scan(&dv.DeliveryID, &dv.Subscriber, &dv.Status); err != nil {
			return nil, internalErr("scan delivery", err)
		}
		res.Deliveries = append(res.Deliveries, dv)
	}
	return &res, nil
}

// Detail 返回公告的收敛视图：版本历史 + 每个订阅方在各版本上的投递状态。
func (s *Service) Detail(ctx context.Context, businessKey string) (*Detail, *apperr.Error) {
	var d Detail
	err := s.pool.QueryRow(ctx,
		`SELECT business_key, status, current_version, created_at, updated_at, id
		 FROM announcements WHERE business_key = $1`, businessKey).
		Scan(&d.BusinessKey, &d.Status, &d.CurrentVersion, &d.CreatedAt, &d.UpdatedAt, new(int64))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, apperr.WithDetails(apperr.CodeUnknownAnnouncement,
			"no announcement for this business key", map[string]any{"business_key": businessKey})
	}
	if err != nil {
		return nil, internalErr("load announcement", err)
	}

	versionRows, err := s.pool.Query(ctx,
		`SELECT av.version, av.kind, av.note, av.idempotency_key, av.created_at
		 FROM announcement_versions av
		 JOIN announcements a ON a.id = av.announcement_id
		 WHERE a.business_key = $1
		 ORDER BY av.version`, businessKey)
	if err != nil {
		return nil, internalErr("load versions", err)
	}
	d.Versions = []VersionView{}
	for versionRows.Next() {
		var v VersionView
		if err := versionRows.Scan(&v.Version, &v.Kind, &v.Note, &v.IdempotencyKey, &v.CreatedAt); err != nil {
			versionRows.Close()
			return nil, internalErr("scan version", err)
		}
		d.Versions = append(d.Versions, v)
	}
	versionRows.Close()

	deliveryRows, err := s.pool.Query(ctx,
		`SELECT s.name, d.id, d.version, d.kind, d.status, d.attempt_count, d.max_attempts,
		        d.next_attempt_at, d.lease_owner, d.lease_expires_at, d.last_error,
		        d.sent_at, d.acked_at, d.receipt_id
		 FROM deliveries d
		 JOIN subscribers s ON s.id = d.subscriber_id
		 JOIN announcements a ON a.id = d.announcement_id
		 WHERE a.business_key = $1
		 ORDER BY s.name, d.version`, businessKey)
	if err != nil {
		return nil, internalErr("load deliveries", err)
	}
	defer deliveryRows.Close()

	bySubscriber := map[string]*SubscriberConvergence{}
	order := []string{}
	for deliveryRows.Next() {
		var sub string
		var dd DeliveryDetail
		if err := deliveryRows.Scan(&sub, &dd.DeliveryID, &dd.Version, &dd.Kind, &dd.Status,
			&dd.AttemptCount, &dd.MaxAttempts, &dd.NextAttemptAt, &dd.LeaseOwner, &dd.LeaseExpiresAt,
			&dd.LastError, &dd.SentAt, &dd.AckedAt, &dd.ReceiptID); err != nil {
			return nil, internalErr("scan delivery", err)
		}
		sc, ok := bySubscriber[sub]
		if !ok {
			sc = &SubscriberConvergence{Subscriber: sub, Deliveries: []DeliveryDetail{}}
			bySubscriber[sub] = sc
			order = append(order, sub)
		}
		sc.Deliveries = append(sc.Deliveries, dd)
		d.Summary.Total++
		switch dd.Status {
		case DeliveryPending:
			d.Summary.Pending++
		case DeliveryLeased:
			d.Summary.Leased++
		case DeliverySent:
			d.Summary.Sent++
		case DeliveryAcked:
			d.Summary.Acked++
		case DeliverySuperseded:
			d.Summary.Superseded++
		case DeliveryDead:
			d.Summary.Dead++
		}
	}

	d.Subscribers = []SubscriberConvergence{}
	for _, sub := range order {
		sc := bySubscriber[sub]
		for _, dd := range sc.Deliveries {
			if dd.Version == d.CurrentVersion && dd.Status == DeliveryAcked {
				sc.Converged = true
			}
		}
		d.Subscribers = append(d.Subscribers, *sc)
	}
	return &d, nil
}

func internalErr(op string, err error) *apperr.Error {
	return apperr.WithDetails(apperr.CodeInternal, op+" failed", map[string]any{"error": err.Error()})
}
