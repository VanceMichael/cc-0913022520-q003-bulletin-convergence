// Package receipts 实现订阅方回执的受理。回执是订阅记录“生效”的唯一路径，
// 因此校验必须严格：未知投递、过期（已被更高版本取代）版本、未投递的投递、
// 订阅方不符一律拒绝；重复回执按幂等键返回首次结果，不产生二次生效。
package receipts

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/disruption-bulletins/internal/apperr"
	"example.com/disruption-bulletins/internal/bulletins"
	"example.com/disruption-bulletins/internal/metrics"
)

type Request struct {
	ReceiptKey string `json:"receipt_key"`
	DeliveryID int64  `json:"delivery_id"`
	Subscriber string `json:"subscriber"`
	Version    int64  `json:"version"`
	Verdict    string `json:"verdict"`
	Note       string `json:"note"`
}

type Result struct {
	ReceiptID  int64     `json:"receipt_id"`
	DeliveryID int64     `json:"delivery_id"`
	Subscriber string    `json:"subscriber"`
	Version    int64     `json:"version"`
	Status     string    `json:"status"`
	Duplicate  bool      `json:"duplicate"`
	CreatedAt  time.Time `json:"created_at"`
}

type Service struct {
	pool    *pgxpool.Pool
	metrics *metrics.Metrics
}

func NewService(pool *pgxpool.Pool, m *metrics.Metrics) *Service {
	return &Service{pool: pool, metrics: m}
}

// Submit 受理一条回执。duplicate=true 表示幂等命中，返回首次受理的结果。
func (s *Service) Submit(ctx context.Context, req Request) (*Result, bool, *apperr.Error) {
	if verr := validate(&req); verr != nil {
		s.metrics.ReceiptsTotal.WithLabelValues("rejected_invalid").Inc()
		return nil, false, verr
	}
	for attempt := 0; attempt < 10; attempt++ {
		result, found, aerr := s.lookupByKey(ctx, req.ReceiptKey, req.DeliveryID)
		if aerr != nil {
			s.metrics.ReceiptsTotal.WithLabelValues("rejected_key_conflict").Inc()
			return nil, false, aerr
		}
		if found {
			s.metrics.ReceiptsTotal.WithLabelValues("duplicate").Inc()
			return result, true, nil
		}

		result, aerr = s.submitTx(ctx, req)
		if aerr == nil {
			s.metrics.ReceiptsTotal.WithLabelValues("accepted").Inc()
			return result, false, nil
		}
		if !errors.Is(aerr, errRetryReceipt) {
			s.metrics.ReceiptsTotal.WithLabelValues(metricResult(aerr.Code)).Inc()
			return nil, false, aerr
		}
		select {
		case <-ctx.Done():
			return nil, false, apperr.New(apperr.CodeInternal, "request canceled")
		case <-time.After(time.Duration(25*(attempt+1)) * time.Millisecond):
		}
	}
	return nil, false, apperr.New(apperr.CodeInternal, "receipt did not converge after retries")
}

var errRetryReceipt = errors.New("retry receipt after unique violation")

func metricResult(code string) string {
	switch code {
	case apperr.CodeUnknownDelivery:
		return "rejected_unknown_delivery"
	case apperr.CodeStaleVersion:
		return "rejected_stale_version"
	case apperr.CodeVersionMismatch:
		return "rejected_version_mismatch"
	case apperr.CodeSubscriberMismatch:
		return "rejected_subscriber_mismatch"
	case apperr.CodeNotDelivered:
		return "rejected_not_delivered"
	case apperr.CodeAlreadyAcknowledged:
		return "rejected_already_acknowledged"
	default:
		return "rejected_invalid"
	}
}

// submitTx 在单事务内校验并落库：行锁保证与并发撤销/回执的串行化。
func (s *Service) submitTx(ctx context.Context, req Request) (*Result, *apperr.Error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, internalErr("begin tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		status          string
		deliveryVersion int64
		subscriberID    int64
		subscriberName  string
		announcementID  int64
	)
	err = tx.QueryRow(ctx,
		`SELECT d.status, d.version, d.subscriber_id, s.name, d.announcement_id
		 FROM deliveries d JOIN subscribers s ON s.id = d.subscriber_id
		 WHERE d.id = $1
		 FOR UPDATE OF d`, req.DeliveryID).
		Scan(&status, &deliveryVersion, &subscriberID, &subscriberName, &announcementID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, apperr.WithDetails(apperr.CodeUnknownDelivery,
			"delivery does not exist", map[string]any{"delivery_id": req.DeliveryID})
	}
	if err != nil {
		return nil, internalErr("lock delivery", err)
	}

	// 投递行锁串行化了同投递的回执：此处能看到的同键回执必已提交，
	// 交给外层重查并返回首次结果（并发重复回执不能误判为重复生效）。
	var dupReceiptID int64
	err = tx.QueryRow(ctx, `SELECT id FROM receipts WHERE receipt_key = $1`, req.ReceiptKey).Scan(&dupReceiptID)
	if err == nil {
		return nil, apperr.Wrap(apperr.CodeInternal, "receipt key committed concurrently", errRetryReceipt)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, internalErr("recheck receipt key", err)
	}

	if subscriberName != req.Subscriber {
		return nil, apperr.WithDetails(apperr.CodeSubscriberMismatch,
			"delivery belongs to a different subscriber",
			map[string]any{"delivery_subscriber": subscriberName})
	}
	if status == bulletins.DeliverySuperseded {
		var currentVersion int64
		_ = tx.QueryRow(ctx, `SELECT current_version FROM announcements WHERE id = $1`, announcementID).Scan(&currentVersion)
		return nil, apperr.WithDetails(apperr.CodeStaleVersion,
			"delivery was superseded by a higher version (e.g. a revocation); its version is expired",
			map[string]any{"delivery_version": deliveryVersion, "current_version": currentVersion})
	}
	if deliveryVersion != req.Version {
		return nil, apperr.WithDetails(apperr.CodeVersionMismatch,
			"receipt version does not match the delivery version",
			map[string]any{"delivery_version": deliveryVersion, "receipt_version": req.Version})
	}
	if status == bulletins.DeliveryAcked {
		return nil, apperr.WithDetails(apperr.CodeAlreadyAcknowledged,
			"delivery is already acknowledged; a second receipt must not take effect",
			map[string]any{"delivery_id": req.DeliveryID})
	}
	if status != bulletins.DeliverySent {
		return nil, apperr.WithDetails(apperr.CodeNotDelivered,
			"delivery has not been sent yet; a receipt cannot complete it early",
			map[string]any{"delivery_status": status})
	}

	var receiptID int64
	var createdAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO receipts (receipt_key, delivery_id, subscriber_id, version, verdict, note)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at`,
		req.ReceiptKey, req.DeliveryID, subscriberID, req.Version, req.Verdict, req.Note).
		Scan(&receiptID, &createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, apperr.Wrap(apperr.CodeInternal, "concurrent receipt conflict", errRetryReceipt)
		}
		return nil, internalErr("insert receipt", err)
	}

	// 行锁已持有，此处必然成功；守卫条件只是最后一道防线。
	tag, err := tx.Exec(ctx,
		`UPDATE deliveries
		 SET status = 'acked', acked_at = now(), receipt_id = $1, updated_at = now()
		 WHERE id = $2 AND status = 'sent'`, receiptID, req.DeliveryID)
	if err != nil {
		return nil, internalErr("mark delivery acked", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, apperr.New(apperr.CodeInternal, "delivery state changed during receipt")
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, internalErr("commit", err)
	}
	return &Result{
		ReceiptID:  receiptID,
		DeliveryID: req.DeliveryID,
		Subscriber: req.Subscriber,
		Version:    req.Version,
		Status:     bulletins.DeliveryAcked,
		CreatedAt:  createdAt,
	}, nil
}

// lookupByKey 幂等回查：同一 receipt_key 返回首次结果；键相同但投递不同视为冲突。
func (s *Service) lookupByKey(ctx context.Context, key string, deliveryID int64) (*Result, bool, *apperr.Error) {
	var (
		res          Result
		subscriberID int64
	)
	err := s.pool.QueryRow(ctx,
		`SELECT r.id, r.delivery_id, r.subscriber_id, r.version, r.created_at
		 FROM receipts r WHERE r.receipt_key = $1`, key).
		Scan(&res.ReceiptID, &res.DeliveryID, &subscriberID, &res.Version, &res.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, internalErr("lookup receipt key", err)
	}
	if res.DeliveryID != deliveryID {
		return nil, false, apperr.WithDetails(apperr.CodeReceiptKeyConflict,
			"receipt_key was already used for a different delivery",
			map[string]any{"receipt_key": key})
	}
	var name string
	if err := s.pool.QueryRow(ctx, `SELECT name FROM subscribers WHERE id = $1`, subscriberID).Scan(&name); err != nil {
		return nil, false, internalErr("lookup subscriber", err)
	}
	res.Subscriber = name
	res.Status = bulletins.DeliveryAcked
	res.Duplicate = true
	return &res, true, nil
}

func validate(req *Request) *apperr.Error {
	if req.ReceiptKey == "" || len(req.ReceiptKey) > 200 {
		return apperr.New(apperr.CodeInvalidRequest, "receipt_key is required (1-200 chars)")
	}
	if req.DeliveryID <= 0 {
		return apperr.New(apperr.CodeInvalidRequest, "delivery_id must be a positive integer")
	}
	if req.Subscriber == "" {
		return apperr.New(apperr.CodeInvalidRequest, "subscriber is required")
	}
	if req.Version <= 0 {
		return apperr.New(apperr.CodeInvalidRequest, "version must be a positive integer")
	}
	if req.Verdict == "" {
		req.Verdict = "accepted"
	}
	if req.Verdict != "accepted" {
		return apperr.WithDetails(apperr.CodeInvalidRequest, "verdict must be accepted",
			map[string]any{"verdict": req.Verdict})
	}
	return nil
}

func internalErr(op string, err error) *apperr.Error {
	return apperr.WithDetails(apperr.CodeInternal, op+" failed", map[string]any{"error": err.Error()})
}
