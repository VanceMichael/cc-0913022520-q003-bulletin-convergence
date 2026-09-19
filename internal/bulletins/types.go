// Package bulletins 实现公告（取消/恢复/撤销）的发布、版本控制与收敛详情查询。
// 核心不变量：
//   - 发布与发件箱写入在同一事务：公告版本与全部订阅方投递同生共死；
//   - expected_version（前版号）对头表行做乐观并发控制，并发发布只有一个成功；
//   - idempotency_key 唯一：重复请求返回首次结果；
//   - 撤销本身是一个更高版本，且只能作用于未处于 revoked 状态的公告；
//   - 每个新版本都会取代（supersede）尚未被回执确认的旧版本投递。
package bulletins

import (
	"encoding/json"
	"regexp"
	"time"

	"example.com/disruption-bulletins/internal/apperr"
)

const (
	KindCancellation = "cancellation"
	KindRecovery     = "recovery"
	KindRevocation   = "revocation"
)

const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
)

// 投递状态机取值（与数据库 CHECK 约束一致）。
const (
	DeliveryPending    = "pending"
	DeliveryLeased     = "leased"
	DeliverySent       = "sent"
	DeliveryAcked      = "acked"
	DeliverySuperseded = "superseded"
	DeliveryDead       = "dead"
)

type PublishRequest struct {
	IdempotencyKey  string          `json:"idempotency_key"`
	BusinessKey     string          `json:"business_key"`
	ExpectedVersion int64           `json:"expected_version"`
	Kind            string          `json:"kind"`
	Payload         json.RawMessage `json:"payload"`
	Note            string          `json:"note"`
}

type RevokeRequest struct {
	IdempotencyKey  string `json:"idempotency_key"`
	ExpectedVersion int64  `json:"expected_version"`
	Note            string `json:"note"`
}

type DeliveryView struct {
	DeliveryID int64  `json:"delivery_id"`
	Subscriber string `json:"subscriber"`
	Status     string `json:"status"`
}

// PublishResult 是发布/撤销的响应体；幂等命中时返回与首次完全相同的结构。
type PublishResult struct {
	AnnouncementID       int64          `json:"announcement_id"`
	BusinessKey          string         `json:"business_key"`
	Version              int64          `json:"version"`
	Kind                 string         `json:"kind"`
	AnnouncementStatus   string         `json:"announcement_status"`
	IdempotencyKey       string         `json:"idempotency_key"`
	SupersededDeliveries int64          `json:"superseded_deliveries"`
	Deliveries           []DeliveryView `json:"deliveries"`
	CreatedAt            time.Time      `json:"created_at"`
}

type VersionView struct {
	Version        int64     `json:"version"`
	Kind           string    `json:"kind"`
	Note           string    `json:"note"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

type DeliveryDetail struct {
	DeliveryID     int64      `json:"delivery_id"`
	Version        int64      `json:"version"`
	Kind           string     `json:"kind"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	MaxAttempts    int        `json:"max_attempts"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	LeaseOwner     *string    `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	SentAt         *time.Time `json:"sent_at,omitempty"`
	AckedAt        *time.Time `json:"acked_at,omitempty"`
	ReceiptID      *int64     `json:"receipt_id,omitempty"`
}

// SubscriberConvergence 描述一个订阅方对该公告的收敛情况：
// 只有最新版本的投递被回执确认（acked）才算收敛。
type SubscriberConvergence struct {
	Subscriber string           `json:"subscriber"`
	Converged  bool             `json:"converged"`
	Deliveries []DeliveryDetail `json:"deliveries"`
}

type Summary struct {
	Total      int `json:"total"`
	Pending    int `json:"pending"`
	Leased     int `json:"leased"`
	Sent       int `json:"sent"`
	Acked      int `json:"acked"`
	Superseded int `json:"superseded"`
	Dead       int `json:"dead"`
}

type Detail struct {
	BusinessKey    string                  `json:"business_key"`
	Status         string                  `json:"status"`
	CurrentVersion int64                   `json:"current_version"`
	CreatedAt      time.Time               `json:"created_at"`
	UpdatedAt      time.Time               `json:"updated_at"`
	Versions       []VersionView           `json:"versions"`
	Subscribers    []SubscriberConvergence `json:"subscribers"`
	Summary        Summary                 `json:"summary"`
}

var businessKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:._-]{0,199}$`)

// validate 校验发布请求。kind 只允许 cancellation/recovery；撤销走专用端点。
func validatePublish(req *PublishRequest) *apperr.Error {
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		return apperr.New(apperr.CodeInvalidRequest, "idempotency_key is required (1-200 chars)")
	}
	if !businessKeyPattern.MatchString(req.BusinessKey) {
		return apperr.New(apperr.CodeInvalidRequest, "business_key must match ^[A-Za-z0-9][A-Za-z0-9:._-]{0,199}$")
	}
	if req.ExpectedVersion < 0 {
		return apperr.New(apperr.CodeInvalidRequest, "expected_version must be >= 0")
	}
	if req.Kind != KindCancellation && req.Kind != KindRecovery {
		return apperr.WithDetails(apperr.CodeInvalidRequest, "kind must be cancellation or recovery; use the revocations endpoint to revoke",
			map[string]any{"kind": req.Kind})
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return apperr.New(apperr.CodeInvalidRequest, "payload must be valid JSON")
	}
	return nil
}

func validateRevoke(req *RevokeRequest) *apperr.Error {
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 {
		return apperr.New(apperr.CodeInvalidRequest, "idempotency_key is required (1-200 chars)")
	}
	if req.ExpectedVersion < 0 {
		return apperr.New(apperr.CodeInvalidRequest, "expected_version must be >= 0")
	}
	return nil
}
