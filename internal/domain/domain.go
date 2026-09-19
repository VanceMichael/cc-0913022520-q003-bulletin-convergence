// Package domain 定义公告收敛中心的核心类型与状态机，不依赖任何传输或存储细节。
package domain

import (
	"errors"
	"time"
)

// Kind 是公告的业务类型。撤销与恢复都只是同一条公告上的新版本。
type Kind string

const (
	KindCancellation Kind = "cancellation" // 取消
	KindRestoration  Kind = "restoration"  // 恢复
	KindUpdate       Kind = "update"       // 一般变更
)

func (k Kind) Valid() bool {
	switch k {
	case KindCancellation, KindRestoration, KindUpdate:
		return true
	default:
		return false
	}
}

// DeliveryStatus 是发件箱投递记录的生命周期状态。
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"    // 等待首次投递
	StatusLeased    DeliveryStatus = "leased"     // 已被某个投递器租约领取
	StatusRetryWait DeliveryStatus = "retry_wait" // 投递失败，分级退避中
	StatusDelivered DeliveryStatus = "delivered"  // 订阅方回调已返回 2xx
	StatusAcked     DeliveryStatus = "acked"      // 订阅方已显式回执
	StatusDead      DeliveryStatus = "dead"       // 超过最大次数，进入死信
)

// 回执被拒绝的原因，用于指标与错误码。
const (
	ReceiptReasonUnknown    = "unknown_version"
	ReceiptReasonObsolete   = "obsolete_version"
	ReceiptReasonUndeliv    = "not_delivered"
	ReceiptReasonSubscriber = "unknown_subscriber"
)

var (
	// ErrNotFound 业务键或订阅方不存在。
	ErrNotFound = errors.New("not found")
	// ErrVersionConflict 前版号与当前版本不一致（并发发布落败，或试图发布更低版本）。
	ErrVersionConflict = errors.New("version conflict")
	// ErrInvalidInput 参数不合法。
	ErrInvalidInput = errors.New("invalid input")
	// ErrDuplicateRequest 幂等键相同但请求体不同。
	ErrDuplicateRequest = errors.New("idempotency key reused with a different request")
	// ErrRequestInFlight 相同幂等键的前序请求尚未完成。
	ErrRequestInFlight = errors.New("request with same idempotency key is still in flight")
	// ErrReceiptRejected 回执引用了未知 / 过期 / 尚未送达的版本。
	ErrReceiptRejected = errors.New("receipt rejected")
)

// Announcement 是业务键对应的公告头，current_version 始终指向最新收敛版本。
type Announcement struct {
	BusinessKey    string    `json:"business_key"`
	FlightKey      string    `json:"flight_key,omitempty"`
	CurrentVersion int       `json:"current_version"`
	LatestKind     Kind      `json:"latest_kind,omitempty"`
	LatestStatus   string    `json:"latest_status,omitempty"`
	LatestPayload  []byte    `json:"latest_payload,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Version 是只追加的公告版本。
type Version struct {
	Version     int       `json:"version"`
	Kind        Kind      `json:"kind"`
	StatusText  string    `json:"status_text"`
	Body        []byte    `json:"body"`
	Publisher   string    `json:"publisher"`
	PublishedAt time.Time `json:"published_at"`
}

// Subscriber 是公告订阅方。
type Subscriber struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	CallbackURL string    `json:"callback_url"`
	APIKey      string    `json:"api_key,omitempty"`
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"created_at"`
}

// Delivery 是「某订阅方 × 某版本」的唯一投递记录。
type Delivery struct {
	ID             string         `json:"id"`
	BusinessKey    string         `json:"business_key"`
	Version        int            `json:"version"`
	SubscriberID   string         `json:"subscriber_id"`
	SubscriberName string         `json:"subscriber_name"`
	CallbackURL    string         `json:"callback_url"`
	Payload        []byte         `json:"-"`
	Status         DeliveryStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	LeasedBy       string         `json:"leased_by,omitempty"`
	NextAttemptAt  time.Time      `json:"next_attempt_at"`
	LastError      string         `json:"last_error,omitempty"`
	DeliveredAt    *time.Time     `json:"delivered_at,omitempty"`
	AckedAt        *time.Time     `json:"acked_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	// Recovered 不持久化：ClaimDue 返回 true 表示该行来自过期租约回收（前进程崩溃）。
	Recovered bool `json:"-"`
}

// PublishInput 是发布用例的入参。
type PublishInput struct {
	BusinessKey     string
	FlightKey       string
	Kind            Kind
	StatusText      string
	Body            []byte // 已是合法 JSON
	Publisher       string
	PreviousVersion int // 调用方认为的当前版本；新键必须为 0
}

// PublishResult 是发布（或幂等重放后由上层补齐）的结果。
type PublishResult struct {
	BusinessKey     string `json:"business_key"`
	Version         int    `json:"version"`
	Kind            Kind   `json:"kind"`
	FanOut          int    `json:"active_subscribers"`
	PreviousVersion int    `json:"previous_version"`
}

// ReceiptOutcome 是回执处理结果。Duplicate=true 表示记录早已生效，本次没有产生新状态迁移。
type ReceiptOutcome struct {
	BusinessKey string `json:"business_key"`
	Version     int    `json:"version"`
	Status      string `json:"status"`
	Duplicate   bool   `json:"duplicate"`
}

// SubscriberDeliveries 聚合公告详情中单个订阅方在所有版本上的投递记录。
type SubscriberDeliveries struct {
	SubscriberID string         `json:"subscriber_id"`
	Name         string         `json:"name"`
	CallbackURL  string         `json:"callback_url"`
	Active       bool           `json:"active"`
	Deliveries   []DeliveryItem `json:"deliveries"`
}

// DeliveryItem 是公告详情里的单条投递视图。
type DeliveryItem struct {
	Version        int            `json:"version"`
	Status         DeliveryStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	LastError      string         `json:"last_error,omitempty"`
	NextAttemptAt  *time.Time     `json:"next_attempt_at,omitempty"`
	DeliveredAt    *time.Time     `json:"delivered_at,omitempty"`
	AcknowledgedAt *time.Time     `json:"acknowledged_at,omitempty"`
}

// AnnouncementDetail 是公告详情接口的完整收敛视图。
type AnnouncementDetail struct {
	Announcement Announcement
	Versions     []Version
	Subscribers  []SubscriberDeliveries
}
