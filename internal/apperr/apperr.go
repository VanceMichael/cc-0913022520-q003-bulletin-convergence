// Package apperr 定义跨层共享的业务错误。Code 是稳定的机器可读标识，
// HTTP 层据此映射状态码，订阅方与集成测试据此断言拒绝原因。
package apperr

import "net/http"

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	cause   error
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Unwrap 暴露内部原因，供 errors.Is 识别可重试信号（如并发唯一约束冲突）。
func (e *Error) Unwrap() error { return e.cause }

func New(code, message string) *Error { return &Error{Code: code, Message: message} }

// Wrap 构造携带内部原因的错误；原因不会序列化到响应体。
func Wrap(code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

func WithDetails(code, message string, details map[string]any) *Error {
	return &Error{Code: code, Message: message, Details: details}
}

const (
	CodeInvalidRequest      = "invalid_request"
	CodeUnknownAnnouncement = "unknown_announcement"
	CodeUnknownDelivery     = "unknown_delivery"
	CodeVersionConflict     = "version_conflict"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeAlreadyRevoked      = "already_revoked"
	CodeStaleVersion        = "stale_version"
	CodeVersionMismatch     = "version_mismatch"
	CodeSubscriberMismatch  = "subscriber_mismatch"
	CodeNotDelivered        = "not_delivered"
	CodeAlreadyAcknowledged = "already_acknowledged"
	CodeReceiptKeyConflict  = "receipt_key_conflict"
	CodeInternal            = "internal"
)

// HTTPStatus 将业务码映射为 HTTP 状态码。
func HTTPStatus(code string) int {
	switch code {
	case CodeInvalidRequest:
		return http.StatusBadRequest
	case CodeUnknownAnnouncement, CodeUnknownDelivery:
		return http.StatusNotFound
	case CodeVersionConflict, CodeIdempotencyConflict, CodeAlreadyRevoked,
		CodeStaleVersion, CodeVersionMismatch, CodeSubscriberMismatch,
		CodeNotDelivered, CodeAlreadyAcknowledged, CodeReceiptKeyConflict:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
