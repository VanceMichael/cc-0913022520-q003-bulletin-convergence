package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"example.com/disruption-bulletins/internal/domain"
)

func encodeJSON(w http.ResponseWriter, v any) error {
	return json.NewEncoder(w).Encode(v)
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

// isHTTPURL 粗略校验回调地址必须是 http/https 绝对 URL。
func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// envelope 是所有错误响应的统一外壳。
type envelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, envelope{Error: errorBody{Code: code, Message: message}})
}

func writeReceiptError(w http.ResponseWriter, status int, code, reason string) {
	writeJSON(w, status, envelope{Error: errorBody{Code: code, Message: "receipt rejected", Reason: reason}})
}

// decodeEnvelope 解析请求体：拒绝空体、非法 JSON 与未知字段。
func decodeEnvelope(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is required")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+sanitizeErr(err))
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain a single JSON object")
		return false
	}
	return true
}

func sanitizeErr(err error) string {
	msg := err.Error()
	msg = strings.ReplaceAll(msg, "\n", " ")
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

// mapDomainError 把仓储错误翻译成 HTTP 状态码与错误码。
func mapDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, domain.ErrVersionConflict):
		writeError(w, http.StatusConflict, "version_conflict", err.Error())
	case errors.Is(err, domain.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, domain.ErrDuplicateRequest):
		writeError(w, http.StatusConflict, "idempotency_conflict", err.Error())
	case errors.Is(err, domain.ErrRequestInFlight):
		writeError(w, http.StatusConflict, "request_in_flight", err.Error())
	default:
		log.Printf("api: unhandled error: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}
