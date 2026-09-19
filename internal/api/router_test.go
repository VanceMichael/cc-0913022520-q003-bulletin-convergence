package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 不依赖数据库：repo 为 nil 时 /health 只报告进程存活。
func TestHealthWithoutDB(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()
	Router(Deps{}).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "disruption-bulletins") {
		t.Fatalf("unexpected health response: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestPublishRequiresIdempotencyKey(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/announcements", strings.NewReader(`{}`))
	recorder := httptest.NewRecorder()
	Router(Deps{}).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest ||
		!strings.Contains(recorder.Body.String(), "Idempotency-Key") {
		t.Fatalf("expected 400 about idempotency key, got %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestReceiptRequiresAPIKey(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/announcements/X/receipts", strings.NewReader(`{"version":1}`))
	recorder := httptest.NewRecorder()
	Router(Deps{}).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", recorder.Code)
	}
}
