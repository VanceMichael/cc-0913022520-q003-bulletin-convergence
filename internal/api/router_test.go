package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealth(t *testing.T) {
	handler := NewHandler(nil, nil, nil, slog.Default())
	for _, path := range []string{"/health", "/healthz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		handler.Router().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "disruption-bulletins") {
			t.Fatalf("unexpected health response for %s: %d %s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestPublishRejectsMalformedJSON(t *testing.T) {
	handler := NewHandler(nil, nil, nil, slog.Default())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/announcements", strings.NewReader("{not json"))
	recorder := httptest.NewRecorder()
	handler.Router().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "invalid_request") {
		t.Fatalf("expected invalid_request code, got %s", recorder.Body.String())
	}
}
