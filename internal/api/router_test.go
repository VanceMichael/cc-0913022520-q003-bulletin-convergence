package api

import (
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
)

func TestHealth(t *testing.T) {
    request := httptest.NewRequest(http.MethodGet, "/health", nil)
    recorder := httptest.NewRecorder()
    Router().ServeHTTP(recorder, request)
    if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "disruption-bulletins") {
        t.Fatalf("unexpected health response: %d %s", recorder.Code, recorder.Body.String())
    }
}
