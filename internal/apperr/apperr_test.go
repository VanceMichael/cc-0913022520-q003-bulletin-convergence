package apperr

import (
	"errors"
	"net/http"
	"testing"
)

func TestHTTPStatus(t *testing.T) {
	cases := map[string]int{
		CodeInvalidRequest:      http.StatusBadRequest,
		CodeUnknownAnnouncement: http.StatusNotFound,
		CodeUnknownDelivery:     http.StatusNotFound,
		CodeVersionConflict:     http.StatusConflict,
		CodeStaleVersion:        http.StatusConflict,
		CodeAlreadyAcknowledged: http.StatusConflict,
		CodeInternal:            http.StatusInternalServerError,
		"unlisted":              http.StatusInternalServerError,
	}
	for code, want := range cases {
		if got := HTTPStatus(code); got != want {
			t.Errorf("HTTPStatus(%s) = %d, want %d", code, got, want)
		}
	}
}

func TestWrapUnwrap(t *testing.T) {
	cause := errors.New("retry me")
	err := Wrap(CodeInternal, "wrapper", cause)
	if !errors.Is(err, cause) {
		t.Fatal("wrapped cause not visible via errors.Is")
	}
	if err.Error() == "" {
		t.Fatal("empty error string")
	}
}
