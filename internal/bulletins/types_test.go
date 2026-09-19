package bulletins

import (
	"testing"

	"example.com/disruption-bulletins/internal/apperr"
)

func TestValidatePublish(t *testing.T) {
	valid := PublishRequest{
		IdempotencyKey:  "k1",
		BusinessKey:     "CA1234:2026-09-19",
		ExpectedVersion: 0,
		Kind:            KindCancellation,
	}
	if err := validatePublish(&valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*PublishRequest)
	}{
		{"missing idempotency key", func(r *PublishRequest) { r.IdempotencyKey = "" }},
		{"bad business key", func(r *PublishRequest) { r.BusinessKey = "has/slash" }},
		{"empty business key", func(r *PublishRequest) { r.BusinessKey = "" }},
		{"negative version", func(r *PublishRequest) { r.ExpectedVersion = -1 }},
		{"revocation via publish", func(r *PublishRequest) { r.Kind = KindRevocation }},
		{"unknown kind", func(r *PublishRequest) { r.Kind = "delay" }},
		{"invalid payload json", func(r *PublishRequest) { r.Payload = []byte("{nope") }},
	}
	for _, tc := range cases {
		req := valid
		tc.mutate(&req)
		err := validatePublish(&req)
		if err == nil {
			t.Fatalf("%s: expected rejection, got nil", tc.name)
		}
		if err.Code != apperr.CodeInvalidRequest {
			t.Fatalf("%s: expected invalid_request, got %s", tc.name, err.Code)
		}
	}
}

func TestValidateRevoke(t *testing.T) {
	if err := validateRevoke(&RevokeRequest{IdempotencyKey: "r1", ExpectedVersion: 2}); err != nil {
		t.Fatalf("valid revoke rejected: %v", err)
	}
	if err := validateRevoke(&RevokeRequest{ExpectedVersion: 2}); err == nil {
		t.Fatal("missing idempotency key accepted")
	}
	if err := validateRevoke(&RevokeRequest{IdempotencyKey: "r1", ExpectedVersion: -1}); err == nil {
		t.Fatal("negative version accepted")
	}
}
