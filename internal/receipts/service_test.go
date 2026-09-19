package receipts

import "testing"

func TestValidate(t *testing.T) {
	valid := Request{ReceiptKey: "rk1", DeliveryID: 7, Subscriber: "airport:PEK", Version: 2}
	if err := validate(&valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	if valid.Verdict != "accepted" {
		t.Fatalf("empty verdict should default to accepted, got %q", valid.Verdict)
	}

	cases := []struct {
		name   string
		mutate func(*Request)
	}{
		{"missing key", func(r *Request) { r.ReceiptKey = "" }},
		{"zero delivery", func(r *Request) { r.DeliveryID = 0 }},
		{"missing subscriber", func(r *Request) { r.Subscriber = "" }},
		{"zero version", func(r *Request) { r.Version = 0 }},
		{"bad verdict", func(r *Request) { r.Verdict = "maybe" }},
	}
	for _, tc := range cases {
		req := valid
		tc.mutate(&req)
		if err := validate(&req); err == nil {
			t.Fatalf("%s: expected rejection, got nil", tc.name)
		}
	}
}
