package retry

import (
	"testing"
	"time"
)

func TestDelayTiers(t *testing.T) {
	tiers := []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 200 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{4, time.Second}, // 层级用完后保持最后一级
		{9, time.Second},
	}
	for _, c := range cases {
		if got := Delay(tiers, c.attempt); got != c.want {
			t.Errorf("Delay(attempt=%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestDelayEmpty(t *testing.T) {
	if got := Delay(nil, 3); got != 0 {
		t.Errorf("Delay(nil) = %v, want 0", got)
	}
}
