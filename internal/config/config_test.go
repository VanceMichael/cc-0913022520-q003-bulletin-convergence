package config

import "testing"

func TestParseSubscribers(t *testing.T) {
	subs, err := parseSubscribers("airport:PEK=http://sim:9000/hook/airport:PEK;airline:CA=http://sim:9000/hook/airline:CA")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(subs) != 2 {
		t.Fatalf("expected 2 subscribers, got %d", len(subs))
	}
	if subs[0].Name != "airport:PEK" || subs[0].CallbackURL != "http://sim:9000/hook/airport:PEK" {
		t.Fatalf("unexpected first subscriber: %+v", subs[0])
	}
	if subs[1].Name != "airline:CA" {
		t.Fatalf("unexpected second subscriber: %+v", subs[1])
	}

	if subs, err := parseSubscribers("  "); err != nil || subs != nil {
		t.Fatalf("empty input should yield nil, got %+v, %v", subs, err)
	}
	if _, err := parseSubscribers("no-equals-sign"); err == nil {
		t.Fatal("malformed entry accepted")
	}
	if _, err := parseSubscribers("name="); err == nil {
		t.Fatal("empty url accepted")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SUBSCRIBERS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || !cfg.DispatcherOn || cfg.MaxAttempts != 8 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if len(cfg.RetryTiers) == 0 {
		t.Fatal("default retry tiers must not be empty")
	}
}

func TestLoadParsesEnv(t *testing.T) {
	t.Setenv("DISPATCHER_TICK", "250ms")
	t.Setenv("DISPATCHER_BATCH_SIZE", "10")
	t.Setenv("RETRY_TIER_DELAYS", "100ms,1s,2s")
	t.Setenv("DELIVERY_MAX_ATTEMPTS", "3")
	t.Setenv("SUBSCRIBERS", "a=http://x/1")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.DispatchTick.Milliseconds() != 250 || cfg.DispatchBatch != 10 || cfg.MaxAttempts != 3 {
		t.Fatalf("env not applied: %+v", cfg)
	}
	if len(cfg.RetryTiers) != 3 || cfg.RetryTiers[1].Seconds() != 1 {
		t.Fatalf("retry tiers not parsed: %v", cfg.RetryTiers)
	}
	if len(cfg.Subscribers) != 1 || cfg.Subscribers[0].Name != "a" {
		t.Fatalf("subscribers not parsed: %+v", cfg.Subscribers)
	}
}
