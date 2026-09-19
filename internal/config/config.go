// Package config 从环境变量加载运行参数。所有时间值支持 Go duration 语法（如 "500ms"、"3s"）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Subscriber 订阅方种子配置，来自 SUBSCRIBERS 环境变量（"name=url;name=url"）。
type Subscriber struct {
	Name        string
	CallbackURL string
}

type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	DispatcherOn    bool
	DispatchTick    time.Duration
	DispatchBatch   int
	LeaseTTL        time.Duration
	RetryTiers      []time.Duration // 分级退避：第 n 次失败后等待 RetryTiers[min(n-1, len-1)]
	MaxAttempts     int
	DeliveryTimeout time.Duration
	ShutdownTimeout time.Duration
	Subscribers     []Subscriber
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		DatabaseURL:     env("DATABASE_URL", "postgres://service:service@localhost:5432/bulletins?sslmode=disable"),
		DispatcherOn:    envBool("DISPATCHER_ENABLED", true),
		DispatchTick:    envDuration("DISPATCHER_TICK", 500*time.Millisecond),
		DispatchBatch:   envInt("DISPATCHER_BATCH_SIZE", 50),
		LeaseTTL:        envDuration("DISPATCHER_LEASE_TTL", 30*time.Second),
		MaxAttempts:     envInt("DELIVERY_MAX_ATTEMPTS", 8),
		DeliveryTimeout: envDuration("DELIVERY_HTTP_TIMEOUT", 5*time.Second),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 3*time.Second),
	}
	tiers, err := envDurationList("RETRY_TIER_DELAYS", []time.Duration{
		time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute,
	})
	if err != nil {
		return cfg, err
	}
	cfg.RetryTiers = tiers
	subs, err := parseSubscribers(os.Getenv("SUBSCRIBERS"))
	if err != nil {
		return cfg, err
	}
	cfg.Subscribers = subs
	if cfg.DispatchBatch < 1 {
		return cfg, fmt.Errorf("DISPATCHER_BATCH_SIZE must be >= 1")
	}
	if cfg.MaxAttempts < 1 {
		return cfg, fmt.Errorf("DELIVERY_MAX_ATTEMPTS must be >= 1")
	}
	if len(cfg.RetryTiers) == 0 {
		return cfg, fmt.Errorf("RETRY_TIER_DELAYS must not be empty")
	}
	return cfg, nil
}

func parseSubscribers(raw string) ([]Subscriber, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []Subscriber
	for _, entry := range strings.Split(raw, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, url, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(url) == "" {
			return nil, fmt.Errorf("SUBSCRIBERS entry %q must be name=callback_url", entry)
		}
		out = append(out, Subscriber{Name: strings.TrimSpace(name), CallbackURL: strings.TrimSpace(url)})
	}
	return out, nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envDurationList(key string, fallback []time.Duration) ([]time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	var out []time.Duration
	for _, part := range strings.Split(v, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out = append(out, d)
	}
	return out, nil
}
