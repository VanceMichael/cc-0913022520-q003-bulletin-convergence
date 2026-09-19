package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"example.com/disruption-bulletins/internal/api"
	"example.com/disruption-bulletins/internal/dispatcher"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := loadConfig()

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	pool, err := connectWithRetry(ctx, cfg.DatabaseURL, 30, time.Second)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Printf("migrations applied")

	repo := store.New(pool)
	registry := prometheus.NewRegistry()
	metricSet := metrics.New(registry)

	// DB 对账 gauge：周期性以数据库为事实源刷新发件箱状态行数。
	metricsCtx, stopMetrics := context.WithCancel(ctx)
	metricSet.StartGaugeCollector(metricsCtx, repo, cfg.MetricsInterval)

	d := dispatcher.New(repo, metricSet, cfg.Dispatcher)
	dispatchCtx, stopDispatcher := context.WithCancel(ctx)
	stopDispatch := d.RunBackground(dispatchCtx)

	router := api.Router(api.Deps{
		Repo:     repo,
		Metrics:  metricSet,
		Registry: registry,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("listening on :%s (dispatcher=%s)", cfg.Port, d.WorkerID())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining")
	case err := <-serverErr:
		log.Fatalf("http server: %v", err)
	}

	// 先停接收流量，再让在飞回调收尾。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	stopDispatcher()
	stopDispatch()
	stopMetrics()
	log.Printf("shutdown complete")
}

type config struct {
	Port            string
	DatabaseURL     string
	MetricsInterval time.Duration
	Dispatcher      dispatcher.Config
}

func loadConfig() config {
	c := config{
		Port:            getenv("PORT", "8080"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		MetricsInterval: getdur("METRICS_INTERVAL", 5*time.Second),
		Dispatcher:      dispatcher.DefaultConfig(),
	}
	if c.DatabaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	if v := os.Getenv("DISPATCH_POLL_INTERVAL"); v != "" {
		c.Dispatcher.PollInterval = mustDur(v)
	}
	if v := os.Getenv("DISPATCH_BATCH_SIZE"); v != "" {
		c.Dispatcher.BatchSize = mustInt(v)
	}
	if v := os.Getenv("DISPATCH_LEASE_DURATION"); v != "" {
		c.Dispatcher.LeaseDuration = mustDur(v)
	}
	if v := os.Getenv("DISPATCH_HTTP_TIMEOUT"); v != "" {
		c.Dispatcher.HTTPTimeout = mustDur(v)
	}
	if v := os.Getenv("DISPATCH_CONCURRENCY"); v != "" {
		c.Dispatcher.Concurrency = mustInt(v)
	}
	return c
}

func connectWithRetry(ctx context.Context, url string, attempts int, delay time.Duration) (*pgxpool.Pool, error) {
	// 启动时数据库可能尚未就绪（compose 场景），做有限次重试。
	for i := 1; i <= attempts; i++ {
		p, err := store.Open(ctx, url)
		if err == nil {
			if pingErr := p.Ping(ctx); pingErr == nil {
				return p, nil
			} else {
				p.Close()
				err = pingErr
				if i == attempts {
					return nil, err
				}
			}
		}
		log.Printf("waiting for postgres (%d/%d): %v", i, attempts, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, errors.New("postgres unreachable")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		return mustDur(v)
	}
	return def
}

func mustDur(v string) time.Duration {
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid duration %q: %v", v, err)
	}
	return d
}

func mustInt(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Fatalf("invalid positive integer %q: %v", v, err)
	}
	return n
}
