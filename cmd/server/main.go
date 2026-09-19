// 服务入口：装配配置、存储、迁移、订阅方种子、指标、HTTP 服务与后台投递器。
// 关闭顺序：先停投递器（在途投递放弃，租约留给存活实例/重启后的自己），
// 再优雅关闭 HTTP，最后关闭连接池。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/disruption-bulletins/internal/api"
	"example.com/disruption-bulletins/internal/bulletins"
	"example.com/disruption-bulletins/internal/config"
	"example.com/disruption-bulletins/internal/dispatcher"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/receipts"
	"example.com/disruption-bulletins/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := seedSubscribers(ctx, pool, cfg.Subscribers, logger); err != nil {
		return fmt.Errorf("seed subscribers: %w", err)
	}

	m := metrics.New(pool)
	bulletinSvc := bulletins.NewService(pool, cfg.MaxAttempts, m, logger)
	receiptSvc := receipts.NewService(pool, m)
	handler := api.NewHandler(bulletinSvc, receiptSvc, m, logger)

	// 后台投递器：独立 ctx，先于 HTTP 停止。
	dispatcherCtx, stopDispatcher := context.WithCancel(context.Background())
	dispatcherDone := make(chan struct{})
	if cfg.DispatcherOn {
		workerID := fmt.Sprintf("%s-%d-%d", hostname(), os.Getpid(), time.Now().UnixNano())
		d := dispatcher.New(pool, dispatcher.Config{
			Tick:        cfg.DispatchTick,
			BatchSize:   cfg.DispatchBatch,
			LeaseTTL:    cfg.LeaseTTL,
			RetryTiers:  cfg.RetryTiers,
			SendTimeout: cfg.DeliveryTimeout,
		}, workerID, m, logger)
		go func() {
			defer close(dispatcherDone)
			d.Run(dispatcherCtx)
		}()
		logger.Info("dispatcher started",
			"tick", cfg.DispatchTick, "lease_ttl", cfg.LeaseTTL, "retry_tiers", cfg.RetryTiers)
	} else {
		close(dispatcherDone)
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		stopDispatcher()
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	// 1) 停投递器：在途发送被 ctx 取消，租约到期后由下次启动续跑。
	stopDispatcher()
	select {
	case <-dispatcherDone:
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("dispatcher drain timed out")
	}

	// 2) 优雅关闭 HTTP。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}

func seedSubscribers(ctx context.Context, pool *pgxpool.Pool, subs []config.Subscriber, logger *slog.Logger) error {
	for _, sub := range subs {
		if _, err := pool.Exec(ctx,
			`INSERT INTO subscribers (name, callback_url) VALUES ($1, $2)
			 ON CONFLICT (name) DO UPDATE SET callback_url = EXCLUDED.callback_url, active = TRUE`,
			sub.Name, sub.CallbackURL); err != nil {
			return fmt.Errorf("upsert subscriber %s: %w", sub.Name, err)
		}
		logger.Info("subscriber registered", "name", sub.Name, "callback_url", sub.CallbackURL)
	}
	return nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "unknown"
	}
	return name
}
