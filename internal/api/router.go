package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

const serviceName = "disruption-bulletins"

// Deps 是路由装配的外部依赖。
type Deps struct {
	Repo          *store.Repo
	Metrics       *metrics.Set
	Registry      *prometheus.Registry
	EnableWorkers bool // false 时仅挂健康检查（测试中可由外部自行启动投递器）
}

// Router 构造完整 HTTP 路由。
func Router(deps Deps) http.Handler {
	if deps.Metrics == nil {
		deps.Metrics = metrics.New(prometheus.NewRegistry())
	}
	h := NewHandlers(deps.Repo, deps.Metrics)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/health", health(serviceName, deps.Repo))

	r.Route("/v1", func(r chi.Router) {
		r.Post("/subscribers", h.registerSubscriber)
		r.Post("/announcements", h.publish)
		r.Get("/announcements/{businessKey}", h.getAnnouncement)
		r.Post("/announcements/{businessKey}/receipts", h.receipt)
	})

	reg := deps.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	r.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Timeout: 10 * time.Second}))

	return r
}

func health(service string, repo *store.Repo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := "ok"
		dbStatus := "up"
		code := http.StatusOK
		if repo != nil {
			ctx, cancel := contextWithTimeout(r, 2*time.Second)
			defer cancel()
			if err := repo.Pool().Ping(ctx); err != nil {
				status = "degraded"
				dbStatus = "down"
				code = http.StatusServiceUnavailable
			}
		}
		w.WriteHeader(code)
		_ = encodeJSON(w, map[string]string{
			"status":  status,
			"service": service,
			"db":      dbStatus,
		})
	}
}
