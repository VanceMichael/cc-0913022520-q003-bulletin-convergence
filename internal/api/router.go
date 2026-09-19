// Package api 组装 HTTP 路由与处理器。错误统一为
// {"error": {"code", "message", "details?}} 的 JSON 形态。
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/common/expfmt"

	"example.com/disruption-bulletins/internal/apperr"
	"example.com/disruption-bulletins/internal/bulletins"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/receipts"
)

const serviceName = "disruption-bulletins"

type Handler struct {
	bulletins *bulletins.Service
	receipts  *receipts.Service
	metrics   *metrics.Metrics
	logger    *slog.Logger
}

func NewHandler(b *bulletins.Service, r *receipts.Service, m *metrics.Metrics, logger *slog.Logger) *Handler {
	return &Handler{bulletins: b, receipts: r, metrics: m, logger: logger}
}

func (h *Handler) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/health", h.health)
	r.Get("/healthz", h.health)
	r.Get("/metrics", h.prometheusMetrics)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/announcements", h.publish)
		r.Get("/announcements/{businessKey}", h.detail)
		r.Post("/announcements/{businessKey}/revocations", h.revoke)
		r.Post("/receipts", h.submitReceipt)
	})
	return r
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": serviceName})
}

// prometheusMetrics 以 Prometheus 文本格式导出指标（等价于 promhttp.HandlerFor，
// 但只依赖 expfmt，避免引入额外的压缩库）。
func (h *Handler) prometheusMetrics(w http.ResponseWriter, r *http.Request) {
	families, err := h.metrics.Handler().Gather()
	if err != nil {
		http.Error(w, `{"error":{"code":"internal","message":"metrics gather failed"}}`, http.StatusInternalServerError)
		return
	}
	contentType := expfmt.Negotiate(r.Header)
	w.Header().Set("Content-Type", string(contentType))
	enc := expfmt.NewEncoder(w, contentType)
	for _, family := range families {
		if err := enc.Encode(family); err != nil {
			h.logger.Error("metrics encode failed", "error", err)
			return
		}
	}
}

// publish 发布取消/恢复公告。201 为新版本，200 为幂等命中（返回首次结果）。
func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	var req bulletins.PublishRequest
	if !decode(w, r, &req) {
		return
	}
	result, duplicate, aerr := h.bulletins.Publish(r.Context(), req)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	if duplicate {
		writeJSON(w, http.StatusOK, result)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

// revoke 撤销公告：只能以更高版本发布，且不能重复撤销。
func (h *Handler) revoke(w http.ResponseWriter, r *http.Request) {
	businessKey := chi.URLParam(r, "businessKey")
	var req bulletins.RevokeRequest
	if !decode(w, r, &req) {
		return
	}
	result, duplicate, aerr := h.bulletins.Revoke(r.Context(), businessKey, req)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	if duplicate {
		writeJSON(w, http.StatusOK, result)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *Handler) detail(w http.ResponseWriter, r *http.Request) {
	businessKey := chi.URLParam(r, "businessKey")
	detail, aerr := h.bulletins.Detail(r.Context(), businessKey)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// submitReceipt 受理订阅方回执。201 为新回执，200 为幂等命中。
func (h *Handler) submitReceipt(w http.ResponseWriter, r *http.Request) {
	var req receipts.Request
	if !decode(w, r, &req) {
		return
	}
	result, duplicate, aerr := h.receipts.Submit(r.Context(), req)
	if aerr != nil {
		writeError(w, aerr)
		return
	}
	if duplicate {
		writeJSON(w, http.StatusOK, result)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, apperr.WithDetails(apperr.CodeInvalidRequest, "request body is not valid JSON for this endpoint",
			map[string]any{"error": err.Error()}))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, aerr *apperr.Error) {
	var target *apperr.Error
	if !errors.As(aerr, &target) {
		target = apperr.New(apperr.CodeInternal, aerr.Error())
	}
	writeJSON(w, apperr.HTTPStatus(target.Code), map[string]any{"error": target})
}
