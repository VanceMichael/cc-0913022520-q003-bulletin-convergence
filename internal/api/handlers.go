package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"example.com/disruption-bulletins/internal/domain"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

// Handlers 装配全部 HTTP 路由的依赖。
type Handlers struct {
	repo        *store.Repo
	metrics     *metrics.Set
	inFlightTTL time.Duration
}

func NewHandlers(repo *store.Repo, m *metrics.Set) *Handlers {
	return &Handlers{repo: repo, metrics: m, inFlightTTL: 30 * time.Second}
}

// ---------------------------------------------------------------------------
// 请求 / 响应结构
// ---------------------------------------------------------------------------

type registerSubscriberRequest struct {
	Name        string `json:"name"`
	CallbackURL string `json:"callback_url"`
	APIKey      string `json:"api_key"`
}

type publishRequest struct {
	BusinessKey     string          `json:"business_key"`
	FlightKey       string          `json:"flight_key,omitempty"`
	Kind            string          `json:"kind"`
	StatusText      string          `json:"status_text"`
	Body            json.RawMessage `json:"body,omitempty"`
	Publisher       string          `json:"publisher,omitempty"`
	PreviousVersion int             `json:"previous_version"`
}

type receiptRequest struct {
	Version int `json:"version"`
}

type versionView struct {
	Version     int             `json:"version"`
	Kind        string          `json:"kind"`
	StatusText  string          `json:"status_text"`
	Body        json.RawMessage `json:"body"`
	Publisher   string          `json:"publisher"`
	PublishedAt time.Time       `json:"published_at"`
}

type announcementResponse struct {
	BusinessKey    string                     `json:"business_key"`
	FlightKey      string                     `json:"flight_key,omitempty"`
	CurrentVersion int                        `json:"current_version"`
	LatestKind     string                     `json:"latest_kind,omitempty"`
	LatestStatus   string                     `json:"latest_status,omitempty"`
	LatestPayload  json.RawMessage            `json:"latest_payload,omitempty"`
	CreatedAt      time.Time                  `json:"created_at"`
	UpdatedAt      time.Time                  `json:"updated_at"`
	Versions       []versionView              `json:"versions"`
	Subscribers    []subscriberDeliveriesView `json:"subscribers"`
}

type subscriberDeliveriesView struct {
	SubscriberID string             `json:"subscriber_id"`
	Name         string             `json:"name"`
	CallbackURL  string             `json:"callback_url"`
	Active       bool               `json:"active"`
	Deliveries   []deliveryItemView `json:"deliveries"`
}

type deliveryItemView struct {
	Version        int        `json:"version"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	MaxAttempts    int        `json:"max_attempts"`
	LastError      string     `json:"last_error,omitempty"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
}

// ---------------------------------------------------------------------------
// 订阅方注册
// ---------------------------------------------------------------------------

func (h *Handlers) registerSubscriber(w http.ResponseWriter, r *http.Request) {
	var req registerSubscriberRequest
	if !decodeEnvelope(w, r, &req) {
		return
	}
	if req.Name == "" || req.CallbackURL == "" || req.APIKey == "" {
		writeError(w, http.StatusBadRequest, "invalid_request",
			"name, callback_url and api_key are required")
		return
	}
	if !isHTTPURL(req.CallbackURL) {
		writeError(w, http.StatusBadRequest, "invalid_request", "callback_url must be http(s) URL")
		return
	}
	s, err := h.repo.CreateSubscriber(r.Context(), req.Name, req.CallbackURL, req.APIKey)
	if err != nil {
		mapDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, s)
}

// ---------------------------------------------------------------------------
// 公告发布（幂等）
// ---------------------------------------------------------------------------

func (h *Handlers) publish(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "invalid_request",
			"Idempotency-Key header is required")
		return
	}

	raw, ok := readBodyForFingerprint(w, r)
	if !ok {
		return
	}
	var req publishRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+sanitizeErr(err))
		return
	}
	if err := validatePublish(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(req.Body) > 0 && !json.Valid(req.Body) {
		writeError(w, http.StatusBadRequest, "invalid_request", "body must be valid JSON")
		return
	}

	fingerprint := sha256Hex(raw)
	outcome, err := h.repo.PublishAnnouncementIdempotent(
		r.Context(), key, fingerprint, h.inFlightTTL, domain.PublishInput{
			BusinessKey:     req.BusinessKey,
			FlightKey:       req.FlightKey,
			Kind:            domain.Kind(req.Kind),
			StatusText:      req.StatusText,
			Body:            req.Body,
			Publisher:       req.Publisher,
			PreviousVersion: req.PreviousVersion,
		})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrVersionConflict):
			h.metrics.PublishConflicts.Inc()
		case errors.Is(err, domain.ErrDuplicateRequest):
			h.metrics.IdempotentRejected.WithLabelValues("reused_different_payload").Inc()
		case errors.Is(err, domain.ErrRequestInFlight):
			h.metrics.IdempotentRejected.WithLabelValues("in_flight").Inc()
		}
		mapDomainError(w, err)
		return
	}

	if outcome.Replay != nil {
		h.metrics.IdempotentReplays.Inc()
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, outcome.Replay.Status, outcome.Replay.Body)
		return
	}

	h.metrics.AnnouncementsPublished.WithLabelValues(req.Kind).Inc()
	writeJSON(w, http.StatusCreated, outcome.Result)
}

func validatePublish(req *publishRequest) error {
	if req.BusinessKey == "" {
		return errors.New("business_key is required")
	}
	if !domain.Kind(req.Kind).Valid() {
		return errors.New("kind must be one of cancellation, restoration, update")
	}
	if req.StatusText == "" {
		return errors.New("status_text is required")
	}
	if req.PreviousVersion < 0 {
		return errors.New("previous_version must be >= 0")
	}
	return nil
}

// ---------------------------------------------------------------------------
// 公告详情（收敛核对视图）
// ---------------------------------------------------------------------------

func (h *Handlers) getAnnouncement(w http.ResponseWriter, r *http.Request) {
	businessKey := chi.URLParam(r, "businessKey")
	detail, err := h.repo.GetAnnouncementDetail(r.Context(), businessKey)
	if err != nil {
		mapDomainError(w, err)
		return
	}

	resp := announcementResponse{
		BusinessKey:    detail.Announcement.BusinessKey,
		FlightKey:      detail.Announcement.FlightKey,
		CurrentVersion: detail.Announcement.CurrentVersion,
		LatestKind:     string(detail.Announcement.LatestKind),
		LatestStatus:   detail.Announcement.LatestStatus,
		LatestPayload:  json.RawMessage(detail.Announcement.LatestPayload),
		CreatedAt:      detail.Announcement.CreatedAt,
		UpdatedAt:      detail.Announcement.UpdatedAt,
		Versions:       make([]versionView, 0, len(detail.Versions)),
		Subscribers:    make([]subscriberDeliveriesView, 0, len(detail.Subscribers)),
	}
	if resp.LatestPayload == nil {
		resp.LatestPayload = json.RawMessage(`{}`)
	}
	for _, v := range detail.Versions {
		resp.Versions = append(resp.Versions, versionView{
			Version:     v.Version,
			Kind:        string(v.Kind),
			StatusText:  v.StatusText,
			Body:        json.RawMessage(v.Body),
			Publisher:   v.Publisher,
			PublishedAt: v.PublishedAt,
		})
	}
	for _, s := range detail.Subscribers {
		view := subscriberDeliveriesView{
			SubscriberID: s.SubscriberID,
			Name:         s.Name,
			CallbackURL:  s.CallbackURL,
			Active:       s.Active,
			Deliveries:   make([]deliveryItemView, 0, len(s.Deliveries)),
		}
		for _, d := range s.Deliveries {
			view.Deliveries = append(view.Deliveries, deliveryItemView{
				Version:        d.Version,
				Status:         string(d.Status),
				Attempts:       d.Attempts,
				MaxAttempts:    d.MaxAttempts,
				LastError:      d.LastError,
				NextAttemptAt:  d.NextAttemptAt,
				DeliveredAt:    d.DeliveredAt,
				AcknowledgedAt: d.AcknowledgedAt,
			})
		}
		resp.Subscribers = append(resp.Subscribers, view)
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// 订阅方回执
// ---------------------------------------------------------------------------

func (h *Handlers) receipt(w http.ResponseWriter, r *http.Request) {
	apiKey := r.Header.Get("X-Api-Key")
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized", "X-Api-Key header is required")
		return
	}
	var req receiptRequest
	if !decodeEnvelope(w, r, &req) {
		return
	}
	if req.Version < 1 {
		writeError(w, http.StatusBadRequest, "invalid_request", "version must be >= 1")
		return
	}

	businessKey := chi.URLParam(r, "businessKey")
	outcome, reason, err := h.repo.RecordReceipt(r.Context(), apiKey, businessKey, req.Version)
	if err != nil {
		mapDomainError(w, err)
		return
	}
	if outcome == nil {
		h.metrics.ReceiptsRejected.WithLabelValues(reason).Inc()
		status := http.StatusConflict
		if reason == domain.ReceiptReasonSubscriber {
			status = http.StatusUnauthorized
		}
		writeReceiptError(w, status, "receipt_rejected", reason)
		return
	}
	h.metrics.AcksRecorded.WithLabelValues(boolLabel(outcome.Duplicate)).Inc()
	writeJSON(w, http.StatusOK, outcome)
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func readBodyForFingerprint(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is required")
		return nil, false
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "cannot read body: "+sanitizeErr(err))
		return nil, false
	}
	if len(raw) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is required")
		return nil, false
	}
	return raw, true
}
