// subscriber-sim 是集成环境的模拟订阅方（机场/航司/地服）。
// 它接收投递器 webhook，并按当前模式行动：
//
//	auto  立即 200 并异步回执（默认）
//	slow  延迟 SIM_SLOW_DELAY 后 200 并回执（用于制造“投递途中进程被杀”的窗口）
//	hold  200 但暂不回执（用于制造“撤销后迟到确认”）
//	fail  永远 500（用于分级重试与死信测试）
//
// 控制面：POST /sim/control {subscriber, mode}；POST /sim/drain 补发所有暂存回执；
// GET /sim/received 查询已收 webhook（含每条投递的接收次数，用于断言重投）。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type webhook struct {
	DeliveryID  int64           `json:"delivery_id"`
	BusinessKey string          `json:"business_key"`
	Version     int64           `json:"version"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Note        string          `json:"note"`
}

type receivedRecord struct {
	DeliveryID  int64     `json:"delivery_id"`
	Subscriber  string    `json:"subscriber"`
	BusinessKey string    `json:"business_key"`
	Version     int64     `json:"version"`
	Kind        string    `json:"kind"`
	Count       int       `json:"count"`
	FirstAt     time.Time `json:"first_at"`
	LastAt      time.Time `json:"last_at"`
}

type drainOutcome struct {
	DeliveryID int64  `json:"delivery_id"`
	HTTPStatus int    `json:"http_status"`
	Code       string `json:"code"`
}

type sim struct {
	mu        sync.Mutex
	modes     map[string]string
	received  map[int64]*receivedRecord
	held      map[int64]webhook
	heldSub   map[int64]string
	appURL    string
	slowDelay time.Duration
	client    *http.Client
	logger    *slog.Logger
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	appURL := env("APP_BASE_URL", "http://localhost:8080")
	addr := env("HTTP_ADDR", ":9000")
	slowDelay, err := time.ParseDuration(env("SIM_SLOW_DELAY", "3s"))
	if err != nil {
		slowDelay = 3 * time.Second
	}

	s := &sim{
		modes:     map[string]string{},
		received:  map[int64]*receivedRecord{},
		held:      map[int64]webhook{},
		heldSub:   map[int64]string{},
		appURL:    appURL,
		slowDelay: slowDelay,
		client:    &http.Client{Timeout: 5 * time.Second},
		logger:    logger,
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"subscriber-sim"}`))
	})
	r.Post("/hook/{name}", s.hook)
	r.Get("/sim/received", s.listReceived)
	r.Post("/sim/control", s.control)
	r.Post("/sim/drain", s.drain)
	r.Post("/sim/reset", s.reset)

	logger.Info("subscriber-sim listening", "addr", addr, "app_base_url", appURL, "slow_delay", slowDelay)
	if err := http.ListenAndServe(addr, r); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func (s *sim) hook(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var wh webhook
	if err := json.NewDecoder(r.Body).Decode(&wh); err != nil {
		http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if rec, ok := s.received[wh.DeliveryID]; ok {
		rec.Count++
		rec.LastAt = time.Now().UTC()
	} else {
		s.received[wh.DeliveryID] = &receivedRecord{
			DeliveryID: wh.DeliveryID, Subscriber: name, BusinessKey: wh.BusinessKey,
			Version: wh.Version, Kind: wh.Kind, Count: 1,
			FirstAt: time.Now().UTC(), LastAt: time.Now().UTC(),
		}
	}
	mode := s.modes[name]
	if mode == "" {
		mode = "auto"
	}
	if mode == "hold" {
		s.held[wh.DeliveryID] = wh
		s.heldSub[wh.DeliveryID] = name
	}
	s.mu.Unlock()

	s.logger.Info("webhook received", "subscriber", name, "delivery_id", wh.DeliveryID,
		"version", wh.Version, "kind", wh.Kind, "mode", mode)

	switch mode {
	case "fail":
		http.Error(w, `{"error":"simulated subscriber failure"}`, http.StatusInternalServerError)
	case "slow":
		time.Sleep(s.slowDelay)
		writeOK(w)
		go s.receiptWithRetry(name, wh)
	case "hold":
		writeOK(w)
	default: // auto
		writeOK(w)
		go s.receiptWithRetry(name, wh)
	}
}

// receiptWithRetry 向收敛中心发送回执。not_delivered 说明投递器尚未落库 sent，
// 传输错误说明对端可能在重启——两者都可重试；其余 4xx 是终态拒绝，停止。
func (s *sim) receiptWithRetry(subscriber string, wh webhook) {
	body, _ := json.Marshal(map[string]any{
		"receipt_key": fmt.Sprintf("sim-%d", wh.DeliveryID),
		"delivery_id": wh.DeliveryID,
		"subscriber":  subscriber,
		"version":     wh.Version,
		"verdict":     "accepted",
	})
	for attempt := 0; attempt < 40; attempt++ {
		status, code := s.postReceipt(body)
		switch {
		case status >= 200 && status < 300:
			s.logger.Info("receipt accepted", "delivery_id", wh.DeliveryID, "status", status)
			return
		case status == http.StatusConflict && code == "not_delivered":
			// 投递器还没把 sent 落库，稍等重试
		case status >= 400 && status < 500:
			s.logger.Info("receipt rejected permanently", "delivery_id", wh.DeliveryID, "status", status, "code", code)
			return
		default:
			// 网络错误或 5xx：对端可能在重启
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.logger.Warn("receipt gave up", "delivery_id", wh.DeliveryID)
}

// postReceipt 返回 (HTTP 状态码, 错误码)。网络错误时状态码为 0。
func (s *sim) postReceipt(body []byte) (int, string) {
	resp, err := s.client.Post(s.appURL+"/api/v1/receipts", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, ""
	}
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&errBody)
	return resp.StatusCode, errBody.Error.Code
}

func (s *sim) listReceived(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]receivedRecord, 0, len(s.received))
	for _, rec := range s.received {
		out = append(out, *rec)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].DeliveryID < out[j].DeliveryID })
	writeJSON(w, http.StatusOK, map[string]any{"received": out})
}

func (s *sim) control(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Subscriber string `json:"subscriber"`
		Mode       string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
		return
	}
	switch req.Mode {
	case "auto", "hold", "slow", "fail":
	default:
		http.Error(w, `{"error":"mode must be auto|hold|slow|fail"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.modes[req.Subscriber] = req.Mode
	s.mu.Unlock()
	s.logger.Info("mode set", "subscriber", req.Subscriber, "mode", req.Mode)
	writeJSON(w, http.StatusOK, map[string]string{"subscriber": req.Subscriber, "mode": req.Mode})
}

// drain 补发所有暂存（hold）回执，返回每条的结果——迟到确认会在这里被 stale_version 拒绝。
func (s *sim) drain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Subscriber string `json:"subscriber"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	s.mu.Lock()
	type pending struct {
		sub string
		wh  webhook
	}
	var batch []pending
	for id, wh := range s.held {
		sub := s.heldSub[id]
		if req.Subscriber == "" || req.Subscriber == sub {
			batch = append(batch, pending{sub, wh})
		}
	}
	s.mu.Unlock()

	outcomes := []drainOutcome{}
	for _, p := range batch {
		body, _ := json.Marshal(map[string]any{
			"receipt_key": fmt.Sprintf("sim-%d", p.wh.DeliveryID),
			"delivery_id": p.wh.DeliveryID,
			"subscriber":  p.sub,
			"version":     p.wh.Version,
			"verdict":     "accepted",
		})
		var httpStatus int
		var code string
		for attempt := 0; attempt < 20; attempt++ {
			st, c := s.postReceipt(body)
			httpStatus, code = st, c
			if st == http.StatusConflict && c == "not_delivered" {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			break
		}
		outcomes = append(outcomes, drainOutcome{DeliveryID: p.wh.DeliveryID, HTTPStatus: httpStatus, Code: code})
		if httpStatus >= 200 && httpStatus < 300 || httpStatus == http.StatusConflict && code != "not_delivered" {
			s.mu.Lock()
			delete(s.held, p.wh.DeliveryID)
			delete(s.heldSub, p.wh.DeliveryID)
			s.mu.Unlock()
		}
	}
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].DeliveryID < outcomes[j].DeliveryID })
	writeJSON(w, http.StatusOK, map[string]any{"outcomes": outcomes})
}

func (s *sim) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.received = map[int64]*receivedRecord{}
	s.held = map[int64]webhook{}
	s.heldSub = map[int64]string{}
	s.modes = map[string]string{}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"received":true}`))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
