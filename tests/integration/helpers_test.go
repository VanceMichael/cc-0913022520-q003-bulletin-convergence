//go:build integration

// 集成测试以纯黑盒方式运行：只通过 app 的 HTTP API、subscriber-sim 的控制面
// 和 Prometheus 指标断言系统行为，不触碰数据库。
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

var (
	appBaseURL   = envOr("APP_BASE_URL", "http://localhost:8080")
	simBaseURL   = envOr("SIM_BASE_URL", "http://localhost:9000")
	appContainer = os.Getenv("APP_CONTAINER")
	subscribers  = strings.Split(envOr("SUBSCRIBERS_UNDER_TEST", "airport:PEK,airline:CA,ground:SWP"), ",")
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---- 与 app/sim 响应对应的镜像类型 ----

type publishRequest struct {
	IdempotencyKey  string         `json:"idempotency_key"`
	BusinessKey     string         `json:"business_key"`
	ExpectedVersion int64          `json:"expected_version"`
	Kind            string         `json:"kind"`
	Payload         map[string]any `json:"payload,omitempty"`
	Note            string         `json:"note,omitempty"`
}

type revokeRequest struct {
	IdempotencyKey  string `json:"idempotency_key"`
	ExpectedVersion int64  `json:"expected_version"`
	Note            string `json:"note,omitempty"`
}

type publishResult struct {
	AnnouncementID       int64  `json:"announcement_id"`
	BusinessKey          string `json:"business_key"`
	Version              int64  `json:"version"`
	Kind                 string `json:"kind"`
	AnnouncementStatus   string `json:"announcement_status"`
	IdempotencyKey       string `json:"idempotency_key"`
	SupersededDeliveries int64  `json:"superseded_deliveries"`
	Deliveries           []struct {
		DeliveryID int64  `json:"delivery_id"`
		Subscriber string `json:"subscriber"`
		Status     string `json:"status"`
	} `json:"deliveries"`
}

type receiptRequest struct {
	ReceiptKey string `json:"receipt_key"`
	DeliveryID int64  `json:"delivery_id"`
	Subscriber string `json:"subscriber"`
	Version    int64  `json:"version"`
	Verdict    string `json:"verdict"`
}

type receiptResult struct {
	ReceiptID  int64  `json:"receipt_id"`
	DeliveryID int64  `json:"delivery_id"`
	Subscriber string `json:"subscriber"`
	Version    int64  `json:"version"`
	Status     string `json:"status"`
	Duplicate  bool   `json:"duplicate"`
}

type errorBody struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

type deliveryDetail struct {
	DeliveryID   int64      `json:"delivery_id"`
	Version      int64      `json:"version"`
	Kind         string     `json:"kind"`
	Status       string     `json:"status"`
	AttemptCount int        `json:"attempt_count"`
	MaxAttempts  int        `json:"max_attempts"`
	LastError    string     `json:"last_error"`
	SentAt       *time.Time `json:"sent_at"`
	AckedAt      *time.Time `json:"acked_at"`
	ReceiptID    *int64     `json:"receipt_id"`
}

type detailResponse struct {
	BusinessKey    string `json:"business_key"`
	Status         string `json:"status"`
	CurrentVersion int64  `json:"current_version"`
	Versions       []struct {
		Version int64  `json:"version"`
		Kind    string `json:"kind"`
	} `json:"versions"`
	Subscribers []struct {
		Subscriber string           `json:"subscriber"`
		Converged  bool             `json:"converged"`
		Deliveries []deliveryDetail `json:"deliveries"`
	} `json:"subscribers"`
	Summary struct {
		Total      int `json:"total"`
		Pending    int `json:"pending"`
		Leased     int `json:"leased"`
		Sent       int `json:"sent"`
		Acked      int `json:"acked"`
		Superseded int `json:"superseded"`
		Dead       int `json:"dead"`
	} `json:"summary"`
}

type receivedRecord struct {
	DeliveryID  int64  `json:"delivery_id"`
	Subscriber  string `json:"subscriber"`
	BusinessKey string `json:"business_key"`
	Version     int64  `json:"version"`
	Kind        string `json:"kind"`
	Count       int    `json:"count"`
}

type drainOutcome struct {
	DeliveryID int64  `json:"delivery_id"`
	HTTPStatus int    `json:"http_status"`
	Code       string `json:"code"`
}

// ---- HTTP 辅助 ----

type apiResponse struct {
	status int
	body   []byte
}

// doJSON 永不 t.Fatal（可在并发 goroutine 中使用）；传输错误以 status=0 返回。
func doJSON(method, url string, payload any) apiResponse {
	var reader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return apiResponse{0, []byte(err.Error())}
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return apiResponse{0, []byte(err.Error())}
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return apiResponse{0, []byte(err.Error())}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResponse{0, []byte(err.Error())}
	}
	return apiResponse{resp.StatusCode, body}
}

func decode[T any](t *testing.T, resp apiResponse) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(resp.body, &out); err != nil {
		t.Fatalf("decode response (status %d): %v\nbody: %s", resp.status, err, resp.body)
	}
	return out
}

func publish(t *testing.T, req publishRequest) apiResponse {
	t.Helper()
	return doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements", req)
}

func mustPublish(t *testing.T, req publishRequest) publishResult {
	t.Helper()
	resp := publish(t, req)
	if resp.status != http.StatusCreated {
		t.Fatalf("publish failed: %d %s", resp.status, resp.body)
	}
	return decode[publishResult](t, resp)
}

func getDetail(t *testing.T, businessKey string) detailResponse {
	t.Helper()
	resp := doJSON(http.MethodGet, appBaseURL+"/api/v1/announcements/"+businessKey, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("detail failed: %d %s", resp.status, resp.body)
	}
	return decode[detailResponse](t, resp)
}

func postReceipt(t *testing.T, req receiptRequest) apiResponse {
	t.Helper()
	return doJSON(http.MethodPost, appBaseURL+"/api/v1/receipts", req)
}

// ---- 订阅方模拟器控制面 ----

func simSetMode(t *testing.T, subscriber, mode string) {
	t.Helper()
	resp := doJSON(http.MethodPost, simBaseURL+"/sim/control",
		map[string]string{"subscriber": subscriber, "mode": mode})
	if resp.status != http.StatusOK {
		t.Fatalf("sim control failed: %d %s", resp.status, resp.body)
	}
}

func simAllModes(t *testing.T, mode string) {
	t.Helper()
	for _, sub := range subscribers {
		simSetMode(t, sub, mode)
	}
}

func simReceived(t *testing.T) []receivedRecord {
	t.Helper()
	resp := doJSON(http.MethodGet, simBaseURL+"/sim/received", nil)
	if resp.status != http.StatusOK {
		t.Fatalf("sim received failed: %d %s", resp.status, resp.body)
	}
	var out struct {
		Received []receivedRecord `json:"received"`
	}
	if err := json.Unmarshal(resp.body, &out); err != nil {
		t.Fatalf("decode sim received: %v", err)
	}
	return out.Received
}

func simReceivedCount(t *testing.T, deliveryID int64) int {
	t.Helper()
	for _, rec := range simReceived(t) {
		if rec.DeliveryID == deliveryID {
			return rec.Count
		}
	}
	return 0
}

func simDrain(t *testing.T, subscriber string) []drainOutcome {
	t.Helper()
	resp := doJSON(http.MethodPost, simBaseURL+"/sim/drain",
		map[string]string{"subscriber": subscriber})
	if resp.status != http.StatusOK {
		t.Fatalf("sim drain failed: %d %s", resp.status, resp.body)
	}
	var out struct {
		Outcomes []drainOutcome `json:"outcomes"`
	}
	if err := json.Unmarshal(resp.body, &out); err != nil {
		t.Fatalf("decode drain: %v", err)
	}
	return out.Outcomes
}

// ---- 等待与断言 ----

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out (%s) waiting for: %s", timeout, desc)
}

func waitForHTTP(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

// waitConverged 等待公告进入静止且全部订阅方收敛于指定版本：
// 没有在途投递（pending/leased/sent 均为 0），且每个订阅方该版本的投递均已 acked。
func waitConverged(t *testing.T, businessKey string, version int64) detailResponse {
	t.Helper()
	var detail detailResponse
	waitFor(t, 90*time.Second, fmt.Sprintf("%s converged at v%d", businessKey, version), func() bool {
		resp := doJSON(http.MethodGet, appBaseURL+"/api/v1/announcements/"+businessKey, nil)
		if resp.status != http.StatusOK {
			return false
		}
		detail = decode[detailResponse](t, resp)
		if detail.CurrentVersion != version {
			return false
		}
		if detail.Summary.Pending+detail.Summary.Leased+detail.Summary.Sent != 0 {
			return false
		}
		for _, sub := range detail.Subscribers {
			if !sub.Converged {
				return false
			}
		}
		return len(detail.Subscribers) == len(subscribers)
	})
	return detail
}

func findDelivery(t *testing.T, detail detailResponse, subscriber string, version int64) deliveryDetail {
	t.Helper()
	for _, sub := range detail.Subscribers {
		if sub.Subscriber != subscriber {
			continue
		}
		for _, d := range sub.Deliveries {
			if d.Version == version {
				return d
			}
		}
	}
	t.Fatalf("delivery not found: subscriber=%s version=%d", subscriber, version)
	return deliveryDetail{}
}

func uniqueKey(prefix string) string {
	return fmt.Sprintf("IT:%s:%d", prefix, time.Now().UnixNano())
}

// ---- Prometheus 指标 ----

// scrapeMetrics 返回 "name{k=v,k=v}" → value 的扁平映射。
func scrapeMetrics(t *testing.T) map[string]float64 {
	t.Helper()
	resp := doJSON(http.MethodGet, appBaseURL+"/metrics", nil)
	if resp.status != http.StatusOK {
		t.Fatalf("scrape metrics failed: %d %s", resp.status, resp.body)
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(resp.body))
	if err != nil {
		t.Fatalf("parse metrics: %v", err)
	}
	out := map[string]float64{}
	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			labels := make([]string, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				labels = append(labels, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(labels)
			key := name + "{" + strings.Join(labels, ",") + "}"
			switch {
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			}
		}
	}
	return out
}

func metricValue(metrics map[string]float64, name string, labels ...string) float64 {
	sort.Strings(labels)
	return metrics[name+"{"+strings.Join(labels, ",")+"}"]
}

func metricDelta(before, after map[string]float64, name string, labels ...string) float64 {
	return metricValue(after, name, labels...) - metricValue(before, name, labels...)
}
