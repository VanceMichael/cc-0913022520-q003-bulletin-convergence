//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"example.com/disruption-bulletins/internal/api"
	"example.com/disruption-bulletins/internal/dispatcher"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

// ---------------------------------------------------------------------------
// 订阅方回调 mock
// ---------------------------------------------------------------------------

type receivedCall struct {
	DeliveryID string
	Version    int
	Body       []byte
	At         time.Time
}

// callbackMock 模拟一个下游订阅方：可配置前 N 次失败、注入阻塞、记录全部收到的调用。
type callbackMock struct {
	mu sync.Mutex

	t          *testing.T
	server     *httptest.Server
	calls      []receivedCall
	failNext   int           // 还需失败多少次（返回 500）
	blockCh    chan struct{} // 非 nil 时，回调阻塞直到该通道关闭
	closeOnHit chan struct{} // 非 nil 时，收到一次回调后关闭（用于唤醒测试）
	statusCode int
}

func newCallbackMock(t *testing.T) *callbackMock {
	m := &callbackMock{t: t, statusCode: 200}
	m.server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

func (m *callbackMock) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	m.mu.Lock()
	blockCh := m.blockCh
	m.mu.Unlock()
	if blockCh != nil {
		<-blockCh
	}

	m.mu.Lock()
	fail := m.failNext
	if fail > 0 {
		m.failNext--
	}
	m.calls = append(m.calls, receivedCall{
		DeliveryID: r.Header.Get("X-Delivery-Id"),
		Version:    atoiOrMinus(r.Header.Get("X-Bulletin-Version")),
		Body:       append([]byte(nil), body...),
		At:         time.Now(),
	})
	ch := m.closeOnHit
	m.mu.Unlock()

	if ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	if fail > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("synthetic failure"))
		return
	}
	w.WriteHeader(m.statusCode)
}

func (m *callbackMock) URL() string { return m.server.URL }

// FailNext 让接下来 n 次回调返回 500。
func (m *callbackMock) FailNext(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext += n
}

// Block 让回调阻塞，直到返回的 release 被调用。
func (m *callbackMock) Block() (release func()) {
	ch := make(chan struct{})
	m.mu.Lock()
	m.blockCh = ch
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		m.blockCh = nil
		m.mu.Unlock()
		close(ch)
	}
}

func (m *callbackMock) Calls() []receivedCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]receivedCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func atoiOrMinus(s string) int {
	n := 0
	if s == "" {
		return -1
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// ---------------------------------------------------------------------------
// 应用装配
// ---------------------------------------------------------------------------

type testApp struct {
	t        *testing.T
	repo     *store.Repo
	registry *prometheus.Registry
	metr     *metrics.Set
	server   *httptest.Server
}

type appEnv struct {
	app        *testApp
	dispatcher *dispatcher.Dispatcher
	stop       func()
}

// startApp 装配 HTTP 应用（不含投递器，投递器由各测试按需启停，模拟中断/重启）。
func startApp(t *testing.T, repo *store.Repo, reg *prometheus.Registry, metr *metrics.Set) *testApp {
	a := &testApp{t: t, repo: repo, registry: reg, metr: metr}
	a.server = httptest.NewServer(api.Router(api.Deps{
		Repo:     repo,
		Metrics:  metr,
		Registry: reg,
	}))
	t.Cleanup(a.server.Close)
	return a
}

func startDispatcher(t *testing.T, repo *store.Repo, metr *metrics.Set, cfg dispatcher.Config) (*dispatcher.Dispatcher, func()) {
	d := dispatcher.New(repo, metr, cfg)
	stop := d.RunBackground(context.Background())
	return d, stop
}

// fastDispatcherConfig 返回快节奏配置：租约足够短，方便测试崩溃回收。
func fastDispatcherConfig() dispatcher.Config {
	cfg := dispatcher.DefaultConfig()
	cfg.PollInterval = 20 * time.Millisecond
	cfg.BatchSize = 100
	cfg.LeaseDuration = 2 * time.Second
	cfg.HTTPTimeout = 5 * time.Second
	cfg.Concurrency = 16
	cfg.Backoffs = []time.Duration{
		100 * time.Millisecond, 300 * time.Millisecond, time.Second,
	}
	return cfg
}

// ---------------------------------------------------------------------------
// HTTP 客户端辅助
// ---------------------------------------------------------------------------

func (a *testApp) post(path, idemKey, apiKey string, body any) (int, []byte) {
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rdr = bytes.NewReader(b)
	case string:
		rdr = bytes.NewBufferString(b)
	default:
		raw, _ := json.Marshal(b)
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(http.MethodPost, a.server.URL+path, rdr)
	if err != nil {
		a.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func (a *testApp) get(path string) (int, []byte) {
	resp, err := http.Get(a.server.URL + path)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func (a *testApp) registerSubscriber(name, apiKey string, cb *callbackMock) {
	status, body := a.post("/v1/subscribers", "", "", map[string]any{
		"name": name, "callback_url": cb.URL(), "api_key": apiKey,
	})
	if status != http.StatusCreated {
		a.t.Fatalf("register subscriber: status=%d body=%s", status, body)
	}
}

// publishBody 是发布请求的标准结构。
type publishBody struct {
	BusinessKey     string          `json:"business_key"`
	FlightKey       string          `json:"flight_key,omitempty"`
	Kind            string          `json:"kind"`
	StatusText      string          `json:"status_text"`
	Body            json.RawMessage `json:"body,omitempty"`
	Publisher       string          `json:"publisher,omitempty"`
	PreviousVersion int             `json:"previous_version"`
}

func (a *testApp) publish(key string, prev int, kind, statusText, idem string) (int, map[string]any) {
	return a.publishFull(publishBody{
		BusinessKey: key, Kind: kind, StatusText: statusText,
		PreviousVersion: prev, Publisher: "ops-desk",
	}, idem)
}

func (a *testApp) publishFull(p publishBody, idem string) (int, map[string]any) {
	status, raw := a.post("/v1/announcements", idem, "", p)
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return status, out
}

type detailResp struct {
	BusinessKey    string `json:"business_key"`
	CurrentVersion int    `json:"current_version"`
	Versions       []struct {
		Version int    `json:"version"`
		Kind    string `json:"kind"`
	} `json:"versions"`
	Subscribers []struct {
		Name       string        `json:"name"`
		Deliveries []deliveryRow `json:"deliveries"`
	} `json:"subscribers"`
}

type deliveryRow struct {
	Version        int        `json:"version"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	MaxAttempts    int        `json:"max_attempts"`
	DeliveredAt    *time.Time `json:"delivered_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at"`
}

func decodeJSON(raw []byte, v any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}

func (a *testApp) detail(key string) detailResp {
	status, raw := a.get("/v1/announcements/" + key)
	if status != http.StatusOK {
		a.t.Fatalf("get detail: %d %s", status, raw)
	}
	var d detailResp
	if err := json.Unmarshal(raw, &d); err != nil {
		a.t.Fatalf("decode detail: %v: %s", err, raw)
	}
	return d
}

// ---------------------------------------------------------------------------
// Prometheus 指标辅助
// ---------------------------------------------------------------------------

type metricSample struct {
	Value  float64
	Labels map[string]string
}

// metricsText 拉取 /metrics 并解析成 name -> samples。
func (a *testApp) metricsText() string {
	resp, err := http.Get(a.server.URL + "/metrics")
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

// gatherMetric 通过注册表直接收集指定指标族（比文本解析精确）。
func gatherValue(t *testing.T, reg *prometheus.Registry, family, sampleName string, labels map[string]string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			if !labelsMatch(m, labels) {
				continue
			}
			switch sampleName {
			case "counter":
				return m.GetCounter().GetValue()
			case "gauge":
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	have := map[string]string{}
	for _, lp := range m.GetLabel() {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func (a *testApp) gaugeByStatus(status string) float64 {
	return gatherValue(a.t, a.registry, "bulletin_outbox_deliveries", "gauge",
		map[string]string{"status": status})
}

// waitFor 轮询条件直到为真或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

var _ = fmt.Sprintf
