//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

// env 聚合单个场景所需的全部组件；每个场景独立注册表，指标互不干扰。
type env struct {
	t         *testing.T
	repo      *store.Repo
	registry  *prometheus.Registry
	metr      *metrics.Set
	app       *testApp
	stopGauge func()
}

func newEnv(t *testing.T) *env {
	t.Helper()
	url, cleanup := startPostgres(t)
	t.Cleanup(cleanup)
	pool := migrateOpen(t, url)

	repo := store.New(pool)
	reg := prometheus.NewRegistry()
	metr := metrics.New(reg)

	gctx, stopGauge := context.WithCancel(context.Background())
	metr.StartGaugeCollector(gctx, repo, 100*time.Millisecond)
	t.Cleanup(stopGauge)

	app := startApp(t, repo, reg, metr)
	return &env{t: t, repo: repo, registry: reg, metr: metr, app: app, stopGauge: stopGauge}
}

func (e *env) runDispatcher() func() {
	d, stop := startDispatcher(e.t, e.repo, e.metr, fastDispatcherConfig())
	e.t.Logf("dispatcher worker: %s", d.WorkerID())
	return stop
}

func (e *env) receipt(apiKey, businessKey string, version int) (int, map[string]any) {
	status, raw := e.app.post("/v1/announcements/"+businessKey+"/receipts", "", apiKey,
		map[string]any{"version": version})
	var out map[string]any
	_ = decodeJSON(raw, &out)
	return status, out
}

func (e *env) counter(name string, labels map[string]string) float64 {
	return gatherValue(e.t, e.registry, name, "counter", labels)
}

func (e *env) gauge(name string, labels map[string]string) float64 {
	return gatherValue(e.t, e.registry, name, "gauge", labels)
}

// ---------------------------------------------------------------------------
// 场景 1：并发发布只有一个胜出者；重复请求返回首次结果；撤销必须更高版本
// ---------------------------------------------------------------------------

func TestConcurrentPublishAndIdempotentReplay(t *testing.T) {
	e := newEnv(t)
	cb := newCallbackMock(t)
	e.app.registerSubscriber("ops-a", "key-a", cb)

	const businessKey = "MU5735-2026-09-19"
	const goroutines = 20

	var wg sync.WaitGroup
	statuses := make([]int, goroutines)
	versions := make([]int, goroutines)
	winnerKey := ""
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// 每个 goroutine 都认为前版是 0（客户端各自独立的首次尝试，
			// 并非同一幂等键的重试）。
			key := "concurrent-" + itoa(i)
			st, body := e.app.publish(businessKey, 0, "cancellation", "CANCELLED", key)
			statuses[i] = st
			if st == 201 {
				winnerKey = key
				if v, ok := body["version"].(float64); ok {
					versions[i] = int(v)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if winnerKey == "" {
		t.Fatal("no winning request observed")
	}

	var ok, conflict int
	for _, st := range statuses {
		switch st {
		case 201:
			ok++
		case 409:
			conflict++
		default:
			t.Fatalf("unexpected publish status %d", st)
		}
	}
	if ok != 1 || conflict != goroutines-1 {
		t.Fatalf("expected exactly 1 winner and %d conflicts, got ok=%d conflict=%d (%v)",
			goroutines-1, ok, conflict, statuses)
	}

	// 公告只前进到第 1 版。
	detail := e.app.detail(businessKey)
	if detail.CurrentVersion != 1 || len(detail.Versions) != 1 {
		t.Fatalf("expected single version, got current=%d versions=%d",
			detail.CurrentVersion, len(detail.Versions))
	}

	// 同一个幂等键重放：必须返回首次结果（同版本号），且不产生新投递。
	st, replay := e.app.publishFull(publishBody{
		BusinessKey: businessKey, Kind: "cancellation", StatusText: "CANCELLED",
		PreviousVersion: 0, Publisher: "ops-desk",
	}, winnerKey)
	if st != 201 {
		t.Fatalf("replay expected 201, got %d: %v", st, replay)
	}
	if int(replay["version"].(float64)) != 1 {
		t.Fatalf("replay must return first result version=1, got %v", replay["version"])
	}
	if got := e.counter("bulletin_idempotent_replays_total", nil); got != 1 {
		t.Fatalf("expected 1 idempotent replay metric, got %v", got)
	}
	detail = e.app.detail(businessKey)
	if len(detail.Versions) != 1 {
		t.Fatalf("replay created a new version: %d versions", len(detail.Versions))
	}
	if rows := deliveryRows(detail); rows != 1 {
		t.Fatalf("replay created extra deliveries: %d", rows)
	}

	// 同键不同请求体：拒绝（防止用幂等键偷换语义）。
	st, _ = e.app.publishFull(publishBody{
		BusinessKey: businessKey, Kind: "restoration", StatusText: "REOPENED",
		PreviousVersion: 0,
	}, winnerKey)
	if st != 409 {
		t.Fatalf("same idempotency key with different body must be 409, got %d", st)
	}

	// 撤销之后只能以更高版本继续：prev=0 再来一次必须冲突；prev=1 恢复成功为 v2。
	st, _ = e.app.publish(businessKey, 0, "cancellation", "CANCELLED AGAIN", "again-0")
	if st != 409 {
		t.Fatalf("publish with stale previous_version must be 409, got %d", st)
	}
	if got := e.counter("bulletin_publish_version_conflicts_total", nil); got != float64(goroutines) {
		// 19 并发落败 + 1 次陈旧前版号。
		t.Fatalf("expected %d conflict metrics, got %v", goroutines, got)
	}
	st, body := e.app.publish(businessKey, 1, "restoration", "OPERATIONS RESUMED", "restore-1")
	if st != 201 || int(body["version"].(float64)) != 2 {
		t.Fatalf("restoration after cancellation must create v2, got %d %v", st, body)
	}
	detail = e.app.detail(businessKey)
	if detail.CurrentVersion != 2 || len(detail.Versions) != 2 {
		t.Fatalf("expected current=2 with 2 versions, got %d / %d",
			detail.CurrentVersion, len(detail.Versions))
	}
	if detail.Versions[1].Kind != "restoration" {
		t.Fatalf("v2 kind = %q, want restoration", detail.Versions[1].Kind)
	}
}

// ---------------------------------------------------------------------------
// 场景 2：投递器中断 / 重启——租约回收，继续未完成投递且不重复生效
// ---------------------------------------------------------------------------

func TestDispatcherCrashRecovery(t *testing.T) {
	e := newEnv(t)
	cb := newCallbackMock(t)
	e.app.registerSubscriber("ops-b", "key-b", cb)

	// 第一轮投递器投出 v1 并正常完成，建立基线。
	stop1 := e.runDispatcher()
	st, body := e.app.publish("FLIGHT-RECOVERY", 0, "cancellation", "CANCELLED", "pub-1")
	if st != 201 {
		t.Fatalf("publish: %d %v", st, body)
	}
	waitFor(t, 5*time.Second, func() bool {
		d := e.app.detail("FLIGHT-RECOVERY")
		return len(d.Subscribers[0].Deliveries) == 1 &&
			d.Subscribers[0].Deliveries[0].Status == "delivered"
	}, "v1 delivered")
	stop1()

	// 让回调阻塞，模拟「投递已发出但结果未知」时进程被杀死。
	release := cb.Block()
	stop2 := e.runDispatcher()
	st, body = e.app.publish("FLIGHT-RECOVERY", 1, "restoration", "RESUMED", "pub-2")
	if st != 201 {
		t.Fatalf("publish v2: %d %v", st, body)
	}
	// 等行进入 leased（HTTP 调用正挂在订阅方）。
	waitFor(t, 5*time.Second, func() bool {
		d := e.app.detail("FLIGHT-RECOVERY")
		for _, dl := range d.Subscribers[0].Deliveries {
			if dl.Version == 2 && dl.Status == "leased" {
				return true
			}
		}
		return false
	}, "v2 leased with in-flight HTTP call")

	// “杀死”投递器：停机时挂起的回调被取消，行保持 leased。
	stop2()
	// 此刻订阅方才返回 200，但原持有者已不存在——这个迟到的成功绝不能被记账。
	release()

	// 等待租约过期后启动新投递器（= 进程重启）。
	time.Sleep(2300 * time.Millisecond)
	stop3 := e.runDispatcher()
	defer stop3()

	waitFor(t, 8*time.Second, func() bool {
		d := e.app.detail("FLIGHT-RECOVERY")
		for _, dl := range d.Subscribers[0].Deliveries {
			if dl.Version == 2 && dl.Status == "delivered" && dl.Attempts >= 2 {
				return true
			}
		}
		return false
	}, "v2 redelivered after lease recovery")

	// 同一投递 ID 被送出两次（至少一次语义），但库里只有一条订阅记录、delivered 仅生效一次。
	calls := cb.Calls()
	var v2Calls []receivedCall
	for _, c := range calls {
		if c.Version == 2 {
			v2Calls = append(v2Calls, c)
		}
	}
	if len(v2Calls) != 2 {
		t.Fatalf("v2 expected 2 physical HTTP attempts (crash + recovery), got %d", len(v2Calls))
	}
	if v2Calls[0].DeliveryID != v2Calls[1].DeliveryID {
		t.Fatalf("recovery must reuse the same outbox row/delivery id: %q vs %q",
			v2Calls[0].DeliveryID, v2Calls[1].DeliveryID)
	}

	d := e.app.detail("FLIGHT-RECOVERY")
	counts := countByVersion(d.Subscribers[0].Deliveries)
	for v, n := range map[int]int{1: 1, 2: 1} {
		if counts[v] != n {
			t.Fatalf("version %d delivery rows = %d, want exactly 1 (all=%v)", v, counts[v], counts)
		}
	}
	var v2 struct {
		Status   string
		Attempts int
	}
	for _, dl := range d.Subscribers[0].Deliveries {
		if dl.Version == 2 {
			v2.Status, v2.Attempts = dl.Status, dl.Attempts
		}
	}
	if v2.Status != "delivered" || v2.Attempts != 2 {
		t.Fatalf("v2 = status=%s attempts=%d, want delivered/2", v2.Status, v2.Attempts)
	}
	if got := e.counter("bulletin_dispatch_lease_recycled_total", nil); got < 1 {
		t.Fatalf("lease_recycled metric = %v, want >= 1", got)
	}
	if got := e.gauge("bulletin_dispatch_in_flight", nil); got != 0 {
		t.Fatalf("in_flight gauge after recovery = %v, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// 场景 3：分级重试与死信
// ---------------------------------------------------------------------------

func TestTieredRetryAndDeadLetter(t *testing.T) {
	e := newEnv(t)

	flaky := newCallbackMock(t)
	flaky.FailNext(2) // 前两次 500，第三次成功
	e.app.registerSubscriber("flaky", "key-flaky", flaky)

	dead := newCallbackMock(t)
	dead.FailNext(100) // 始终失败
	e.app.registerSubscriber("doomed", "key-doomed", dead)

	stop := e.runDispatcher()
	defer stop()

	st, _ := e.app.publish("FLIGHT-RETRY", 0, "update", "DELAY 40M", "pub")
	if st != 201 {
		t.Fatalf("publish status = %d", st)
	}

	waitFor(t, 15*time.Second, func() bool {
		d := e.app.detail("FLIGHT-RETRY")
		got := map[string]struct {
			status   string
			attempts int
		}{}
		for _, s := range d.Subscribers {
			got[s.Name] = struct {
				status   string
				attempts int
			}{s.Deliveries[0].Status, s.Deliveries[0].Attempts}
		}
		return got["flaky"].status == "delivered" && got["flaky"].attempts == 3 &&
			got["doomed"].status == "dead" && got["doomed"].attempts == 6
	}, "flaky recovers via retry; doomed goes dead")

	if got := e.counter("bulletin_dispatch_attempts_total",
		map[string]string{"result": "success"}); got < 1 {
		t.Fatalf("success counter = %v", got)
	}
	if got := e.counter("bulletin_dispatch_retries_scheduled_total", nil); got < 2 {
		t.Fatalf("retry counter = %v, want >= 2", got)
	}
	if got := e.counter("bulletin_dispatch_attempts_total",
		map[string]string{"result": "dead"}); got != 1 {
		t.Fatalf("dead counter = %v, want 1", got)
	}
	// gauge 由后台采集器周期性刷新，等它追上 DB 事实。
	waitFor(t, 3*time.Second, func() bool {
		return e.gauge("bulletin_outbox_deliveries",
			map[string]string{"status": "dead"}) == 1
	}, "dead gauge catches up")
}

// ---------------------------------------------------------------------------
// 场景 4：回执——未知/过期/未送达全部拒绝；重复回执不重复生效
// ---------------------------------------------------------------------------

func TestReceiptValidationAndDuplicate(t *testing.T) {
	e := newEnv(t)
	cb := newCallbackMock(t)
	e.app.registerSubscriber("ops-c", "key-c", cb)

	const key = "MU-RECEIPTS"
	st, _ := e.app.publish(key, 0, "cancellation", "CANCELLED", "v1")
	if st != 201 {
		t.Fatalf("publish v1: %d", st)
	}

	// 未送达时回执：拒绝，杜绝提前完成。
	if st, body := e.receipt("key-c", key, 1); st != 409 ||
		body["error"].(map[string]any)["reason"] != "not_delivered" {
		t.Fatalf("pre-delivery ack must be 409/not_delivered, got %d %v", st, body)
	}
	// 未知版本、未知公告、未知订阅方。
	if st, body := e.receipt("key-c", key, 99); st != 409 ||
		body["error"].(map[string]any)["reason"] != "unknown_version" {
		t.Fatalf("unknown version must be 409/unknown_version, got %d %v", st, body)
	}
	if st, _ := e.receipt("key-c", "MISSING-KEY", 1); st != 409 {
		t.Fatalf("unknown announcement receipt must be 409, got %d", st)
	}
	if st, _ := e.receipt("wrong-key", key, 1); st != 401 {
		t.Fatalf("unknown subscriber receipt must be 401, got %d", st)
	}

	stop := e.runDispatcher()
	defer stop()
	waitFor(t, 5*time.Second, func() bool {
		d := e.app.detail(key)
		return len(d.Subscribers[0].Deliveries) == 1 &&
			d.Subscribers[0].Deliveries[0].Status == "delivered"
	}, "v1 delivered")

	// 发布撤销 v2（恢复）并等待送达，再让 v1 的确认迟到。
	st, _ = e.app.publish(key, 1, "restoration", "RESUMED", "v2")
	if st != 201 {
		t.Fatalf("publish v2: %d", st)
	}
	waitFor(t, 5*time.Second, func() bool {
		d := e.app.detail(key)
		for _, dl := range d.Subscribers[0].Deliveries {
			if dl.Version == 2 && dl.Status == "delivered" {
				return true
			}
		}
		return false
	}, "v2 delivered")

	// 撤销/恢复后迟到的 v1 确认：必须以过期版本拒绝。
	if st, body := e.receipt("key-c", key, 1); st != 409 ||
		body["error"].(map[string]any)["reason"] != "obsolete_version" {
		t.Fatalf("late ack of v1 must be 409/obsolete_version, got %d %v", st, body)
	}
	if got := e.counter("bulletin_receipts_rejected_total",
		map[string]string{"reason": "obsolete_version"}); got != 1 {
		t.Fatalf("obsolete rejection metric = %v, want 1", got)
	}

	// 当前版本的确认生效；紧接着的重复回执返回首次结果，duplicate=true。
	st, body := e.receipt("key-c", key, 2)
	if st != 200 || body["duplicate"] != false {
		t.Fatalf("first ack v2 = %d %v", st, body)
	}
	st, body = e.receipt("key-c", key, 2)
	if st != 200 || body["duplicate"] != true {
		t.Fatalf("duplicate ack must be 200 duplicate=true, got %d %v", st, body)
	}
	st, body = e.receipt("key-c", key, 2)
	if st != 200 || body["duplicate"] != true {
		t.Fatalf("third ack must still be duplicate=true, got %v", body)
	}
	if got := e.counter("bulletin_receipts_acked_total",
		map[string]string{"duplicate": "true"}); got != 2 {
		t.Fatalf("duplicate ack metric = %v, want 2", got)
	}
	if got := e.counter("bulletin_receipts_acked_total",
		map[string]string{"duplicate": "false"}); got != 1 {
		t.Fatalf("first ack metric = %v, want 1", got)
	}

	// 状态对账：v2 恰好一条 acked；没有任何行被提前置为 acked。
	d := e.app.detail(key)
	acked, delivered := 0, 0
	for _, s := range d.Subscribers {
		for _, dl := range s.Deliveries {
			switch dl.Status {
			case "acked":
				acked++
				if dl.Version != 2 {
					t.Fatalf("only v2 may be acked, found v%d acked", dl.Version)
				}
			case "delivered":
				delivered++
			}
		}
	}
	if acked != 1 || delivered != 1 {
		t.Fatalf("want acked=1(v2) delivered=1(v1), got acked=%d delivered=%d", acked, delivered)
	}
}

// ---------------------------------------------------------------------------
// 场景 5：最终收敛——多版本 × 多订阅方，从详情与指标双向核对
// ---------------------------------------------------------------------------

func TestConvergenceAcrossDetailAndMetrics(t *testing.T) {
	e := newEnv(t)
	cbs := make([]*callbackMock, 4)
	for i := range cbs {
		cbs[i] = newCallbackMock(t)
		e.app.registerSubscriber("sub-"+itoa(i), "key-"+itoa(i), cbs[i])
	}
	stop := e.runDispatcher()
	defer stop()

	const key = "MU-CONVERGE"
	kinds := []string{"cancellation", "update", "restoration"}
	for v, kind := range kinds {
		st, _ := e.app.publish(key, v, kind, "text-"+itoa(v), "pub-"+itoa(v))
		if st != 201 {
			t.Fatalf("publish v%d: %d", v+1, st)
		}
	}

	const versions, subs = 3, 4
	// 全部投递成功。
	waitFor(t, 10*time.Second, func() bool {
		return e.gauge("bulletin_outbox_deliveries",
			map[string]string{"status": "delivered"}) == float64(versions*subs)
	}, "all 12 deliveries delivered")

	// 每个订阅方确认最新版（旧版确认在本场景不做，保持 delivered）。
	for i := 0; i < subs; i++ {
		if st, body := e.receipt("key-"+itoa(i), key, versions); st != 200 {
			t.Fatalf("ack from sub %d: %d %v", i, st, body)
		}
	}

	waitFor(t, 5*time.Second, func() bool {
		return e.gauge("bulletin_outbox_deliveries",
			map[string]string{"status": "acked"}) == float64(subs)
	}, "acked gauge settles")

	d := e.app.detail(key)
	if d.CurrentVersion != versions || len(d.Versions) != versions {
		t.Fatalf("versions = %d/%d", d.CurrentVersion, len(d.Versions))
	}
	if len(d.Subscribers) != subs {
		t.Fatalf("subscribers in detail = %d", len(d.Subscribers))
	}
	totalRows := 0
	for _, s := range d.Subscribers {
		if len(s.Deliveries) != versions {
			t.Fatalf("subscriber %s has %d delivery rows, want %d (no loss, no duplicate)",
				s.Name, len(s.Deliveries), versions)
		}
		seen := map[int]string{}
		for _, dl := range s.Deliveries {
			if prev, dup := seen[dl.Version]; dup {
				t.Fatalf("duplicate delivery row for %s v%d (already %s)", s.Name, dl.Version, prev)
			}
			seen[dl.Version] = dl.Status
			totalRows++
		}
		if seen[versions] != "acked" {
			t.Fatalf("subscriber %s latest version not acked: %q", s.Name, seen[versions])
		}
		for v := 1; v < versions; v++ {
			if seen[v] != "delivered" {
				t.Fatalf("subscriber %s v%d = %q, want delivered", s.Name, v, seen[v])
			}
		}
	}
	if totalRows != versions*subs {
		t.Fatalf("total delivery rows = %d, want %d", totalRows, versions*subs)
	}

	// 指标侧：无残留进行中状态；已发布版本计数对；计数器总和 = 物理行数。
	for _, st := range []string{"pending", "leased", "retry_wait", "dead"} {
		if got := e.gauge("bulletin_outbox_deliveries",
			map[string]string{"status": st}); got != 0 {
			t.Fatalf("gauge[%s] = %v, want 0 at convergence", st, got)
		}
	}
	if got := e.gauge("bulletin_outbox_deliveries",
		map[string]string{"status": "delivered"}); got != float64(subs*(versions-1)) {
		t.Fatalf("delivered gauge = %v, want %d", got, subs*(versions-1))
	}
	if got := e.counter("bulletin_announcements_published_total",
		map[string]string{"kind": "cancellation"}); got != 1 {
		t.Fatalf("published{cancellation} = %v, want 1", got)
	}
	if got := e.counter("bulletin_dispatch_attempts_total",
		map[string]string{"result": "success"}); got != float64(versions*subs) {
		t.Fatalf("success attempts = %v, want %d", got, versions*subs)
	}
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

func deliveryRows(d detailResp) int {
	n := 0
	for _, s := range d.Subscribers {
		n += len(s.Deliveries)
	}
	return n
}

func countByVersion(items []deliveryRow) map[int]int {
	out := map[int]int{}
	for _, dl := range items {
		out[dl.Version]++
	}
	return out
}
