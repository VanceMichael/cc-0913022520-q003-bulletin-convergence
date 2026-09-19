//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if err := waitForHTTP(appBaseURL+"/healthz", 120*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "app not healthy:", err)
		os.Exit(1)
	}
	if err := waitForHTTP(simBaseURL+"/healthz", 60*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "subscriber-sim not healthy:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// 并发发布：同一业务键、同一前版号的并发请求只有一个能成功；
// 同一幂等键的并发重复请求全部返回首次结果。
func TestConcurrentPublish(t *testing.T) {
	key := uniqueKey("conc")

	// 8 个并发发布（不同幂等键，expected_version=0）：恰好一个 201，其余 409。
	const racers = 8
	statuses := make([]int, racers)
	bodies := make([][]byte, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements", publishRequest{
				IdempotencyKey:  fmt.Sprintf("conc-a-%d", i),
				BusinessKey:     key,
				ExpectedVersion: 0,
				Kind:            "cancellation",
				Payload:         map[string]any{"flight": "CA1234", "reason": "weather"},
			})
			statuses[i] = resp.status
			bodies[i] = resp.body
		}(i)
	}
	wg.Wait()

	created, conflicts := 0, 0
	for i, st := range statuses {
		switch st {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
			body := decode[errorBody](t, apiResponse{st, bodies[i]})
			if body.Error.Code != "version_conflict" {
				t.Fatalf("racer %d: expected version_conflict, got %s", i, body.Error.Code)
			}
		default:
			t.Fatalf("racer %d: unexpected status %d: %s", i, st, bodies[i])
		}
	}
	if created != 1 || conflicts != racers-1 {
		t.Fatalf("expected 1 created + %d conflicts, got %d + %d", racers-1, created, conflicts)
	}

	// 4 个并发重复请求（同一幂等键）：全部成功且版本一致，恰好一个 201。
	const dupRacers = 4
	dupStatuses := make([]int, dupRacers)
	dupVersions := make([]int64, dupRacers)
	for i := 0; i < dupRacers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements", publishRequest{
				IdempotencyKey:  "conc-b-shared",
				BusinessKey:     key,
				ExpectedVersion: 1,
				Kind:            "recovery",
				Payload:         map[string]any{"flight": "CA1234"},
			})
			dupStatuses[i] = resp.status
			if resp.status == http.StatusOK || resp.status == http.StatusCreated {
				body := decode[publishResult](t, resp)
				dupVersions[i] = body.Version
			}
		}(i)
	}
	wg.Wait()
	dupCreated := 0
	for i, st := range dupStatuses {
		if st != http.StatusOK && st != http.StatusCreated {
			t.Fatalf("dup racer %d: unexpected status %d", i, st)
		}
		if st == http.StatusCreated {
			dupCreated++
		}
		if dupVersions[i] != 2 {
			t.Fatalf("dup racer %d: expected version 2, got %d", i, dupVersions[i])
		}
	}
	if dupCreated != 1 {
		t.Fatalf("expected exactly one 201 among duplicate racers, got %d", dupCreated)
	}

	// 头表只推进到 v2，每个版本恰好为每个订阅方生成一条投递。
	detail := getDetail(t, key)
	if detail.CurrentVersion != 2 || len(detail.Versions) != 2 {
		t.Fatalf("expected current_version=2 with 2 versions, got %+v", detail)
	}
	if len(detail.Subscribers) != len(subscribers) {
		t.Fatalf("expected %d subscriber records, got %d", len(subscribers), len(detail.Subscribers))
	}
	for _, sub := range detail.Subscribers {
		if len(sub.Deliveries) != 2 {
			t.Fatalf("subscriber %s: expected 2 deliveries (v1,v2), got %d", sub.Subscriber, len(sub.Deliveries))
		}
	}

	// 同一幂等键配不同请求体 → 409 idempotency_conflict。
	resp := publish(t, publishRequest{
		IdempotencyKey:  "conc-b-shared",
		BusinessKey:     key,
		ExpectedVersion: 1,
		Kind:            "cancellation",
		Payload:         map[string]any{"flight": "OTHER"},
	})
	if resp.status != http.StatusConflict {
		t.Fatalf("expected 409 for idempotency conflict, got %d: %s", resp.status, resp.body)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "idempotency_conflict" {
		t.Fatalf("expected idempotency_conflict, got %s", body.Error.Code)
	}

	waitConverged(t, key, 2)
}

// 重复请求返回首次结果：响应体逐字段一致，且不产生新投递。
func TestIdempotentPublishReturnsFirstResult(t *testing.T) {
	key := uniqueKey("idem")
	req := publishRequest{
		IdempotencyKey:  "idem-1",
		BusinessKey:     key,
		ExpectedVersion: 0,
		Kind:            "cancellation",
		Payload:         map[string]any{"flight": "MU511", "reason": "flow control"},
	}
	first := mustPublish(t, req)

	before := scrapeMetrics(t)
	resp := publish(t, req)
	if resp.status != http.StatusOK {
		t.Fatalf("duplicate publish should return 200, got %d: %s", resp.status, resp.body)
	}
	second := decode[publishResult](t, resp)
	if second.Version != first.Version || second.AnnouncementID != first.AnnouncementID ||
		second.IdempotencyKey != first.IdempotencyKey || len(second.Deliveries) != len(first.Deliveries) {
		t.Fatalf("duplicate result differs from first:\nfirst=%+v\nsecond=%+v", first, second)
	}
	for i := range first.Deliveries {
		if first.Deliveries[i].DeliveryID != second.Deliveries[i].DeliveryID {
			t.Fatalf("duplicate result has different delivery ids")
		}
	}
	after := scrapeMetrics(t)
	if got := metricDelta(before, after, "bulletin_deliveries_created_total"); got != 0 {
		t.Fatalf("duplicate publish created %v new deliveries", got)
	}
	if got := metricDelta(before, after, "bulletin_publish_total", "kind=cancellation", "result=duplicate"); got != 1 {
		t.Fatalf("expected duplicate counter +1, got %+v", got)
	}
	waitConverged(t, key, 1)
}

// 撤销规则：只能以更高版本发布；不能撤销不存在的公告；不能重复撤销；
// 撤销后仍可用更高版本重新发布。
func TestRevocationRules(t *testing.T) {
	key := uniqueKey("rev")

	// 撤销不存在的业务键 → 404。
	resp := doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "rev-x", ExpectedVersion: 0})
	if resp.status != http.StatusNotFound {
		t.Fatalf("revoke unknown key: expected 404, got %d: %s", resp.status, resp.body)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "unknown_announcement" {
		t.Fatalf("expected unknown_announcement, got %s", body.Error.Code)
	}

	mustPublish(t, publishRequest{
		IdempotencyKey: "rev-p1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
	})

	// 前版号不符 → 409 version_conflict。
	resp = doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "rev-stale", ExpectedVersion: 0})
	if resp.status != http.StatusConflict {
		t.Fatalf("revoke with stale version: expected 409, got %d", resp.status)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "version_conflict" {
		t.Fatalf("expected version_conflict, got %s", body.Error.Code)
	}

	// 正常撤销 → 更高版本 v2，公告进入 revoked。
	resp = doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "rev-ok", ExpectedVersion: 1, Note: "issued by mistake"})
	if resp.status != http.StatusCreated {
		t.Fatalf("revoke failed: %d %s", resp.status, resp.body)
	}
	revoked := decode[publishResult](t, resp)
	if revoked.Version != 2 || revoked.Kind != "revocation" || revoked.AnnouncementStatus != "revoked" {
		t.Fatalf("unexpected revoke result: %+v", revoked)
	}

	// 重复撤销（同一幂等键）→ 200 返回首次结果。
	resp = doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "rev-ok", ExpectedVersion: 1, Note: "issued by mistake"})
	if resp.status != http.StatusOK {
		t.Fatalf("duplicate revoke should return 200, got %d", resp.status)
	}

	// 已撤销公告再次撤销 → 409 already_revoked。
	resp = doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "rev-again", ExpectedVersion: 2})
	if resp.status != http.StatusConflict {
		t.Fatalf("double revoke: expected 409, got %d", resp.status)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "already_revoked" {
		t.Fatalf("expected already_revoked, got %s", body.Error.Code)
	}

	// 撤销后可以用更高版本重新发布（例如恢复公告）。
	resp = publish(t, publishRequest{
		IdempotencyKey: "rev-p3", BusinessKey: key, ExpectedVersion: 2, Kind: "recovery",
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("publish after revoke failed: %d %s", resp.status, resp.body)
	}
	waitConverged(t, key, 3)
}

// 正常路径：发布后全部订阅方收敛；指标与详情互相印证。
func TestHappyPathConvergence(t *testing.T) {
	before := scrapeMetrics(t)
	key := uniqueKey("happy")
	result := mustPublish(t, publishRequest{
		IdempotencyKey: "happy-1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
		Payload: map[string]any{"flight": "CZ3901"},
	})
	if len(result.Deliveries) != len(subscribers) {
		t.Fatalf("expected %d outbox deliveries, got %d", len(subscribers), len(result.Deliveries))
	}

	detail := waitConverged(t, key, 1)
	if detail.Summary.Acked != len(subscribers) {
		t.Fatalf("expected %d acked deliveries, got %+v", len(subscribers), detail.Summary)
	}
	for _, sub := range detail.Subscribers {
		d := findDelivery(t, detail, sub.Subscriber, 1)
		if d.Status != "acked" || d.AckedAt == nil || d.ReceiptID == nil {
			t.Fatalf("subscriber %s not properly acked: %+v", sub.Subscriber, d)
		}
		// 正常路径无重投：每个投递恰好收到一次 webhook。
		if got := simReceivedCount(t, d.DeliveryID); got != 1 {
			t.Fatalf("delivery %d received %d webhooks, want exactly 1", d.DeliveryID, got)
		}
	}

	after := scrapeMetrics(t)
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=accepted"); got != float64(len(subscribers)) {
		t.Fatalf("accepted receipts delta = %v, want %d", got, len(subscribers))
	}
	if got := metricDelta(before, after, "bulletin_deliveries_status", "status=acked"); got != float64(len(subscribers)) {
		t.Fatalf("acked gauge delta = %v, want %d", got, len(subscribers))
	}
}

// 分级重试：订阅方持续失败时按层级退避，最终转死信，且次数与层级间隔可观察。
func TestTieredRetryAndDeadLetter(t *testing.T) {
	const failingSub = "ground:SWP"
	simSetMode(t, failingSub, "fail")
	defer simSetMode(t, failingSub, "auto")

	before := scrapeMetrics(t)
	key := uniqueKey("retry")
	started := time.Now()
	mustPublish(t, publishRequest{
		IdempotencyKey: "retry-1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
	})

	var dead deliveryDetail
	waitFor(t, 45*time.Second, "delivery to become dead", func() bool {
		detail := getDetail(t, key)
		d := findDelivery(t, detail, failingSub, 1)
		if d.Status == "dead" {
			dead = d
			return true
		}
		return false
	})
	elapsed := time.Since(started)

	if dead.AttemptCount != dead.MaxAttempts {
		t.Fatalf("dead delivery attempts = %d, want %d", dead.AttemptCount, dead.MaxAttempts)
	}
	// 层级退避 200ms,500ms,1s,1s ≈ 2.7s；瞬时重试应远小于该值。
	if elapsed < 2500*time.Millisecond {
		t.Fatalf("retries were not tiered: dead after only %v", elapsed)
	}
	if dead.LastError == "" {
		t.Fatalf("dead delivery should record last_error")
	}

	after := scrapeMetrics(t)
	if got := metricDelta(before, after, "bulletin_delivery_send_total", "result=retry"); got < 4 {
		t.Fatalf("retry counter delta = %v, want >= 4", got)
	}
	if got := metricDelta(before, after, "bulletin_delivery_send_total", "result=dead"); got < 1 {
		t.Fatalf("dead counter delta = %v, want >= 1", got)
	}
	if got := metricValue(after, "bulletin_deliveries_status", "status=dead"); got < 1 {
		t.Fatalf("dead gauge = %v, want >= 1", got)
	}
}

// 重复回执：同一回执键返回首次结果；不同回执键的二次确认被拒绝，不产生二次生效。
func TestDuplicateReceipts(t *testing.T) {
	const heldSub = "airline:CA"
	simSetMode(t, heldSub, "hold")
	defer simSetMode(t, heldSub, "auto")

	key := uniqueKey("duprcpt")
	mustPublish(t, publishRequest{
		IdempotencyKey: "duprcpt-1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
	})

	// 等待被 hold 的投递送达（sent，未回执），且其余订阅方已自动确认——
	// 之后的指标基线才不会被在途回执污染。
	var sent deliveryDetail
	waitFor(t, 30*time.Second, "held delivery sent and others acked", func() bool {
		detail := getDetail(t, key)
		d := findDelivery(t, detail, heldSub, 1)
		if d.Status == "sent" && detail.Summary.Acked == len(subscribers)-1 {
			sent = d
			return true
		}
		return false
	})

	before := scrapeMetrics(t)
	receipt := receiptRequest{
		ReceiptKey: "manual-1", DeliveryID: sent.DeliveryID, Subscriber: heldSub,
		Version: 1, Verdict: "accepted",
	}
	resp := postReceipt(t, receipt)
	if resp.status != http.StatusCreated {
		t.Fatalf("first receipt failed: %d %s", resp.status, resp.body)
	}
	first := decode[receiptResult](t, resp)

	// 同一回执键重发 → 200，返回首次结果。
	resp = postReceipt(t, receipt)
	if resp.status != http.StatusOK {
		t.Fatalf("duplicate receipt should return 200, got %d: %s", resp.status, resp.body)
	}
	dup := decode[receiptResult](t, resp)
	if !dup.Duplicate || dup.ReceiptID != first.ReceiptID {
		t.Fatalf("duplicate receipt mismatch: %+v vs %+v", dup, first)
	}

	// 不同回执键对已确认投递 → 409 already_acknowledged（防止二次生效）。
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "manual-2", DeliveryID: sent.DeliveryID, Subscriber: heldSub,
		Version: 1, Verdict: "accepted",
	})
	if resp.status != http.StatusConflict {
		t.Fatalf("second effect should be rejected with 409, got %d", resp.status)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "already_acknowledged" {
		t.Fatalf("expected already_acknowledged, got %s", body.Error.Code)
	}

	// 订阅方不符 / 版本不符 → 409。
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "manual-3", DeliveryID: sent.DeliveryID, Subscriber: "airport:PEK",
		Version: 1, Verdict: "accepted",
	})
	if body := decode[errorBody](t, resp); resp.status != http.StatusConflict || body.Error.Code != "subscriber_mismatch" {
		t.Fatalf("expected subscriber_mismatch 409, got %d %s", resp.status, body.Error.Code)
	}
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "manual-4", DeliveryID: sent.DeliveryID, Subscriber: heldSub,
		Version: 99, Verdict: "accepted",
	})
	if body := decode[errorBody](t, resp); resp.status != http.StatusConflict || body.Error.Code != "version_mismatch" {
		t.Fatalf("expected version_mismatch 409, got %d %s", resp.status, body.Error.Code)
	}

	// 回执键被不同投递复用 → 409 receipt_key_conflict。
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "manual-1", DeliveryID: sent.DeliveryID + 999999, Subscriber: heldSub,
		Version: 1, Verdict: "accepted",
	})
	if body := decode[errorBody](t, resp); resp.status != http.StatusConflict || body.Error.Code != "receipt_key_conflict" {
		t.Fatalf("expected receipt_key_conflict 409, got %d %s", resp.status, body.Error.Code)
	}

	after := scrapeMetrics(t)
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=accepted"); got != 1 {
		t.Fatalf("accepted delta = %v, want exactly 1 (no duplicate effect)", got)
	}
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=duplicate"); got < 1 {
		t.Fatalf("duplicate delta = %v, want >= 1", got)
	}
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=rejected_already_acknowledged"); got < 1 {
		t.Fatalf("already_acknowledged delta = %v, want >= 1", got)
	}

	detail := getDetail(t, key)
	d := findDelivery(t, detail, heldSub, 1)
	if d.Status != "acked" || d.ReceiptID == nil || *d.ReceiptID != first.ReceiptID {
		t.Fatalf("delivery should be acked with the first receipt: %+v", d)
	}
	waitConverged(t, key, 1)
}

// 撤销后迟到确认：更高版本（撤销）发布后，旧版本投递被取代，
// 迟到的回执必须被 stale_version 拒绝，订阅记录不得提前完成。
func TestLateReceiptAfterRevocation(t *testing.T) {
	const heldSub = "ground:SWP"
	simSetMode(t, heldSub, "hold")
	defer simSetMode(t, heldSub, "auto")

	key := uniqueKey("late")
	mustPublish(t, publishRequest{
		IdempotencyKey: "late-1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
	})
	var sentV1 deliveryDetail
	waitFor(t, 30*time.Second, "v1 delivery sent", func() bool {
		d := findDelivery(t, getDetail(t, key), heldSub, 1)
		if d.Status == "sent" {
			sentV1 = d
			return true
		}
		return false
	})

	before := scrapeMetrics(t)

	// 撤销（v2，更高版本）→ v1 的未完成投递被取代。
	resp := doJSON(http.MethodPost, appBaseURL+"/api/v1/announcements/"+key+"/revocations",
		revokeRequest{IdempotencyKey: "late-rev", ExpectedVersion: 1, Note: "wrong flight"})
	if resp.status != http.StatusCreated {
		t.Fatalf("revoke failed: %d %s", resp.status, resp.body)
	}

	// 迟到确认 v1 → 409 stale_version。
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "late-manual", DeliveryID: sentV1.DeliveryID, Subscriber: heldSub,
		Version: 1, Verdict: "accepted",
	})
	if resp.status != http.StatusConflict {
		t.Fatalf("late receipt should be rejected with 409, got %d: %s", resp.status, resp.body)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "stale_version" {
		t.Fatalf("expected stale_version, got %s", body.Error.Code)
	}

	// 未知投递 → 404 unknown_delivery。
	resp = postReceipt(t, receiptRequest{
		ReceiptKey: "late-unknown", DeliveryID: 999999999, Subscriber: heldSub,
		Version: 1, Verdict: "accepted",
	})
	if resp.status != http.StatusNotFound {
		t.Fatalf("unknown delivery should return 404, got %d", resp.status)
	}
	if body := decode[errorBody](t, resp); body.Error.Code != "unknown_delivery" {
		t.Fatalf("expected unknown_delivery, got %s", body.Error.Code)
	}

	// 等撤销版本（v2）的投递也送达被 hold 的订阅方，再补发暂存回执。
	waitFor(t, 30*time.Second, "v2 revocation delivery sent", func() bool {
		d := findDelivery(t, getDetail(t, key), heldSub, 2)
		return d.Status == "sent"
	})

	// 模拟器补发暂存回执：v1 再次被拒（stale），v2（撤销版本）被接受。
	outcomes := simDrain(t, heldSub)
	var sawStaleForV1, sawAcceptedForV2 bool
	for _, o := range outcomes {
		if o.DeliveryID == sentV1.DeliveryID {
			sawStaleForV1 = o.HTTPStatus == http.StatusConflict && o.Code == "stale_version"
		} else if o.HTTPStatus == http.StatusCreated || o.HTTPStatus == http.StatusOK {
			sawAcceptedForV2 = true
		}
	}
	if !sawStaleForV1 {
		t.Fatalf("drained v1 receipt should be rejected as stale: %+v", outcomes)
	}
	if !sawAcceptedForV2 {
		t.Fatalf("drained v2 receipt should be accepted: %+v", outcomes)
	}

	detail := waitConverged(t, key, 2)
	v1 := findDelivery(t, detail, heldSub, 1)
	if v1.Status != "superseded" || v1.AckedAt != nil || v1.ReceiptID != nil {
		t.Fatalf("v1 delivery must stay superseded and unacked: %+v", v1)
	}
	v2 := findDelivery(t, detail, heldSub, 2)
	if v2.Status != "acked" || v2.Kind != "revocation" {
		t.Fatalf("v2 revocation delivery should be acked: %+v", v2)
	}

	after := scrapeMetrics(t)
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=rejected_stale_version"); got < 2 {
		t.Fatalf("stale rejections delta = %v, want >= 2 (manual + drained)", got)
	}
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=rejected_unknown_delivery"); got < 1 {
		t.Fatalf("unknown delivery rejections delta = %v, want >= 1", got)
	}
	if got := metricDelta(before, after, "bulletin_deliveries_superseded_total"); got < 1 {
		t.Fatalf("superseded counter delta = %v, want >= 1", got)
	}
}

// 投递器中断：webhook 在途时重启 app 进程，租约到期后未完成的投递被续跑，
// 不丢失、不重复生效（回执幂等），也不提前完成。
func TestDispatcherRestartRecovery(t *testing.T) {
	if appContainer == "" {
		t.Skip("APP_CONTAINER not set; cannot restart the app process")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available in tester")
	}

	// slow 模式：webhook 在途 3s，制造确定的中断窗口。
	simAllModes(t, "slow")
	defer simAllModes(t, "auto")

	key := uniqueKey("crash")
	result := mustPublish(t, publishRequest{
		IdempotencyKey: "crash-1", BusinessKey: key, ExpectedVersion: 0, Kind: "cancellation",
	})
	deliveryIDs := make([]int64, 0, len(result.Deliveries))
	for _, d := range result.Deliveries {
		deliveryIDs = append(deliveryIDs, d.DeliveryID)
	}

	// 等全部订阅方的 webhook 进入在途状态，然后重启进程。
	waitFor(t, 30*time.Second, "webhooks in flight", func() bool {
		for _, id := range deliveryIDs {
			if simReceivedCount(t, id) < 1 {
				return false
			}
		}
		return true
	})

	out, err := exec.Command("docker", "restart", appContainer).CombinedOutput()
	if err != nil {
		t.Fatalf("docker restart %s: %v\n%s", appContainer, err, out)
	}
	if err := waitForHTTP(appBaseURL+"/healthz", 90*time.Second); err != nil {
		t.Fatalf("app did not come back: %v", err)
	}

	// 重启后、租约到期前：投递绝不能处于 acked（没有提前完成）。
	detail := getDetail(t, key)
	for _, sub := range detail.Subscribers {
		d := findDelivery(t, detail, sub.Subscriber, 1)
		if d.Status == "acked" {
			t.Fatalf("delivery %d acked before redelivery completed: %+v", d.DeliveryID, d)
		}
	}

	baseline := scrapeMetrics(t)
	detail = waitConverged(t, key, 1)

	// 每个投递恰好一条回执（无重复生效），且至少一个在途投递被重投过（无丢失）。
	redelivered := 0
	for _, id := range deliveryIDs {
		if got := simReceivedCount(t, id); got >= 2 {
			redelivered++
		}
	}
	if redelivered == 0 {
		t.Fatalf("expected at least one redelivery after restart, got none")
	}
	for _, sub := range detail.Subscribers {
		d := findDelivery(t, detail, sub.Subscriber, 1)
		if d.Status != "acked" || d.ReceiptID == nil {
			t.Fatalf("delivery %d not acked exactly once: %+v", d.DeliveryID, d)
		}
	}

	after := scrapeMetrics(t)
	if got := metricDelta(baseline, after, "bulletin_dispatch_claims_total", "reclaimed=true"); got < 1 {
		t.Fatalf("expected reclaimed lease claims after restart, delta = %v", got)
	}
	if got := metricDelta(baseline, after, "bulletin_receipts_total", "result=accepted"); got != float64(len(subscribers)) {
		t.Fatalf("accepted delta after restart = %v, want exactly %d", got, len(subscribers))
	}
}

// 终态一致性：公告详情与 Prometheus 指标互相印证——
// 没有丢失（每个订阅方都有终态投递）、没有重复生效（accepted 与 acked 1:1）、
// 没有提前完成（没有在途投递滞留）。
func TestMetricsConsistency(t *testing.T) {
	before := scrapeMetrics(t)
	key := uniqueKey("final")
	mustPublish(t, publishRequest{
		IdempotencyKey: "final-1", BusinessKey: key, ExpectedVersion: 0, Kind: "recovery",
	})
	detail := waitConverged(t, key, 1)
	after := scrapeMetrics(t)

	n := float64(len(subscribers))
	if got := metricDelta(before, after, "bulletin_receipts_total", "result=accepted"); got != n {
		t.Fatalf("accepted receipts delta = %v, want %v", got, n)
	}
	// 计数器（事件）与 gauge（状态）必须同幅增长：回执是投递生效的唯一路径。
	if got := metricDelta(before, after, "bulletin_deliveries_status", "status=acked"); got != n {
		t.Fatalf("acked gauge delta = %v, want %v", got, n)
	}
	if got := metricDelta(before, after, "bulletin_deliveries_created_total"); got != n {
		t.Fatalf("created deliveries delta = %v, want %v", got, n)
	}
	for _, status := range []string{"pending", "leased", "sent"} {
		if got := metricValue(after, "bulletin_deliveries_status", "status="+status); got != 0 {
			t.Fatalf("status %s gauge = %v, want 0 (nothing left in flight)", status, got)
		}
	}
	if detail.Summary.Acked != len(subscribers) || detail.Summary.Pending != 0 ||
		detail.Summary.Leased != 0 || detail.Summary.Sent != 0 {
		t.Fatalf("final summary inconsistent: %+v", detail.Summary)
	}
	seenReceipts := map[int64]bool{}
	for _, sub := range detail.Subscribers {
		if !sub.Converged {
			t.Fatalf("subscriber %s not converged", sub.Subscriber)
		}
		d := findDelivery(t, detail, sub.Subscriber, 1)
		if d.ReceiptID == nil {
			t.Fatalf("subscriber %s acked without receipt", sub.Subscriber)
		}
		if seenReceipts[*d.ReceiptID] {
			t.Fatalf("receipt %d bound to two deliveries (duplicate effect)", *d.ReceiptID)
		}
		seenReceipts[*d.ReceiptID] = true
	}
}
