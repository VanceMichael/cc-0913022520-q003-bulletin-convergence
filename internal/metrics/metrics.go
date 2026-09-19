// Package metrics 集中维护收敛中心的 Prometheus 指标。
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"example.com/disruption-bulletins/internal/domain"
)

// Set 持有服务全部自定义指标。
type Set struct {
	AnnouncementsPublished *prometheus.CounterVec
	PublishConflicts       prometheus.Counter
	IdempotentReplays      prometheus.Counter
	IdempotentRejected     *prometheus.CounterVec
	DispatchClaimed        prometheus.Counter
	DispatchAttempts       *prometheus.CounterVec // result: success|failure|dead
	DispatchRetries        prometheus.Counter
	DispatchLeaseRecycled  prometheus.Counter
	AcksRecorded           *prometheus.CounterVec // duplicate: true|false
	ReceiptsRejected       *prometheus.CounterVec // reason
	DeliveriesGauge        *prometheus.GaugeVec   // 按 status 从 DB 对账
	DispatchInFlight       prometheus.Gauge
}

// New 在给定注册表上构造指标。
func New(reg prometheus.Registerer) *Set {
	f := promauto.With(reg)
	return &Set{
		AnnouncementsPublished: f.NewCounterVec(prometheus.CounterOpts{
			Name: "bulletin_announcements_published_total",
			Help: "成功发布的公告版本数（幂等重放不计入）。",
		}, []string{"kind"}),
		PublishConflicts: f.NewCounter(prometheus.CounterOpts{
			Name: "bulletin_publish_version_conflicts_total",
			Help: "因前版号不匹配被拒绝的发布请求数（并发落败或撤销版本过低）。",
		}),
		IdempotentReplays: f.NewCounter(prometheus.CounterOpts{
			Name: "bulletin_idempotent_replays_total",
			Help: "携带已完成幂等键、返回首次结果的重复发布请求数。",
		}),
		IdempotentRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "bulletin_idempotent_rejected_total",
			Help: "幂等键占用失败的请求数。",
		}, []string{"reason"}), // reason: reused_different_payload | in_flight
		DispatchClaimed: f.NewCounter(prometheus.CounterOpts{
			Name: "bulletin_dispatch_claimed_total",
			Help: "投递器从发件箱领取的任务行数（含崩溃回收的行）。",
		}),
		DispatchAttempts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "bulletin_dispatch_attempts_total",
			Help: "对订阅方回调的 HTTP 尝试结果。",
		}, []string{"result"}), // success | failure | dead
		DispatchRetries: f.NewCounter(prometheus.CounterOpts{
			Name: "bulletin_dispatch_retries_scheduled_total",
			Help: "失败后进入分级退避、安排重试的任务数。",
		}),
		DispatchLeaseRecycled: f.NewCounter(prometheus.CounterOpts{
			Name: "bulletin_dispatch_lease_recycled_total",
			Help: "因租约过期被重新领取的任务数（投递器崩溃/重启后恢复）。",
		}),
		AcksRecorded: f.NewCounterVec(prometheus.CounterOpts{
			Name: "bulletin_receipts_acked_total",
			Help: "生效的订阅方回执数；duplicate=true 表示重复回执未产生新迁移。",
		}, []string{"duplicate"}),
		ReceiptsRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "bulletin_receipts_rejected_total",
			Help: "被拒绝的回执数。",
		}, []string{"reason"}), // unknown_version | obsolete_version | not_delivered | unknown_subscriber
		DeliveriesGauge: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bulletin_outbox_deliveries",
			Help: "发件箱当前行数（按投递状态），用于与公告详情核对收敛情况。",
		}, []string{"status"}),
		DispatchInFlight: f.NewGauge(prometheus.GaugeOpts{
			Name: "bulletin_dispatch_in_flight",
			Help: "当前投递器正在回调的任务数。",
		}),
	}
}

// DeliveryStatusLabels 返回全部可出现的状态标签，供 gauge 清零使用。
func DeliveryStatusLabels() []domain.DeliveryStatus {
	return []domain.DeliveryStatus{
		domain.StatusPending,
		domain.StatusLeased,
		domain.StatusRetryWait,
		domain.StatusDelivered,
		domain.StatusAcked,
		domain.StatusDead,
	}
}

// DeliveryCounter 是 gauge 刷新所需的最小仓储接口。
type DeliveryCounter interface {
	DeliveryCounts(ctx context.Context) (map[domain.DeliveryStatus]int, error)
}

// StartGaugeCollector 周期性从数据库刷新发件箱行数 gauge，直到 ctx 取消。
// 以 DB 为唯一事实源，因此即便进程在状态迁移中途崩溃，下一轮采集也会反映真实状态。
func (s *Set) StartGaugeCollector(ctx context.Context, repo DeliveryCounter, interval time.Duration) {
	// 启动时先把所有状态标签初始化为 0，保证指标在无数据时也出现。
	for _, st := range DeliveryStatusLabels() {
		s.DeliveriesGauge.WithLabelValues(string(st)).Set(0)
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.refreshGauge(ctx, repo)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshGauge(ctx, repo)
			}
		}
	}()
}

func (s *Set) refreshGauge(ctx context.Context, repo DeliveryCounter) {
	counts, err := repo.DeliveryCounts(ctx)
	if err != nil {
		return
	}
	for _, st := range DeliveryStatusLabels() {
		s.DeliveriesGauge.WithLabelValues(string(st)).Set(float64(counts[st]))
	}
}
