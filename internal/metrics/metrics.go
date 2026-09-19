// Package metrics 汇总收敛中心的 Prometheus 指标。
// 计数器记录事件，状态 gauge 在抓取时实时查询数据库，二者可在集成测试中与公告详情互相印证。
package metrics

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "bulletin"

type Metrics struct {
	PublishTotal         *prometheus.CounterVec // kind, result
	DeliveriesCreated    prometheus.Counter
	DeliveriesSuperseded prometheus.Counter
	DispatchClaims       *prometheus.CounterVec // reclaimed
	DeliverySendTotal    *prometheus.CounterVec // result: sent|retry|dead|lost_lease|shutdown
	ReceiptsTotal        *prometheus.CounterVec // result
	registry             *prometheus.Registry
	pool                 *pgxpool.Pool
	statusDesc           *prometheus.Desc
}

func New(pool *pgxpool.Pool) *Metrics {
	m := &Metrics{
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "publish_total",
			Help: "公告发布/撤销请求结果（kind=cancellation|recovery|revocation, result=created|duplicate|conflict_version|conflict_idempotency|already_revoked|invalid|unknown_announcement）",
		}, []string{"kind", "result"}),
		DeliveriesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "deliveries_created_total",
			Help: "事务性发件箱生成的投递任务数",
		}),
		DeliveriesSuperseded: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "deliveries_superseded_total",
			Help: "被更高版本（含撤销）取代的未完成投递数",
		}),
		DispatchClaims: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "dispatch_claims_total",
			Help: "投递器领取的投递数（reclaimed=true 表示租约到期后被重新领取，即中断恢复路径）",
		}, []string{"reclaimed"}),
		DeliverySendTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "delivery_send_total",
			Help: "投递发送结果（sent|retry|dead|lost_lease|shutdown）",
		}, []string{"result"}),
		ReceiptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "receipts_total",
			Help: "回执处理结果（accepted|duplicate|rejected_unknown_delivery|rejected_stale_version|rejected_version_mismatch|rejected_subscriber_mismatch|rejected_not_delivered|rejected_already_acknowledged|rejected_key_conflict）",
		}, []string{"result"}),
		registry: prometheus.NewRegistry(),
		pool:     pool,
		statusDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "deliveries", "status"),
			"各状态投递数量的实时 gauge（pending|leased|sent|acked|superseded|dead）",
			[]string{"status"}, nil,
		),
	}
	m.registry.MustRegister(
		m.PublishTotal, m.DeliveriesCreated, m.DeliveriesSuperseded,
		m.DispatchClaims, m.DeliverySendTotal, m.ReceiptsTotal,
		prometheusCollector{m},
	)
	return m
}

func (m *Metrics) Handler() prometheus.Gatherer {
	return m.registry
}

// prometheusCollector 在每次抓取时查询 deliveries 状态分布。
type prometheusCollector struct{ m *Metrics }

func (c prometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.m.statusDesc
}

func (c prometheusCollector) Collect(ch chan<- prometheus.Metric) {
	rows, err := c.m.pool.Query(context.Background(),
		`SELECT status, count(*) FROM deliveries GROUP BY status`)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.m.statusDesc, err)
		return
	}
	defer rows.Close()
	seen := map[string]float64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			continue
		}
		seen[status] = float64(n)
	}
	for _, status := range []string{"pending", "leased", "sent", "acked", "superseded", "dead"} {
		ch <- prometheus.MustNewConstMetric(c.m.statusDesc, prometheus.GaugeValue, seen[status], status)
	}
}
