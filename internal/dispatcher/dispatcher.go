// Package dispatcher 是后台投递器：按租约从事务性发件箱领取投递任务，
// 调用订阅方 webhook，失败按层级退避重试，超限转死信。
//
// 崩溃与重启语义：投递只在数据库状态机上推进，进程死亡不会丢失任务——
// 租约到期后，存活的（或重启后的）投递器会重新领取未完成的投递继续执行。
// 发送成功与状态落库之间崩溃会导致订阅方收到重复 webhook（at-least-once），
// 由回执幂等键保证不产生二次生效。
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/retry"
)

type Config struct {
	Tick        time.Duration // 领取间隔
	BatchSize   int           // 单次领取上限
	LeaseTTL    time.Duration // 租约时长；到期未完成的投递可被重新领取
	RetryTiers  []time.Duration
	SendTimeout time.Duration
}

type Delivery struct {
	ID                    int64
	AnnouncementVersionID int64
	BusinessKey           string
	SubscriberID          int64
	SubscriberName        string
	CallbackURL           string
	Version               int64
	Kind                  string
	AttemptCount          int
	MaxAttempts           int
	Payload               []byte
	Note                  string
	PublishedAt           time.Time
	Reclaimed             bool // 从过期租约中回收而来（即此前有投递器中断）
}

type Dispatcher struct {
	pool     *pgxpool.Pool
	cfg      Config
	metrics  *metrics.Metrics
	logger   *slog.Logger
	workerID string
	sender   *Sender
	wg       sync.WaitGroup
}

func New(pool *pgxpool.Pool, cfg Config, workerID string, m *metrics.Metrics, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		pool:     pool,
		cfg:      cfg,
		metrics:  m,
		logger:   logger.With("component", "dispatcher", "worker", workerID),
		workerID: workerID,
		sender:   NewSender(cfg.SendTimeout),
	}
}

// Run 阻塞运行直到 ctx 取消；退出前等待在途投递收尾（其 ctx 已取消，会立即放弃）。
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.Tick)
	defer ticker.Stop()
	d.dispatchOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			d.wg.Wait()
			d.logger.Info("dispatcher stopped")
			return
		case <-ticker.C:
			d.dispatchOnce(ctx)
		}
	}
}

func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	deliveries, err := d.claim(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			d.logger.Error("claim failed", "error", err)
		}
		return
	}
	for i := range deliveries {
		d.wg.Add(1)
		go func(del Delivery) {
			defer d.wg.Done()
			d.process(ctx, del)
		}(deliveries[i])
	}
}

// claim 用 SKIP LOCKED 批量领取：可领取的是到期的 pending 投递，
// 以及租约已过期的 leased 投递（持有者已崩溃或被重启）。
func (d *Dispatcher) claim(ctx context.Context) ([]Delivery, error) {
	rows, err := d.pool.Query(ctx,
		`WITH picked AS (
		     SELECT id, (status = 'leased') AS reclaimed
		     FROM deliveries
		     WHERE (status = 'pending' AND next_attempt_at <= now())
		        OR (status = 'leased' AND lease_expires_at < now())
		     ORDER BY id
		     LIMIT $1
		     FOR UPDATE SKIP LOCKED
		 )
		 UPDATE deliveries d
		 SET status = 'leased', lease_owner = $2,
		     lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		 FROM picked p
		 WHERE d.id = p.id
		 RETURNING d.id, d.announcement_version_id, d.business_key, d.subscriber_id,
		           d.version, d.kind, d.attempt_count, d.max_attempts, p.reclaimed`,
		d.cfg.BatchSize, d.workerID, d.cfg.LeaseTTL.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}
	defer rows.Close()

	claimed := []Delivery{}
	ids := []int64{}
	for rows.Next() {
		var del Delivery
		if err := rows.Scan(&del.ID, &del.AnnouncementVersionID, &del.BusinessKey, &del.SubscriberID,
			&del.Version, &del.Kind, &del.AttemptCount, &del.MaxAttempts, &del.Reclaimed); err != nil {
			return nil, fmt.Errorf("scan claimed delivery: %w", err)
		}
		claimed = append(claimed, del)
		ids = append(ids, del.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return nil, nil
	}

	// 取回 webhook 所需的负载与订阅方回调地址。
	detailRows, err := d.pool.Query(ctx,
		`SELECT d.id, s.name, s.callback_url, av.payload, av.note, av.created_at
		 FROM deliveries d
		 JOIN subscribers s ON s.id = d.subscriber_id
		 JOIN announcement_versions av ON av.id = d.announcement_version_id
		 WHERE d.id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("load claimed details: %w", err)
	}
	defer detailRows.Close()
	byID := map[int64]Delivery{}
	for _, del := range claimed {
		byID[del.ID] = del
	}
	for detailRows.Next() {
		var id int64
		var name, callbackURL, note string
		var payload []byte
		var publishedAt time.Time
		if err := detailRows.Scan(&id, &name, &callbackURL, &payload, &note, &publishedAt); err != nil {
			return nil, fmt.Errorf("scan claimed detail: %w", err)
		}
		del := byID[id]
		del.SubscriberName = name
		del.CallbackURL = callbackURL
		del.Payload = payload
		del.Note = note
		del.PublishedAt = publishedAt
		byID[id] = del
	}
	for i := range claimed {
		claimed[i] = byID[claimed[i].ID]
		reclaimed := "false"
		if claimed[i].Reclaimed {
			reclaimed = "true"
		}
		d.metrics.DispatchClaims.WithLabelValues(reclaimed).Inc()
	}
	return claimed, nil
}

// process 投递单条任务：成功则落库为 sent（等待回执）；失败按层级退避重排或转死信。
func (d *Dispatcher) process(ctx context.Context, del Delivery) {
	log := d.logger.With("delivery_id", del.ID, "business_key", del.BusinessKey,
		"subscriber", del.SubscriberName, "version", del.Version)

	err := d.sender.Send(ctx, del)
	if err != nil {
		if ctx.Err() != nil {
			// 进程正在退出：不消耗重试次数，租约自然到期后由存活的投递器续跑。
			d.metrics.DeliverySendTotal.WithLabelValues("shutdown").Inc()
			log.Info("send aborted by shutdown; lease left to expire")
			return
		}
		d.fail(ctx, del, err, log)
		return
	}

	tag, err := d.pool.Exec(ctx,
		`UPDATE deliveries
		 SET status = 'sent', sent_at = now(), lease_owner = NULL, lease_expires_at = NULL,
		     last_error = '', updated_at = now()
		 WHERE id = $1 AND status = 'leased' AND lease_owner = $2`,
		del.ID, d.workerID)
	if err != nil {
		d.metrics.DeliverySendTotal.WithLabelValues("lost_lease").Inc()
		log.Error("mark sent failed", "error", err)
		return
	}
	if tag.RowsAffected() == 0 {
		// 发送途中被更高版本取代：webhook 已发出但永不会被确认，
		// 订阅方随后的回执会被 stale_version 拒绝，状态机保持一致。
		d.metrics.DeliverySendTotal.WithLabelValues("superseded_during_send").Inc()
		log.Info("delivery superseded while sending")
		return
	}
	d.metrics.DeliverySendTotal.WithLabelValues("sent").Inc()
	log.Info("delivered, awaiting receipt", "attempt", del.AttemptCount+1, "reclaimed", del.Reclaimed)
}

// fail 记录一次失败：次数+1，按层级计算下次可领取时间；超限转死信。
func (d *Dispatcher) fail(ctx context.Context, del Delivery, sendErr error, log *slog.Logger) {
	attempt := del.AttemptCount + 1
	delay := retry.Delay(d.cfg.RetryTiers, attempt)
	var status string
	err := d.pool.QueryRow(ctx,
		`UPDATE deliveries
		 SET attempt_count = attempt_count + 1,
		     status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'dead' ELSE 'pending' END,
		     next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts
		                            THEN next_attempt_at
		                            ELSE now() + make_interval(secs => $3) END,
		     lease_owner = NULL, lease_expires_at = NULL, last_error = $4, updated_at = now()
		 WHERE id = $1 AND status = 'leased' AND lease_owner = $2
		 RETURNING status`,
		del.ID, d.workerID, delay.Seconds(), sendErr.Error()).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		d.metrics.DeliverySendTotal.WithLabelValues("lost_lease").Inc()
		log.Info("delivery superseded while recording failure")
		return
	}
	if err != nil {
		d.metrics.DeliverySendTotal.WithLabelValues("lost_lease").Inc()
		log.Error("record failure failed", "error", err)
		return
	}
	if status == "dead" {
		d.metrics.DeliverySendTotal.WithLabelValues("dead").Inc()
		log.Warn("delivery exhausted retries and is dead", "attempts", attempt, "error", sendErr)
		return
	}
	d.metrics.DeliverySendTotal.WithLabelValues("retry").Inc()
	log.Warn("send failed, scheduled retry", "attempt", attempt, "retry_in", delay.String(), "error", sendErr)
}
