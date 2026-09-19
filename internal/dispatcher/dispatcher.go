// Package dispatcher 实现发件箱后台投递器：租约领取、HTTP 回调、分级重试、
// 死信与优雅停机。投递器是无状态的——所有进行中状态都在 PostgreSQL 行上，
// 因此进程重启后新的投递器实例会通过过期租约回收继续未完成的投递。
package dispatcher

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"example.com/disruption-bulletins/internal/domain"
	"example.com/disruption-bulletins/internal/metrics"
	"example.com/disruption-bulletins/internal/store"
)

// Config 是投递器的可调参数。
type Config struct {
	PollInterval  time.Duration // 无任务时两轮领取之间的休眠
	BatchSize     int           // 单轮最多领取行数
	LeaseDuration time.Duration // 租约时长；超过后行可被其他实例回收
	HTTPTimeout   time.Duration // 单次回调超时
	Concurrency   int           // 回调并发度
	// Backoffs 是第 N 次失败（从 1 起）后到下次尝试的基础退避；超出表长取最后一档。
	Backoffs []time.Duration
	Jitter   float64 // 退避抖动比例，0.2 表示 ±20%
}

// DefaultConfig 返回生产默认参数。
func DefaultConfig() Config {
	return Config{
		PollInterval:  500 * time.Millisecond,
		BatchSize:     50,
		LeaseDuration: 30 * time.Second,
		HTTPTimeout:   10 * time.Second,
		Concurrency:   8,
		Backoffs: []time.Duration{
			2 * time.Second, 5 * time.Second, 15 * time.Second,
			30 * time.Second, time.Minute, 2 * time.Minute,
		},
		Jitter: 0.2,
	}
}

// Dispatcher 轮询发件箱并把公告版本投递给订阅方。
type Dispatcher struct {
	repo    *store.Repo
	metrics *metrics.Set
	cfg     Config
	client  *http.Client
	worker  string

	wg sync.WaitGroup
}

func New(repo *store.Repo, m *metrics.Set, cfg Config) *Dispatcher {
	return &Dispatcher{
		repo:    repo,
		metrics: m,
		cfg:     cfg,
		client:  &http.Client{Timeout: cfg.HTTPTimeout},
		worker:  "worker-" + randomHex(4),
	}
}

// WorkerID 返回当前实例的租约令牌（测试与日志使用）。
func (d *Dispatcher) WorkerID() string { return d.worker }

// Run 阻塞运行投递器直到 ctx 被取消，随后等待所有进行中的回调收尾。
func (d *Dispatcher) Run(ctx context.Context) {
	sem := make(chan struct{}, d.cfg.Concurrency)

	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if !d.claimAndDeliver(ctx, sem) {
			// 上下文取消或连接池已关闭：收尾在飞回调后退出。
			d.wg.Wait()
			return
		}

		select {
		case <-ctx.Done():
			// 不再领取新任务，但等待已发出的回调 goroutine 结束。
			d.wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// RunBackground 以 goroutine 启动并返回停止函数；停止函数触发优雅停机，
// 等待所有已领取的回调结束后才返回。主要供测试与 main 使用。
func (d *Dispatcher) RunBackground(parent context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func (d *Dispatcher) claimAndDeliver(ctx context.Context, sem chan struct{}) bool {
	claims, err := d.repo.ClaimDue(ctx, d.cfg.BatchSize, d.worker, d.cfg.LeaseDuration)
	if err != nil {
		if errors.Is(err, context.Canceled) || isPoolClosed(err) {
			return false
		}
		log.Printf("dispatcher: claim failed: %v", err)
		return true
	}
	if len(claims) == 0 {
		return true
	}
	d.metrics.DispatchClaimed.Add(float64(len(claims)))

	for _, dlv := range claims {
		if dlv.Recovered {
			// 进程崩溃后继续未完成投递：这一行来自过期租约回收。
			d.metrics.DispatchLeaseRecycled.Inc()
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// 行已领取为 leased；优雅停机后它会留在 leased 状态，
			// 等租约过期被（本进程重启后的）投递器回收——不丢、不提前完成。
			return true
		}

		d.wg.Add(1)
		go func(dlv *domain.Delivery) {
			defer d.wg.Done()
			defer func() { <-sem }()
			d.deliver(ctx, dlv)
		}(dlv)
	}
	return true
}

// isPoolClosed 识别连接池已关闭的错误（停机阶段不再刷日志）。
func isPoolClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "closed pool")
}

// deliver 执行单次回调并依据结果做一次性状态迁移。
func (d *Dispatcher) deliver(ctx context.Context, dlv *domain.Delivery) {
	d.metrics.DispatchInFlight.Inc()
	defer d.metrics.DispatchInFlight.Dec()

	err := d.post(ctx, dlv)
	if err == nil {
		if mErr := d.repo.MarkDelivered(ctx, dlv.ID, d.worker); mErr != nil {
			// 租约易主：另一个投递器已经接手，本次“成功”绝不能覆盖它的状态。
			if !errors.Is(mErr, store.ErrLeaseLost) {
				log.Printf("dispatcher: mark delivered %s: %v", dlv.ID, mErr)
			}
			return
		}
		d.metrics.DispatchAttempts.WithLabelValues("success").Inc()
		return
	}

	if errors.Is(err, context.Canceled) {
		// 进程正在停机：不更新状态，保留租约等待回收。
		return
	}

	next := d.backoffFor(dlv.Attempts)
	status, mErr := d.repo.MarkFailure(ctx, dlv.ID, d.worker, err.Error(), time.Now().Add(next))
	if mErr != nil {
		if !errors.Is(mErr, store.ErrLeaseLost) {
			log.Printf("dispatcher: mark failure %s: %v", dlv.ID, mErr)
		}
		return
	}
	switch status {
	case domain.StatusDead:
		d.metrics.DispatchAttempts.WithLabelValues("dead").Inc()
		log.Printf("dispatcher: delivery %s (%s v%d -> %s) declared dead after %d attempts: %v",
			dlv.ID, dlv.BusinessKey, dlv.Version, dlv.CallbackURL, dlv.Attempts, err)
	default:
		d.metrics.DispatchAttempts.WithLabelValues("failure").Inc()
		d.metrics.DispatchRetries.Inc()
	}
}

func (d *Dispatcher) post(ctx context.Context, dlv *domain.Delivery) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dlv.CallbackURL, bytes.NewReader(dlv.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Delivery-Id", dlv.ID)
	req.Header.Set("X-Bulletin-Version", itoa(dlv.Version))

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return errors.New("subscriber responded with status " + resp.Status)
}

// backoffFor 返回第 attempts 次失败后的退避时长（含抖动）。
func (d *Dispatcher) backoffFor(attempts int) time.Duration {
	table := d.cfg.Backoffs
	if len(table) == 0 {
		return 5 * time.Second
	}
	idx := attempts - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(table) {
		idx = len(table) - 1
	}
	base := table[idx]
	if d.cfg.Jitter <= 0 {
		return base
	}
	// 在 [1-jitter, 1+jitter) 间抖动。
	factor := 1 + d.cfg.Jitter*(2*randomUnit()-1)
	return time.Duration(float64(base) * factor)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
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

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// randomUnit 返回 [0,1) 的伪随机数。
func randomUnit() float64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	// 取 53 位尾数。
	v := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
		uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 |
		uint64(b[6])<<48
	return float64(v&((1<<53)-1)) / (1 << 53)
}
