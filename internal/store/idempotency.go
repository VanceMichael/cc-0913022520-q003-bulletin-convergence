package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"example.com/disruption-bulletins/internal/domain"
)

// CachedResponse 是幂等键首次请求落库的响应，重放时原样返回。
type CachedResponse struct {
	Status int
	Body   json.RawMessage
}

// PublishOutcome 区分本次调用是真的执行了发布还是命中了首次结果缓存。
type PublishOutcome struct {
	Result *domain.PublishResult
	Replay *CachedResponse // 非 nil 表示这是重复请求，应原样返回缓存的首次响应
}

// PublishAnnouncementIdempotent 把「幂等键占用 → 版本校验 → 版本追加 → 发件箱
// fan-out → 幂等完成落盘」放进同一个事务：
//
//   - 键不存在          执行发布并缓存 201 响应；
//   - 键已完成且指纹一致 重放首次响应（不产生新版本、不产生新投递）；
//   - 键存在但指纹不同   domain.ErrDuplicateRequest。
//
// 因为占用行与业务变更同生共死，进程在任何时刻崩溃都只会整体回滚，
// 不存在“业务已提交但响应缓存丢失”的窗口，也不会留下 in_flight 残留；
// 并发同键请求由 INSERT 的唯一冲突 + 对方事务结束自然串行化。
// inFlightTTL 预留给未来跨语句占用场景，当前实现不产生未完成占用。
func (r *Repo) PublishAnnouncementIdempotent(
	ctx context.Context,
	key, fingerprint string,
	inFlightTTL time.Duration,
	in domain.PublishInput,
) (*PublishOutcome, error) {
	_ = inFlightTTL

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 并发同键请求在此等待先到者的事务结束。
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotent_requests(idempotency_key, request_fingerprint)
		VALUES ($1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING`, key, fingerprint)
	if err != nil {
		return nil, err
	}

	if tag.RowsAffected() == 0 {
		// 先到者事务已落定：读它的指纹与缓存结果。
		var existingFP, state string
		var cachedStatus *int
		var cachedBody []byte
		err = tx.QueryRow(ctx, `
			SELECT request_fingerprint, state, response_status, response_body
			FROM idempotent_requests WHERE idempotency_key=$1`, key,
		).Scan(&existingFP, &state, &cachedStatus, &cachedBody)
		if err != nil {
			return nil, err
		}
		if existingFP != fingerprint {
			return nil, domain.ErrDuplicateRequest
		}
		if state != "completed" || cachedStatus == nil {
			// 理论不可达：占用与完成在同一事务内，提交后必然 completed。
			return nil, errors.New("idempotency key in inconsistent state")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &PublishOutcome{Replay: &CachedResponse{Status: *cachedStatus, Body: cachedBody}}, nil
	}

	result, err := publishAnnouncementTx(ctx, tx, in)
	if err != nil {
		return nil, err // 回滚同时抹掉本次的幂等占用
	}

	body, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE idempotent_requests
		SET state='completed', response_status=201, response_body=$2, completed_at=now()
		WHERE idempotency_key=$1`, key, string(body)); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &PublishOutcome{Result: result}, nil
}
