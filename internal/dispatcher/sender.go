package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Webhook 是投递给订阅方的公告载体。订阅方凭 delivery_id + version 回执。
type Webhook struct {
	DeliveryID  int64           `json:"delivery_id"`
	BusinessKey string          `json:"business_key"`
	Version     int64           `json:"version"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Note        string          `json:"note"`
	PublishedAt time.Time       `json:"published_at"`
	SentAt      time.Time       `json:"sent_at"`
}

type Sender struct {
	client *http.Client
}

func NewSender(timeout time.Duration) *Sender {
	return &Sender{client: &http.Client{Timeout: timeout}}
}

// Send 向订阅方回调地址 POST 公告；非 2xx 视为可重试失败。
func (s *Sender) Send(ctx context.Context, del Delivery) error {
	payload := del.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	body, err := json.Marshal(Webhook{
		DeliveryID:  del.ID,
		BusinessKey: del.BusinessKey,
		Version:     del.Version,
		Kind:        del.Kind,
		Payload:     payload,
		Note:        del.Note,
		PublishedAt: del.PublishedAt,
		SentAt:      time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal webhook: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, del.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Delivery-Id", fmt.Sprint(del.ID))
	req.Header.Set("X-Announcement-Version", fmt.Sprint(del.Version))
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("subscriber returned %s", resp.Status)
	}
	return nil
}
