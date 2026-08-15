package admission

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// BreakerState 는 서킷브레이커 상태다.
type BreakerState string

// 서킷브레이커 상태(§7 백프레셔).
const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

// Breaker 는 다운스트림이 무너졌을 때 입장을 멈추는 차단기다.
//
// 대기열은 계속 유지되고 순번도 그대로다 — 멈추는 것은 "내려보내는 양"뿐이다.
// 재고/결제가 죽은 상태에서 계속 입장시키면 사용자는 대기까지 하고 실패 화면을 본다.
type Breaker struct {
	mu        sync.Mutex
	state     BreakerState
	failures  int
	threshold int
	cooldown  time.Duration
	openedAt  time.Time
	now       func() time.Time
}

// NewBreaker 는 연속 실패 threshold 회에 열리고 cooldown 뒤 반열림으로 가는 차단기를 만든다.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 1
	}
	return &Breaker{state: BreakerClosed, threshold: threshold, cooldown: cooldown, now: time.Now}
}

// WithClock 은 시계를 갈아 끼운다(테스트용).
func (b *Breaker) WithClock(fn func() time.Time) *Breaker {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = fn
	return b
}

// State 는 현재 상태다. 열린 지 cooldown 이 지났으면 반열림으로 옮긴다.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = BreakerHalfOpen
	}
	return b.state
}

// Success 는 성공을 기록한다. 반열림에서 성공하면 닫힌다.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = BreakerClosed
}

// Failure 는 실패를 기록한다. 반열림에서의 실패는 즉시 다시 연다 —
// 아직 회복되지 않았다는 뜻이므로 임계값을 다시 채울 때까지 기다릴 이유가 없다.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == BreakerHalfOpen || b.failures >= b.threshold {
		b.state = BreakerOpen
		b.openedAt = b.now()
	}
}

// HealthChecker 는 다운스트림의 상태를 본다.
type HealthChecker interface {
	// Check 는 응답까지 걸린 시간을 돌려준다. 오류면 다운스트림이 아프다는 뜻이다.
	Check(ctx context.Context) (time.Duration, error)
}

// NoopHealth 는 헬스 엔드포인트가 설정되지 않았을 때 쓰는 무검사 구현이다.
type NoopHealth struct{}

// Check 는 언제나 정상을 보고한다.
func (NoopHealth) Check(context.Context) (time.Duration, error) { return 0, nil }

// HTTPHealth 는 mock shop 의 헬스 엔드포인트를 확인한다.
type HTTPHealth struct {
	URL     string
	Timeout time.Duration
	Client  *http.Client
}

// NewHTTPHealth 는 URL 이 비어 있으면 무검사 구현을 돌려준다.
func NewHTTPHealth(url string, timeout time.Duration) HealthChecker {
	if url == "" {
		return NoopHealth{}
	}
	return &HTTPHealth{URL: url, Timeout: timeout, Client: &http.Client{Timeout: timeout}}
}

// Check 는 헬스 엔드포인트를 호출하고 왕복 시간을 돌려준다.
func (h *HTTPHealth) Check(ctx context.Context) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, h.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("health request: %w", err)
	}

	start := time.Now()
	resp, err := h.Client.Do(req)
	if err != nil {
		return time.Since(start), fmt.Errorf("health check: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)
	if resp.StatusCode >= http.StatusInternalServerError {
		return elapsed, fmt.Errorf("health check: %w: status %d", errUnhealthy, resp.StatusCode)
	}
	return elapsed, nil
}

var errUnhealthy = errors.New("downstream unhealthy")
