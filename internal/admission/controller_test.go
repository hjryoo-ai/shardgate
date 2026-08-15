package admission

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestApportion(t *testing.T) {
	tests := []struct {
		name  string
		total int64
		w     []float64
		want  []int64
	}{
		{"균등", 100, []float64{1, 1, 1, 1}, []int64{25, 25, 25, 25}},
		{"인원 비례", 100, []float64{90, 10}, []int64{90, 10}},
		{"빈 샤드는 0", 100, []float64{50, 0, 50}, []int64{50, 0, 50}},
		{"총량 0", 0, []float64{1, 1}, []int64{0, 0}},
		{"가중치 전부 0", 100, []float64{0, 0}, []int64{0, 0}},
		{"샤드 없음", 100, nil, nil},
		{"총량이 샤드보다 적다", 2, []float64{1, 1, 1}, []int64{1, 1, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := apportion(tc.total, tc.w)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("= %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// 나머지를 버리면 실효 admit rate 가 설정값보다 계속 낮아진다.
func TestApportionConservesTheTotal(t *testing.T) {
	tests := []struct {
		name  string
		total int64
		w     []float64
	}{
		{"나눠떨어지지 않음", 100, []float64{3, 3, 3}},
		{"소수 비율", 1000, []float64{7, 11, 13, 17, 19}},
		{"큰 편차", 250, []float64{1, 1, 1, 1, 996}},
		{"샤드가 총량보다 많다", 5, []float64{1, 1, 1, 1, 1, 1, 1, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := apportion(tc.total, tc.w)
			var sum int64
			for _, n := range got {
				if n < 0 {
					t.Fatalf("negative grant in %v", got)
				}
				sum += n
			}
			if sum != tc.total {
				t.Fatalf("sum = %d, want %d (%v)", sum, tc.total, got)
			}
		})
	}
}

// greylist 샤드는 예산을 한 명분도 받지 않는다.
//
// greylist 를 벗어나는 길은 재챌린지 통과로 **원 샤드에 복귀**하는 것뿐이고(§4),
// 복귀한 사람은 원 샤드 예산을 쓴다. greylist 샤드에 예산을 내려보내면 아무도
// 쓸 수 없는 자리가 매 주기 사라진다 — 그 자리는 사람의 자리다.
func TestGreylistShardsGetNoBudget(t *testing.T) {
	shards := []string{"s0001", "g0001"}
	waiting := []int64{1000, 1000}

	w := weights(shards, waiting)
	if w[0] != 1000 {
		t.Fatalf("normal weight = %v, want 1000", w[0])
	}
	if w[1] != 0 {
		t.Fatalf("greylist weight = %v, want 0", w[1])
	}

	grants := apportion(1000, w)
	if grants[1] != 0 {
		t.Fatalf("greylist shard got %d, want 0", grants[1])
	}
	if grants[0] != 1000 {
		t.Fatalf("normal shard got %d, want 1000 (전량이 원 샤드로 가야 한다)", grants[0])
	}
	if grants[0]+grants[1] != 1000 {
		t.Fatalf("grants = %v, want sum 1000", grants)
	}
}

func TestPerCycle(t *testing.T) {
	tests := []struct {
		name     string
		rate     int
		interval time.Duration
		factor   float64
		want     int64
	}{
		{"3000/분, 5초 주기", 3000, 5 * time.Second, 1, 250},
		{"절반 감속", 3000, 5 * time.Second, 0.5, 125},
		{"정지", 3000, 5 * time.Second, 0, 0},
		{"1분 주기", 3000, time.Minute, 1, 3000},
		{"음수 계수는 0", 3000, 5 * time.Second, -1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{cfg: Config{RatePerMin: tc.rate, Interval: tc.interval}}
			if got := c.perCycle(tc.factor); got != tc.want {
				t.Fatalf("= %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLatencyFactor(t *testing.T) {
	c := &Controller{
		cfg:    Config{BackpressureMin: 0.1},
		health: &HTTPHealth{Timeout: 2 * time.Second},
	}
	tests := []struct {
		name    string
		latency time.Duration
		want    float64
	}{
		{"빠르면 감속 없음", 100 * time.Millisecond, 1},
		{"절반까지는 정상", time.Second, 1},
		{"중간", 1500 * time.Millisecond, 0.55},
		{"타임아웃이면 최소치", 2 * time.Second, 0.1},
		{"타임아웃 초과", 5 * time.Second, 0.1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := c.latencyFactor(tc.latency)
			if got < tc.want-1e-9 || got > tc.want+1e-9 {
				t.Fatalf("= %v, want %v", got, tc.want)
			}
		})
	}

	// 헬스 엔드포인트가 없으면 감속 근거도 없다.
	noop := &Controller{cfg: Config{BackpressureMin: 0.1}, health: NoopHealth{}}
	if got := noop.latencyFactor(time.Hour); got != 1 {
		t.Fatalf("noop factor = %v, want 1", got)
	}
}

type stubHealth struct {
	latency time.Duration
	err     error
}

func (s stubHealth) Check(context.Context) (time.Duration, error) { return s.latency, s.err }

// 다운스트림이 죽으면 입장을 멈춘다. 대기열은 그대로 유지된다 — 멈추는 건 배분뿐이다.
func TestPressureOpensAndRecovers(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	health := &stubHealth{err: errors.New("connection refused")}
	c := &Controller{
		cfg:    Config{BackpressureMin: 0.1, BreakerFailures: 2, BreakerCooldown: 30 * time.Second},
		health: health,
		log:    discardLogger(),
	}
	c.brk = NewBreaker(c.cfg.BreakerFailures, c.cfg.BreakerCooldown).WithClock(clock)

	ctx := context.Background()

	// 1회 실패: 아직 닫혀 있고 최소치로 감속만 한다.
	if got := c.pressure(ctx); got != 0.1 {
		t.Fatalf("after 1 failure factor = %v, want 0.1", got)
	}
	if got := c.brk.State(); got != BreakerClosed {
		t.Fatalf("breaker = %s, want closed", got)
	}

	// 임계값 도달: 열린다 → 배분 정지.
	if got := c.pressure(ctx); got != 0 {
		t.Fatalf("after 2 failures factor = %v, want 0", got)
	}
	if got := c.brk.State(); got != BreakerOpen {
		t.Fatalf("breaker = %s, want open", got)
	}

	// 열린 동안에는 헬스 체크조차 하지 않는다(죽은 다운스트림을 계속 두드리지 않는다).
	if got := c.pressure(ctx); got != 0 {
		t.Fatalf("while open factor = %v, want 0", got)
	}

	// cooldown 경과 → 반열림 → 최소치로 조심스럽게 재개.
	now = now.Add(31 * time.Second)
	health.err = nil
	if got := c.pressure(ctx); got != 0.1 {
		t.Fatalf("half-open factor = %v, want 0.1", got)
	}

	// 성공했으니 닫히고 정상 속도로 돌아온다.
	if got := c.brk.State(); got != BreakerClosed {
		t.Fatalf("breaker = %s, want closed", got)
	}
	if got := c.pressure(ctx); got != 1 {
		t.Fatalf("recovered factor = %v, want 1", got)
	}
}

func TestBreakerHalfOpenFailureReopensImmediately(t *testing.T) {
	now := time.Now()
	b := NewBreaker(3, 10*time.Second).WithClock(func() time.Time { return now })

	for range 3 {
		b.Failure()
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("state = %s, want open", got)
	}

	now = now.Add(11 * time.Second)
	if got := b.State(); got != BreakerHalfOpen {
		t.Fatalf("state = %s, want half_open", got)
	}

	// 반열림에서의 실패는 임계값을 다시 채우길 기다리지 않고 즉시 다시 연다.
	b.Failure()
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("state = %s, want open", got)
	}
}

func TestBreakerSuccessResetsFailures(t *testing.T) {
	b := NewBreaker(3, time.Second)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("state = %s, want closed — 연속 실패가 아니면 열리면 안 된다", got)
	}
}

// 추첨 구간 게이트(§3.4). 켜져 있으면 LotteryEnd 전에는 한 명분도 내려보내지 않는다.
func TestGateRemaining(t *testing.T) {
	open := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	window := 2 * time.Minute

	tests := []struct {
		name      string
		on        bool
		openAt    time.Time
		now       time.Time
		wantGated bool
		wantLeft  time.Duration
	}{
		{"꺼져 있으면 항상 통과", false, open, open.Add(time.Second), false, 0},
		{"오픈 직후에는 닫힘", true, open, open.Add(time.Second), true, window - time.Second},
		{"추첨 구간 끝나기 1초 전", true, open, open.Add(window - time.Second), true, time.Second},
		{"정확히 끝나는 순간 열림", true, open, open.Add(window), false, 0},
		{"그 뒤로는 계속 열림", true, open, open.Add(time.Hour), false, 0},
		{"오픈 시각 이전(사전 대기)", true, open, open.Add(-time.Minute), true, window + time.Minute},
		// OpenAt 이 없으면 추첨 구간 자체가 없다(queue.LotteryEnd 와 같은 판단).
		// 게이트만 걸리고 영원히 안 열리는 사고를 막는다.
		{"OpenAt 미설정이면 게이트도 없다", true, time.Time{}, open, false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{
				cfg: Config{AfterLottery: tc.on, OpenAt: tc.openAt, LotteryWindow: window},
				now: func() time.Time { return tc.now },
			}
			left, gated := c.gateRemaining()
			if gated != tc.wantGated {
				t.Fatalf("gated = %v, want %v", gated, tc.wantGated)
			}
			if left != tc.wantLeft {
				t.Fatalf("remaining = %v, want %v", left, tc.wantLeft)
			}
		})
	}
}

// 게이트가 걸린 주기는 Redis 도 다운스트림도 건드리지 않는다.
//
// 예산을 0 으로 배분하는 게 아니라 주기를 건너뛴다는 것이 요점이다. refill_budget.lua
// 의 예산은 누적이고 TTL 도 갱신되므로, grant=0 으로 호출하면 이전 주기에 남은 예산을
// 오히려 살려 둔다 — 게이트가 걸린 채로 입장이 새는 길이 된다.
func TestGatedCycleTouchesNothing(t *testing.T) {
	open := time.Now()
	shards := &countingShards{}
	health := &countingHealth{}
	c := &Controller{
		cfg: Config{
			AfterLottery: true, OpenAt: open, LotteryWindow: time.Minute,
			BreakerFailures: 2, BreakerCooldown: time.Second,
		},
		shards: shards,
		health: health,
		log:    discardLogger(),
		now:    func() time.Time { return open.Add(time.Second) },
	}
	c.brk = NewBreaker(c.cfg.BreakerFailures, c.cfg.BreakerCooldown)

	rep, err := c.Cycle(context.Background())
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if !rep.Gated {
		t.Fatal("rep.Gated = false, want true")
	}
	if rep.Granted != 0 {
		t.Fatalf("granted = %d, want 0", rep.Granted)
	}
	if shards.calls != 0 {
		t.Fatalf("shard 조회 %d회 — 게이트 구간에는 Redis 를 읽지 않는다", shards.calls)
	}
	if health.calls != 0 {
		t.Fatalf("헬스 체크 %d회 — 아무도 안 내보내는 구간에 다운스트림을 두드리지 않는다", health.calls)
	}

	// 추첨 구간이 지나면 평소대로 돈다.
	c.now = func() time.Time { return open.Add(2 * time.Minute) }
	rep, err = c.Cycle(context.Background())
	if err != nil {
		t.Fatalf("cycle after gate: %v", err)
	}
	if rep.Gated {
		t.Fatal("추첨 구간이 지났는데도 게이트가 걸렸다")
	}
	if shards.calls == 0 || health.calls == 0 {
		t.Fatalf("게이트가 열린 뒤에도 조회가 없다 (shards=%d health=%d)", shards.calls, health.calls)
	}
}

type countingShards struct{ calls int }

func (s *countingShards) Shards(context.Context) ([]string, error) {
	s.calls++
	return nil, nil
}

func (s *countingShards) Sizes(context.Context, []string) ([]int64, error) { return nil, nil }

type countingHealth struct{ calls int }

func (h *countingHealth) Check(context.Context) (time.Duration, error) {
	h.calls++
	return 0, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
