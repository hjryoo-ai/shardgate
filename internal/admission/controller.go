package admission

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/shard"
)

// ShardSource 는 배분에 필요한 대기열 정보를 읽어 준다(queue.Store 가 구현한다).
//
// 대기열 ZSET 을 직접 읽지 않고 이 인터페이스를 거치는 이유: ZSET 접근은
// internal/queue 한 곳에 모아 둔다. 두 패키지가 각자 읽기 시작하면 키 조립과
// 오류 처리가 갈라지고, 그게 곧 스키마가 두 벌이 되는 시작이다.
type ShardSource interface {
	Shards(ctx context.Context) ([]string, error)
	Sizes(ctx context.Context, shards []string) ([]int64, error)
}

// Controller 는 주기적으로 글로벌 admit rate 를 샤드 예산으로 배분한다(§3.4).
type Controller struct {
	store  *Store
	shards ShardSource
	health HealthChecker
	brk    *Breaker
	cfg    Config
	log    *slog.Logger
	met    *obs.Metrics
	now    func() time.Time
}

// NewController 는 배분 컨트롤러를 만든다.
func NewController(store *Store, shards ShardSource, health HealthChecker, log *slog.Logger, met *obs.Metrics) *Controller {
	if log == nil {
		log = slog.Default()
	}
	if health == nil {
		health = NoopHealth{}
	}
	return &Controller{
		store:  store,
		shards: shards,
		health: health,
		brk:    NewBreaker(store.cfg.BreakerFailures, store.cfg.BreakerCooldown),
		cfg:    store.cfg,
		log:    log,
		met:    met,
		now:    time.Now,
	}
}

// Breaker 는 내부 서킷브레이커를 노출한다(테스트/관측용).
func (c *Controller) Breaker() *Breaker { return c.brk }

// WithClock 은 시계를 갈아 끼운다(테스트용).
func (c *Controller) WithClock(fn func() time.Time) *Controller {
	c.now = fn
	return c
}

// CycleReport 는 배분 한 주기의 결과다.
type CycleReport struct {
	Shards    int
	Waiting   int64
	Granted   int64
	Factor    float64
	Breaker   BreakerState
	Budgets   []Budget
	Elapsed   time.Duration
	SkipEmpty int

	// Gated 는 추첨 구간이 아직 열려 있어 이번 주기를 통째로 건너뛰었음을 뜻한다.
	// 백프레셔(Factor=0)와 구분해 둔다 — 이쪽은 다운스트림이 아프다는 신호가 아니다.
	Gated bool
}

// Run 은 Interval 주기로 배분을 반복한다. ctx 가 끝나면 조용히 멈춘다.
func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	c.log.Info("admission controller started",
		slog.String("event", c.cfg.EventID),
		slog.Int("rate_per_min", c.cfg.RatePerMin),
		slog.Duration("interval", c.cfg.Interval),
		// 게이트 값은 반드시 로그에 남긴다. 설정으로만 켜지는 기능은 "켜졌다고
		// 믿었는데 값이 닿지 않은" 방식으로 조용히 죽는다(ROADMAP 결함 6).
		slog.Bool("after_lottery", c.cfg.AfterLottery),
		slog.Duration("min_dwell", c.cfg.MinDwell),
		slog.Int64("min_beats", c.cfg.MinBeats))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			rep, err := c.Cycle(ctx)
			if err != nil {
				// 배분 실패는 대기열을 멈추지 않는다. 다음 주기에 다시 시도한다.
				c.log.Error("admission cycle failed", slog.Any("error", err))
				continue
			}
			if rep.Gated {
				continue
			}
			c.log.Debug("admission cycle",
				slog.Int("shards", rep.Shards),
				slog.Int64("waiting", rep.Waiting),
				slog.Int64("granted", rep.Granted),
				slog.Float64("factor", rep.Factor),
				slog.String("breaker", string(rep.Breaker)),
				slog.Duration("elapsed", rep.Elapsed))
		}
	}
}

// Cycle 은 한 주기를 실행한다: 헬스 확인 → 총량 결정 → 샤드별 배분.
func (c *Controller) Cycle(ctx context.Context) (CycleReport, error) {
	start := time.Now()

	// 게이트는 헬스 확인보다 먼저 본다. 아직 아무도 내보내지 않는 구간에서
	// 다운스트림을 두드려 브레이커 상태를 흔들 이유가 없다.
	if until, gated := c.gateRemaining(); gated {
		c.log.Debug("admission gated by lottery window",
			slog.Duration("remaining", until))
		if c.met != nil {
			c.met.AdmitRate.Set(0)
		}
		return CycleReport{Gated: true, Breaker: c.brk.State(), Elapsed: time.Since(start)}, nil
	}

	factor := c.pressure(ctx)
	rep := CycleReport{Factor: factor, Breaker: c.brk.State()}

	shards, err := c.shards.Shards(ctx)
	if err != nil {
		return rep, fmt.Errorf("cycle: %w", err)
	}
	rep.Shards = len(shards)
	if len(shards) == 0 {
		rep.Elapsed = time.Since(start)
		return rep, nil
	}

	waiting, err := c.shards.Sizes(ctx, shards)
	if err != nil {
		return rep, fmt.Errorf("cycle: %w", err)
	}

	total := c.perCycle(factor)
	grants := apportion(total, weights(shards, waiting))

	rep.Budgets = make([]Budget, 0, len(shards))
	for i, sh := range shards {
		rep.Waiting += waiting[i]
		if waiting[i] == 0 && grants[i] == 0 {
			// 아무도 없고 줄 것도 없는 샤드는 왕복을 아낀다.
			rep.SkipEmpty++
			continue
		}
		b, err := c.store.Refill(ctx, sh, grants[i])
		if err != nil {
			// 샤드 하나가 실패해도 나머지 배분은 계속한다 — 한 샤드의 문제가
			// 이벤트 전체를 멈추면 샤딩으로 얻은 격리가 무의미해진다.
			c.log.Warn("refill failed", slog.String("shard", sh), slog.Any("error", err))
			continue
		}
		rep.Granted += grants[i]
		rep.Budgets = append(rep.Budgets, b)
	}

	if c.met != nil {
		c.met.AdmitRate.Set(float64(c.cfg.RatePerMin) * factor)
	}
	rep.Elapsed = time.Since(start)
	return rep, nil
}

// gateRemaining 은 추첨 구간이 닫힐 때까지 남은 시간과, 지금 배분을 막아야 하는지를 돌려준다.
//
// 이 게이트가 있는 이유(§12-7): 조치 파이프라인은 누적 점수로 움직이므로 격리에
// 70~80초가 걸리는데, 그 사이에도 admit 이 나가면 **먼저 입장한 봇은 격리될 기회
// 자체를 얻지 못한다.** 추첨 구간은 어차피 도착 순서에 이점을 주지 않는 구간이므로,
// 그 구간 동안 입장을 닫아 두면 순번의 공정성을 해치지 않고 탐지에 시간을 벌어 준다.
// 대가는 전체 대기 시간이 추첨 구간만큼 늘어나는 것이다.
//
// 예산을 0 으로 배분하는 대신 주기를 통째로 건너뛴다. refill_budget.lua 의 예산은
// 누적되고 TTL 도 갱신되므로, grant=0 으로 호출하면 남은 예산을 오히려 살려 둔다.
// 건너뛰면 예산 키는 TTL 로 스스로 사라진다.
func (c *Controller) gateRemaining() (time.Duration, bool) {
	if !c.cfg.AfterLottery || c.cfg.OpenAt.IsZero() {
		return 0, false
	}
	end := c.cfg.OpenAt.Add(c.cfg.LotteryWindow)
	if remaining := end.Sub(c.now()); remaining > 0 {
		return remaining, true
	}
	return 0, false
}

// perCycle 은 이번 주기에 내려보낼 총 인원이다.
func (c *Controller) perCycle(factor float64) int64 {
	perMin := float64(c.cfg.RatePerMin) * factor
	n := perMin * c.cfg.Interval.Minutes()
	if n <= 0 {
		return 0
	}
	return int64(math.Round(n))
}

// pressure 는 다운스트림 상태를 0~1 의 감속 계수로 바꾼다(§7 백프레셔).
//
//	열림   → 0    입장 정지. 대기열과 순번은 그대로 유지된다.
//	반열림 → 최소치로 조심스럽게 재개
//	닫힘   → 응답이 느려진 정도에 비례해 1 → 최소치로 선형 감속
func (c *Controller) pressure(ctx context.Context) float64 {
	state := c.brk.State()
	if state == BreakerOpen {
		return 0
	}

	latency, err := c.health.Check(ctx)
	if err != nil {
		c.brk.Failure()
		c.log.Warn("downstream health check failed",
			slog.Any("error", err), slog.String("breaker", string(c.brk.State())))
		if c.brk.State() == BreakerOpen {
			return 0
		}
		return c.cfg.BackpressureMin
	}
	c.brk.Success()

	if state == BreakerHalfOpen {
		return c.cfg.BackpressureMin
	}
	return c.latencyFactor(latency)
}

// latencyFactor 는 헬스 응답 지연을 감속 계수로 바꾼다.
// 타임아웃의 절반까지는 정상으로 보고, 그 뒤로 최소치까지 선형으로 줄인다.
func (c *Controller) latencyFactor(latency time.Duration) float64 {
	budget := c.healthBudget()
	if budget <= 0 {
		return 1
	}
	half := budget / 2
	if latency <= half {
		return 1
	}
	if latency >= budget {
		return c.cfg.BackpressureMin
	}
	ratio := float64(latency-half) / float64(budget-half)
	return 1 - ratio*(1-c.cfg.BackpressureMin)
}

func (c *Controller) healthBudget() time.Duration {
	if h, ok := c.health.(*HTTPHealth); ok {
		return h.Timeout
	}
	return 0
}

// weights 는 샤드별 배분 가중치다.
//
// 기본은 잔여 인원 비례 — 사람이 많은 샤드가 더 빨리 빠져야 전체 대기 시간이 고르다.
//
// **greylist 샤드에는 0 을 준다.** 원안(§3.4)은 "가중치 하향"이었는데, 그건 greylist
// 에서 직접 입장하는 경로가 있다는 전제에서만 말이 된다. §4 의 사다리에는 그런 경로가
// 없다 — greylist 를 벗어나는 길은 재챌린지 통과로 **원 샤드에 복귀**하는 것뿐이고
// (rechallenge.lua), 복귀하는 순간 그 사람은 원 샤드 예산을 쓴다. 그러니 greylist
// 샤드에 예산을 내려보내면 아무도 쓸 수 없는 자리가 매 주기 사라진다.
//
// 지금은 greylist 샤드가 `shards:{event}` 에 등록되지도 않아 이 분기에 닿지 않지만,
// 등록 경로가 생겨도 자리를 태우지 않도록 여기서 0 을 못박는다.
func weights(shards []string, waiting []int64) []float64 {
	w := make([]float64, len(shards))
	for i, sh := range shards {
		if shard.IsGreylist(sh) {
			continue
		}
		w[i] = float64(waiting[i])
	}
	return w
}

// apportion 은 총량을 가중치에 따라 정수로 나눈다(최대잔여법).
//
// 단순 반올림을 쓰면 나머지가 사라져 실제 배분 합이 총량보다 작아지고,
// 그 차이가 매 주기 누적되면 실효 admit rate 가 설정값보다 계속 낮아진다.
func apportion(total int64, w []float64) []int64 {
	out := make([]int64, len(w))
	if total <= 0 || len(w) == 0 {
		return out
	}

	sum := 0.0
	for _, v := range w {
		if v > 0 {
			sum += v
		}
	}
	if sum <= 0 {
		return out
	}

	type rem struct {
		idx  int
		frac float64
	}
	rems := make([]rem, 0, len(w))
	var assigned int64

	for i, v := range w {
		if v <= 0 {
			continue
		}
		exact := float64(total) * v / sum
		n := int64(exact)
		out[i] = n
		assigned += n
		rems = append(rems, rem{idx: i, frac: exact - float64(n)})
	}

	// 남은 몫은 잔여가 큰 순서로 하나씩. 동률이면 샤드 순서로 안정적으로 정한다.
	sort.SliceStable(rems, func(a, b int) bool { return rems[a].frac > rems[b].frac })
	for i := 0; assigned < total && len(rems) > 0; i++ {
		out[rems[i%len(rems)].idx]++
		assigned++
	}
	return out
}
