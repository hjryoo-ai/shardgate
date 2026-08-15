// Package admission 은 입장 제어를 담당한다(DESIGN.md §3.4).
//
// 대기열이 존재하는 이유는 원 서버(재고/결제)가 폭주를 직접 맞지 않게 하는 것이다.
// 그러니 "얼마나 내려보낼지"는 대기 인원이 아니라 다운스트림이 견딜 수 있는 양이
// 정해야 한다. 이 패키지는 그 양을 글로벌 admit rate 로 정하고, 샤드별 예산으로
// 쪼개 배분한 뒤, 예산 차감과 상태 전이를 Lua 한 번으로 끝낸다(불변식 1).
package admission

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/redisx"
	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"
)

var (
	scriptRefill = redisx.NewScript("refill_budget.lua", lua.MustRead("refill_budget.lua"))
	scriptAdmit  = redisx.NewScript("admit.lua", lua.MustRead("admit.lua"))
	scriptRedeem = redisx.NewScript("redeem.lua", lua.MustRead("redeem.lua"))
)

// 이 패키지가 반환하는 오류.
var (
	ErrInvalidEvent    = errors.New("admission: invalid event id")
	ErrEmptyToken      = errors.New("admission: token id is required")
	ErrUnexpectedReply = errors.New("admission: unexpected lua reply")
)

// Status 는 입장 시도의 결과다.
type Status string

// 입장 시도 결과. not_yet 과 observing 만이 "다시 시도하면 된다"를 뜻한다.
const (
	StatusAdmitted Status = "admitted"
	StatusNotYet   Status = "not_yet"
	// StatusObserving 은 순번은 왔지만 아직 판정할 만큼 관찰하지 못한 상태다.
	// 거절이 아니라 유예이고, 예산도 소모하지 않는다(§3.4 최소 관찰 게이트).
	StatusObserving Status = "observing"
	// StatusGreylist 는 재검증 대기다. 조치이긴 하지만 나가는 길이 있다(§4).
	// 호출자는 이것을 거절이 아니라 "재챌린지를 받아 오라"로 옮긴다.
	StatusGreylist Status = "greylist"
	StatusUnknown  Status = "unknown"
	StatusEvicting Status = "evicting"
	StatusEvicted  Status = "evicted"
	StatusHeld     Status = "held"
	StatusBlocked  Status = "blocked"
)

// BurnStatus 는 입장 토큰 소각 결과다.
type BurnStatus string

// 소각 결과.
const (
	BurnOK       BurnStatus = "burned"
	BurnMissing  BurnStatus = "missing"
	BurnMismatch BurnStatus = "mismatch"
)

// Config 는 입장 제어에 필요한 설정만 추린 것이다.
type Config struct {
	EventID    string
	RatePerMin int
	Interval   time.Duration

	// AfterLottery 가 켜지면 LotteryEnd 전에는 예산을 한 명분도 내려보내지 않는다.
	// OpenAt 이 비어 있으면 추첨 구간이 없다는 뜻이라 게이트도 없다(§3.2, §3.4).
	AfterLottery  bool
	OpenAt        time.Time
	LotteryWindow time.Duration

	// MinDwell/MinBeats 는 사용자 단위 최소 관찰 게이트다(§3.4).
	// AfterLottery 가 이벤트 전체에 한 번 걸리는 시각 게이트라면, 이쪽은 각자의
	// 진입 시각부터 재는 개인별 게이트다. 늦게 온 사람도 똑같이 관찰된다.
	// 둘 다 0 이면 게이트가 없다(기본값).
	MinDwell time.Duration
	MinBeats int64

	MaxBudgetPerShard int
	BackpressureMin   float64
	BreakerFailures   int
	BreakerCooldown   time.Duration
	EntryTTL          time.Duration
	UserTTL           time.Duration
}

// FromConfig 는 서비스 설정에서 입장 제어 설정을 뽑는다.
func FromConfig(c *config.Config) Config {
	return Config{
		EventID:           c.Event.ID,
		RatePerMin:        c.Admission.RatePerMin,
		Interval:          c.Admission.Interval,
		AfterLottery:      c.Admission.AfterLottery,
		OpenAt:            c.Event.OpenAt,
		LotteryWindow:     c.Event.LotteryWindow,
		MinDwell:          c.Admission.MinDwell,
		MinBeats:          c.Admission.MinBeats,
		MaxBudgetPerShard: c.Admission.MaxBudgetPerShard,
		BackpressureMin:   c.Admission.BackpressureMin,
		BreakerFailures:   c.Admission.BreakerFailures,
		BreakerCooldown:   c.Admission.BreakerCooldown,
		EntryTTL:          c.Token.EntryTTL,
		UserTTL:           c.Token.QueueTTL,
	}
}

// budgetTTLFactor 는 예산 키의 TTL 을 배분 주기의 몇 배로 둘지 정한다.
// 컨트롤러가 죽으면 예산은 이 시간 뒤에 사라지고, 그때부터 아무도 입장하지 못한다.
// 배분이 멈춘 채 남은 예산이 계속 소비되는 것보다 멈추는 쪽이 안전하다.
const budgetTTLFactor = 4

// Store 는 예산·입장·소각을 Redis 에 반영한다.
type Store struct {
	rdb redis.UniversalClient
	cfg Config
	log *slog.Logger
	met *obs.Metrics
	now func() time.Time
}

// NewStore 는 입장 제어 저장소를 만든다.
func NewStore(rdb redis.UniversalClient, cfg Config, log *slog.Logger, met *obs.Metrics) (*Store, error) {
	if cfg.EventID == "" {
		return nil, ErrInvalidEvent
	}
	if log == nil {
		log = slog.Default()
	}
	return &Store{rdb: rdb, cfg: cfg, log: log, met: met, now: time.Now}, nil
}

// WithClock 은 시계를 갈아 끼운다(테스트용).
func (s *Store) WithClock(fn func() time.Time) *Store {
	s.now = fn
	return s
}

// Budget 은 한 샤드의 예산 현황이다.
type Budget struct {
	Shard   string
	Budget  int64
	Waiting int64
}

// Refill 은 샤드에 이번 주기의 몫을 채운다.
func (s *Store) Refill(ctx context.Context, shardID string, grant int64) (Budget, error) {
	if err := shard.Validate(shardID); err != nil {
		return Budget{}, err
	}
	ev := s.cfg.EventID

	res, err := scriptRefill.Run(ctx, s.rdb,
		[]string{keys.Budget(ev, shardID), keys.Queue(ev, shardID)},
		grant,
		s.cfg.MaxBudgetPerShard,
		(s.cfg.Interval * budgetTTLFactor).Milliseconds(),
	)
	if err != nil {
		return Budget{}, err
	}
	vals, err := reply(res, 2)
	if err != nil {
		return Budget{}, err
	}
	b := Budget{Shard: shardID, Budget: asInt64(vals[0]), Waiting: asInt64(vals[1])}
	if s.met != nil {
		s.met.AdmitBudget.WithLabelValues(ev, shardID).Set(float64(b.Budget))
	}
	return b, nil
}

// Result 는 입장 시도의 결과다.
type Result struct {
	Status Status
	Rank   int64
	Budget int64
	// JTI 는 발행된(또는 이미 발행돼 있던) 입장 토큰 ID 다.
	JTI string
	// Waited 는 진입부터 입장까지 걸린 시간이다(§11 P99 진입 지연).
	Waited time.Duration
}

// Admit 은 순번이 예산 안에 들면 입장시키고 1회용 입장 토큰을 발행한다.
//
// 호출자는 이 함수를 부르기 전에 반드시 큐 토큰을 검증해야 한다(불변식 2).
// jti 는 서버가 만든 값이어야 한다 — 클라이언트가 고른 jti 를 받으면 입장 토큰의
// 1회성이 클라이언트 손에 넘어간다.
func (s *Store) Admit(ctx context.Context, shardID, tokenID, jti string) (Result, error) {
	if tokenID == "" || jti == "" {
		return Result{}, ErrEmptyToken
	}
	if err := shard.Validate(shardID); err != nil {
		return Result{}, err
	}
	ev := s.cfg.EventID

	res, err := scriptAdmit.Run(ctx, s.rdb,
		[]string{
			keys.Queue(ev, shardID),
			keys.Budget(ev, shardID),
			keys.User(ev, shardID, tokenID),
		},
		tokenID,
		jti,
		s.now().UnixMilli(),
		keys.EntryPrefix(ev, shardID),
		s.cfg.EntryTTL.Milliseconds(),
		s.cfg.UserTTL.Milliseconds(),
		s.cfg.MinDwell.Milliseconds(),
		s.cfg.MinBeats,
	)
	if err != nil {
		return Result{}, err
	}
	vals, err := reply(res, 5)
	if err != nil {
		return Result{}, err
	}

	r := Result{
		Status: Status(asString(vals[0])),
		Rank:   asInt64(vals[1]),
		Budget: asInt64(vals[2]),
		JTI:    asString(vals[3]),
	}
	if ms := asInt64(vals[4]); ms >= 0 {
		r.Waited = time.Duration(ms) * time.Millisecond
	}

	if s.met != nil {
		s.met.AdmitAttempts.WithLabelValues(ev, string(r.Status)).Inc()
		s.met.AdmitBudget.WithLabelValues(ev, shardID).Set(float64(r.Budget))
		if r.Status == StatusAdmitted && r.Waited > 0 {
			s.met.WaitSeconds.WithLabelValues(ev).Observe(r.Waited.Seconds())
		}
	}
	return r, nil
}

// Burn 은 입장 토큰을 소각한다. burned 를 받은 호출자만 구매를 진행할 수 있다.
func (s *Store) Burn(ctx context.Context, shardID, jti, tokenID string) (BurnStatus, error) {
	if tokenID == "" || jti == "" {
		return "", ErrEmptyToken
	}
	if err := shard.Validate(shardID); err != nil {
		return "", err
	}
	ev := s.cfg.EventID

	res, err := scriptRedeem.Run(ctx, s.rdb,
		[]string{
			keys.Entry(ev, shardID, jti),
			keys.User(ev, shardID, tokenID),
		},
		tokenID,
		s.now().UnixMilli(),
	)
	if err != nil {
		return "", err
	}
	vals, err := reply(res, 2)
	if err != nil {
		return "", err
	}
	st := BurnStatus(asString(vals[0]))
	if s.met != nil {
		s.met.Redeemed.WithLabelValues(ev, string(st)).Inc()
	}
	return st, nil
}

func reply(v any, want int) ([]any, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", ErrUnexpectedReply, v)
	}
	if len(arr) != want {
		return nil, fmt.Errorf("%w: len %d, want %d", ErrUnexpectedReply, len(arr), want)
	}
	return arr, nil
}

func asInt64(v any) int64 {
	if n, ok := v.(int64); ok {
		return n
	}
	return -1
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
