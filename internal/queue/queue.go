// Package queue 는 대기열 도메인 API 다.
//
// 상태를 바꾸는 모든 경로는 scripts/lua 의 스크립트 한 번을 호출하는 것으로 끝난다.
// Redis 명령을 Go 에서 조합해 상태를 옮기지 않는다 — 여러 왕복으로 쪼개는 순간
// 그 사이에 다른 요청이 끼어들어 같은 순번이 두 번 나가거나, 예산이 음수가 되는
// 종류의 결함이 생긴다(CLAUDE.md 불변식 1).
//
// 이 패키지가 직접 쓰는 Redis 명령은 샤드 목록 인덱스(`shards:{event}`) 조작뿐이고,
// 그건 큐 상태가 아니라 발견용 인덱스다. 아래 registerShard 주석 참고.
package queue

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/redisx"
	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"
)

// LotteryBand 는 추첨 구간과 FIFO 구간을 가르는 ZSET score 경계다(DESIGN.md §3.2).
//
//	추첨 구간 진입자 → [0, LotteryBand)   난수 순번
//	FIFO 구간 진입자 → [LotteryBand, ...) LotteryBand + INCR(seq)
//
// 밴드를 Go 에 두고 Lua 에 인자로 넘긴다. 양쪽에 상수를 두면 언젠가 한쪽만 바뀌고,
// 그 순간 두 구간의 순서가 뒤섞인다.
const LotteryBand int64 = 1_000_000_000

// maxIDLen 은 토큰/이벤트 식별자의 길이 상한이다. 이 값들은 Redis 키에 들어간다.
const maxIDLen = 128

// 이 패키지가 반환하는 오류.
var (
	ErrEmptyToken      = errors.New("queue: token id is required")
	ErrInvalidToken    = errors.New("queue: invalid token id")
	ErrInvalidEvent    = errors.New("queue: invalid event id")
	ErrUnexpectedReply = errors.New("queue: unexpected lua reply")
)

// 로드된 스크립트. 이름은 scripts/lua 의 파일명과 1:1 로 대응한다.
var (
	scriptEnqueue   = redisx.NewScript("enqueue.lua", lua.MustRead("enqueue.lua"))
	scriptPosition  = redisx.NewScript("position.lua", lua.MustRead("position.lua"))
	scriptHeartbeat = redisx.NewScript("heartbeat.lua", lua.MustRead("heartbeat.lua"))
	scriptEvict     = redisx.NewScript("evict.lua", lua.MustRead("evict.lua"))
	scriptRechal    = redisx.NewScript("rechallenge.lua", lua.MustRead("rechallenge.lua"))
)

// Status 는 user 해시의 state 필드 값이다.
type Status string

// 대기열 사용자 상태. created/exists/unknown 은 호출 결과를 알리는 값이고,
// 나머지는 실제로 저장되는 상태다.
const (
	StatusCreated  Status = "created"
	StatusExists   Status = "exists"
	StatusWaiting  Status = "waiting"
	StatusGreylist Status = "greylist"
	StatusEvicting Status = "evicting"
	StatusEvicted  Status = "evicted"
	StatusHeld     Status = "held"
	StatusBlocked  Status = "blocked"
	StatusAdmitted Status = "admitted"
	StatusUnknown  Status = "unknown"
)

// Segment 는 순번을 받은 구간이다(§3.2).
type Segment string

// 공정성 구간.
const (
	SegmentLottery Segment = "lottery"
	SegmentFIFO    Segment = "fifo"
)

// Config 는 대기열 동작에 필요한 설정만 추린 것이다.
type Config struct {
	EventID           string
	OpenAt            time.Time
	LotteryWindow     time.Duration
	UserTTL           time.Duration
	HeartbeatInterval time.Duration
	MissedHeartbeats  int
	EvictGrace        time.Duration
	EvictedRetain     time.Duration
	SweepBatch        int
}

// FromConfig 는 서비스 설정에서 대기열 설정을 뽑는다.
func FromConfig(c *config.Config) Config {
	return Config{
		EventID:           c.Event.ID,
		OpenAt:            c.Event.OpenAt,
		LotteryWindow:     c.Event.LotteryWindow,
		UserTTL:           c.Token.QueueTTL,
		HeartbeatInterval: c.Queue.HeartbeatInterval,
		MissedHeartbeats:  c.Queue.MissedHeartbeats,
		EvictGrace:        c.Queue.EvictGrace,
		EvictedRetain:     c.Queue.EvictedRetain,
		SweepBatch:        c.Queue.SweepBatch,
	}
}

// Store 는 대기열 상태 저장소다. 여러 고루틴에서 동시에 써도 안전하다.
type Store struct {
	rdb redis.UniversalClient
	cfg Config
	log *slog.Logger
	met *obs.Metrics

	// registered 는 shards:{event} 에 등록한 샤드의 마지막 등록 시각이다(shardID → time.Time).
	// SADD 왕복을 줄이려고 두지만, **영구 기억이 아니다** — reregisterAfter 참고.
	registered sync.Map

	now func() time.Time
}

// New 는 대기열 저장소를 만든다.
func New(rdb redis.UniversalClient, cfg Config, log *slog.Logger, met *obs.Metrics) (*Store, error) {
	if !validID(cfg.EventID) {
		return nil, ErrInvalidEvent
	}
	if cfg.SweepBatch <= 0 {
		return nil, errors.New("queue: sweep batch must be > 0")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Store{rdb: rdb, cfg: cfg, log: log, met: met, now: time.Now}, nil
}

// WithClock 은 시계를 갈아 끼운다. 추첨/FIFO 경계와 soft-evict 시한처럼 시각에
// 의존하는 동작을 테스트에서 결정적으로 재현하기 위한 훅이다.
func (s *Store) WithClock(fn func() time.Time) *Store {
	s.now = fn
	return s
}

// LotteryEnd 는 추첨 구간이 닫히는 시각이다.
// OpenAt 이 설정되지 않았으면 추첨 구간 없이 전원 FIFO 로 동작한다.
func (s *Store) LotteryEnd() time.Time {
	if s.cfg.OpenAt.IsZero() {
		return time.Time{}
	}
	return s.cfg.OpenAt.Add(s.cfg.LotteryWindow)
}

// EnqueueRequest 는 진입 요청이다.
// 지문과 IP 는 해시·프리픽스 형태로만 받는다 — 원본은 이 경계를 넘지 않는다(불변식 6).
type EnqueueRequest struct {
	Shard    string
	TokenID  string
	FPHash   string
	IPPrefix string
}

// Ticket 은 진입 결과다.
type Ticket struct {
	Status  Status
	Shard   string
	Rank    int64 // 0-based 큐 내 위치. 큐에 없으면 -1
	Size    int64
	Score   int64 // ZSET score = 부여된 순번 값
	Segment Segment
}

// Queued 는 이 호출로 대기열에 자리를 가지게 됐는지 알려준다.
func (t Ticket) Queued() bool { return t.Status == StatusCreated || t.Status == StatusExists }

// Enqueue 는 사용자를 샤드 대기열에 넣는다.
//
// 같은 token_id 로 다시 불러도 순번은 바뀌지 않는다(불변식 4). 재시도나 중복 요청이
// 순번을 앞당길 수 있으면, 그걸 가장 잘 이용하는 쪽은 봇이다.
func (s *Store) Enqueue(ctx context.Context, req EnqueueRequest) (Ticket, error) {
	if err := s.check(req.Shard, req.TokenID); err != nil {
		return Ticket{}, err
	}

	// 샤드 목록에 먼저 등록한다. 이 순서라면 최악의 경우 "비어 있는 샤드가 목록에
	// 남는" 정도로 끝나고(예산 0 이 배분될 뿐), "사용자는 줄 서 있는데 아무도 그
	// 샤드를 모르는" 상황은 생기지 않는다.
	if err := s.registerShard(ctx, req.Shard); err != nil {
		return Ticket{}, err
	}

	lot, err := lotteryRank()
	if err != nil {
		return Ticket{}, err
	}

	now := s.now()
	ev := s.cfg.EventID
	res, err := scriptEnqueue.Run(ctx, s.rdb,
		[]string{
			keys.Queue(ev, req.Shard),
			keys.Seq(ev, req.Shard),
			keys.User(ev, req.Shard, req.TokenID),
		},
		req.TokenID,
		req.Shard,
		now.UnixMilli(),
		milliOrZero(s.LotteryEnd()),
		lot,
		LotteryBand,
		req.FPHash,
		req.IPPrefix,
		s.cfg.UserTTL.Milliseconds(),
	)
	if err != nil {
		return Ticket{}, err
	}

	vals, err := reply(res, 5)
	if err != nil {
		return Ticket{}, err
	}
	t := Ticket{
		Status:  Status(asString(vals[0])),
		Shard:   req.Shard,
		Rank:    asInt64(vals[1]),
		Size:    asInt64(vals[2]),
		Score:   asInt64(vals[3]),
		Segment: Segment(asString(vals[4])),
	}

	if s.met != nil {
		if t.Status == StatusCreated {
			s.met.Enqueued.WithLabelValues(ev, string(t.Segment)).Inc()
		}
		s.met.QueueSize.WithLabelValues(ev, req.Shard).Set(float64(t.Size))
	}
	return t, nil
}

// Snapshot 은 한 번의 원자 읽기로 얻은 대기 상태다.
type Snapshot struct {
	Status   Status
	Shard    string
	Rank     int64 // 0-based. 큐에 없으면 -1
	Size     int64
	HoldRank int64
	HoldSize int64
	OrigRank int64
	Segment  Segment
	JoinedAt time.Time
	LastSeen time.Time
	BotScore int64
	// Rechallenges 는 지금까지 통과한 재챌린지 횟수다(§4).
	Rechallenges int64
	// ObserveFrom 은 최소 관찰 게이트의 기산점이다(§3.4). 보통 JoinedAt 과 같지만
	// 재챌린지 복귀 시 그 시점으로 되감기므로 그 이후로 갈라진다. 제로값이면
	// 되감긴 적이 없다는 뜻이고, 그때의 기산점은 JoinedAt 이다.
	ObserveFrom time.Time
}

// ObservedFrom 은 관찰 시계의 기산점이다. 되감긴 적이 없으면 진입 시각이다.
func (s Snapshot) ObservedFrom() time.Time {
	if s.ObserveFrom.IsZero() {
		return s.JoinedAt
	}
	return s.ObserveFrom
}

// Ahead 는 내 앞에 남은 인원이다.
func (s Snapshot) Ahead() int64 {
	if s.Rank < 0 {
		return 0
	}
	return s.Rank
}

// EstimateWait 는 샤드에 배분된 실효 입장률로 남은 대기 시간을 환산한다.
//
// 스냅샷(rank/size)은 Redis 에서 원자적으로 읽은 서버 값이고, 환산에 쓰는 입장률도
// admission 이 정한 서버 값이다. 클라이언트가 보낸 값은 어디에도 들어가지 않는다.
func (s Snapshot) EstimateWait(shardAdmitPerMin float64) time.Duration {
	if s.Rank < 0 || shardAdmitPerMin <= 0 {
		return 0
	}
	minutes := float64(s.Rank+1) / shardAdmitPerMin
	return time.Duration(minutes * float64(time.Minute))
}

// Gates 는 입장을 여는 시점에 걸린 게이트다(§3.4). 표시용 예상 대기 계산에만 쓴다 —
// 실제 강제는 admission 의 배분 루프와 admit.lua 가 한다.
type Gates struct {
	// AdmitOpensAt 은 이벤트 전체에 걸린 게이트가 열리는 시각이다(ADMIT_AFTER_LOTTERY).
	// 제로값이면 게이트가 없다.
	AdmitOpensAt time.Time
	// MinDwell 은 사용자 개인에게 걸린 최소 관찰 시간이다(ADMIT_MIN_DWELL).
	MinDwell time.Duration
}

// EstimateWaitAt 은 게이트를 반영한 예상 대기 시간이다.
//
// 게이트를 모르는 EstimateWait 는 게이트가 켜진 순간부터 사용자에게 거짓말을 한다.
// "3초 남음"이 3분째 3초로 멈춰 있으면 사용자는 고장으로 읽는다. 대기 화면이 유일한
// 진행 표시인 곳에서 그 오해는 새로고침 폭주로 돌아온다.
//
// 두 게이트는 성질이 달라 합치는 방식도 다르다:
//   - 전체 게이트: 열리기 전에는 아무 자리도 나가지 않으므로 줄 자체가 그때 시작한다.
//     그래서 남은 시간을 순번 대기 **앞에 더한다.**
//   - 관찰 게이트: 그동안에도 다른 사람은 나가고 내 순번도 당겨진다. 내 입장은 둘 중
//     늦게 끝나는 쪽에 걸리므로 **max** 를 쓴다.
func (s Snapshot) EstimateWaitAt(now time.Time, shardAdmitPerMin float64, g Gates) time.Duration {
	wait := s.EstimateWait(shardAdmitPerMin)
	if wait == 0 {
		// 큐에 없거나 배분이 멈춘 상태다. EstimateWait 의 0 은 "모른다"는 뜻이고,
		// 게이트를 더해 그 자리에 그럴듯한 숫자를 채워 넣으면 없는 정보가 생긴다.
		return 0
	}
	if !g.AdmitOpensAt.IsZero() {
		if left := g.AdmitOpensAt.Sub(now); left > 0 {
			wait += left
		}
	}
	// 기산점은 JoinedAt 이 아니라 관찰 시계다 — 재챌린지로 복귀한 사용자는 그 시점부터
	// 다시 관찰되므로(rechallenge.lua), 진입 시각으로 재면 그 사람에게만 남은 시간을
	// 짧게 말하게 된다. 화면이 거짓말을 하는 쪽은 언제나 이미 한 번 걸린 사용자다.
	if from := s.ObservedFrom(); g.MinDwell > 0 && !from.IsZero() {
		if left := g.MinDwell - now.Sub(from); left > wait {
			wait = left
		}
	}
	return wait
}

// Position 은 순번·대기 인원을 원자적으로 읽는다.
func (s *Store) Position(ctx context.Context, shardID, tokenID string) (Snapshot, error) {
	if err := s.check(shardID, tokenID); err != nil {
		return Snapshot{}, err
	}

	ev := s.cfg.EventID
	res, err := scriptPosition.Run(ctx, s.rdb,
		[]string{
			keys.Queue(ev, shardID),
			keys.Hold(ev, shardID),
			keys.User(ev, shardID, tokenID),
		},
		tokenID,
	)
	if err != nil {
		return Snapshot{}, err
	}

	vals, err := reply(res, 12)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{
		Status:   Status(asString(vals[0])),
		Shard:    shardID,
		Rank:     asInt64(vals[1]),
		Size:     asInt64(vals[2]),
		HoldRank: asInt64(vals[3]),
		HoldSize: asInt64(vals[4]),
		OrigRank: asInt64(vals[5]),
		Segment:  Segment(asString(vals[6])),
		JoinedAt: fromMilli(asInt64(vals[7])),
		LastSeen: fromMilli(asInt64(vals[8])),
		BotScore: asInt64(vals[9]),

		Rechallenges: asInt64(vals[10]),
		ObserveFrom:  fromMilli(asInt64(vals[11])),
	}
	if s.met != nil {
		s.met.QueueSize.WithLabelValues(ev, shardID).Set(float64(snap.Size))
	}
	return snap, nil
}

// Beat 는 heartbeat 처리 결과다.
type Beat struct {
	Status Status
	// Interval 은 직전 신호와의 간격이다. 첫 신호면 0.
	// 이 값의 규칙성이 §4-L4 의 탐지 신호가 된다 — 여기서는 관측만 하고 판단하지 않는다.
	Interval time.Duration
	Count    int64
	Rank     int64
	Revived  bool
}

// Heartbeat 는 생존 신호를 기록하고 관측치를 돌려준다.
func (s *Store) Heartbeat(ctx context.Context, shardID, tokenID string) (Beat, error) {
	if err := s.check(shardID, tokenID); err != nil {
		return Beat{}, err
	}

	ev := s.cfg.EventID
	res, err := scriptHeartbeat.Run(ctx, s.rdb,
		[]string{
			keys.Queue(ev, shardID),
			keys.User(ev, shardID, tokenID),
		},
		tokenID,
		s.now().UnixMilli(),
		s.cfg.UserTTL.Milliseconds(),
	)
	if err != nil {
		return Beat{}, err
	}

	vals, err := reply(res, 5)
	if err != nil {
		return Beat{}, err
	}
	b := Beat{
		Status:  Status(asString(vals[0])),
		Count:   asInt64(vals[2]),
		Rank:    asInt64(vals[3]),
		Revived: asInt64(vals[4]) == 1,
	}
	if d := asInt64(vals[1]); d >= 0 {
		b.Interval = time.Duration(d) * time.Millisecond
	}
	return b, nil
}

// SweepResult 는 soft-evict 스윕 1회의 결과다.
type SweepResult struct {
	Scanned int64
	Marked  int64 // waiting → evicting (유예 시작)
	Removed int64 // evicting → evicted (큐에서 제거)
	Ghosts  int64 // 상태 해시가 사라진 유령 항목 제거
	// NextOffset 은 다음 스윕이 이어갈 커서다. 0 이면 샤드를 한 바퀴 돌았다는 뜻이다.
	NextOffset int64
	Size       int64
}

// RechallengeOutcome 은 재챌린지 통과 처리의 결과다.
type RechallengeOutcome string

// 재챌린지 결과.
const (
	// RechallengeRestored 는 원 샤드의 원 순번으로 복귀했다는 뜻이다.
	RechallengeRestored RechallengeOutcome = "restored"
	// RechallengeExhausted 는 허용 횟수를 넘겨 보류로 승급됐다는 뜻이다.
	RechallengeExhausted RechallengeOutcome = "exhausted"
	// RechallengeNoop 은 greylist 가 아니어서 풀어 줄 것이 없었다는 뜻이다.
	RechallengeNoop RechallengeOutcome = "noop"
	// RechallengeNoRank 는 돌아갈 순번을 잃어버린 경우다.
	RechallengeNoRank RechallengeOutcome = "no_rank"
	// RechallengeUnknown 은 사용자 상태가 이미 사라진 경우다.
	RechallengeUnknown RechallengeOutcome = "unknown"
)

// RechallengeRequest 는 재챌린지 통과 처리 요청이다.
//
// 정책값(허용 횟수·통과 점수·승급 점수)을 호출자가 넘긴다. 이 패키지는 큐 상태
// 전이만 알고 점수 정책은 모른다 — 임계값의 단일 출처는 config 이고, 여기에
// 사본을 두면 언젠가 한쪽만 바뀐다.
type RechallengeRequest struct {
	Shard   string
	TokenID string

	MaxAttempts int
	PassScore   int
	HoldScore   int
}

// RechallengeResult 는 처리 결과다.
type RechallengeResult struct {
	Outcome  RechallengeOutcome
	Status   Status
	Rank     int64
	Attempts int64
	Score    int64
}

// Rechallenge 는 재챌린지를 통과한 greylist 사용자를 원 순번으로 되돌린다(§4).
//
// **호출 전에 PoW 검증이 끝나 있어야 한다.** 이 메서드는 "통과했다"는 사실을 상태로
// 옮기기만 하고 통과 여부를 판단하지 않는다 — 검증은 challenge 패키지가, 조치는
// 여기가 한다. 허용 횟수를 넘겨 오면 복귀 대신 보류로 올린다.
func (s *Store) Rechallenge(ctx context.Context, req RechallengeRequest) (RechallengeResult, error) {
	if err := s.check(req.Shard, req.TokenID); err != nil {
		return RechallengeResult{}, err
	}
	origin, err := shard.Origin(req.Shard)
	if err != nil {
		return RechallengeResult{}, err
	}
	grey, err := shard.Greylist(req.Shard)
	if err != nil {
		return RechallengeResult{}, err
	}

	ev := s.cfg.EventID
	res, err := scriptRechal.Run(ctx, s.rdb,
		[]string{
			keys.Queue(ev, origin),
			keys.Queue(ev, grey),
			keys.Hold(ev, origin),
			keys.Score(ev, origin),
			keys.User(ev, origin, req.TokenID),
		},
		req.TokenID, origin, req.MaxAttempts, req.PassScore, req.HoldScore,
		s.now().UnixMilli(), s.cfg.UserTTL.Milliseconds(),
	)
	if err != nil {
		return RechallengeResult{}, err
	}

	vals, err := reply(res, 5)
	if err != nil {
		return RechallengeResult{}, err
	}
	return RechallengeResult{
		Outcome:  RechallengeOutcome(asString(vals[0])),
		Status:   Status(asString(vals[1])),
		Rank:     asInt64(vals[2]),
		Attempts: asInt64(vals[3]),
		Score:    asInt64(vals[4]),
	}, nil
}

// StaleAfter 는 이 시간 동안 신호가 없으면 유예를 시작하는 기준이다(§5: 미수신 3회).
func (s *Store) StaleAfter() time.Duration {
	return s.cfg.HeartbeatInterval * time.Duration(s.cfg.MissedHeartbeats)
}

// Sweep 은 샤드의 한 구간을 훑어 끊긴 사용자를 정리한다.
// 반환된 NextOffset 을 그대로 다음 호출에 넘기면 샤드를 한 바퀴 돈다.
func (s *Store) Sweep(ctx context.Context, shardID string, offset int64) (SweepResult, error) {
	if err := shard.Validate(shardID); err != nil {
		return SweepResult{}, err
	}

	ev := s.cfg.EventID
	res, err := scriptEvict.Run(ctx, s.rdb,
		[]string{keys.Queue(ev, shardID)},
		keys.UserPrefix(ev, shardID),
		s.now().UnixMilli(),
		s.StaleAfter().Milliseconds(),
		s.cfg.EvictGrace.Milliseconds(),
		offset,
		s.cfg.SweepBatch,
		s.cfg.EvictedRetain.Milliseconds(),
	)
	if err != nil {
		return SweepResult{}, err
	}

	vals, err := reply(res, 6)
	if err != nil {
		return SweepResult{}, err
	}
	r := SweepResult{
		Scanned:    asInt64(vals[0]),
		Marked:     asInt64(vals[1]),
		Removed:    asInt64(vals[2]),
		Ghosts:     asInt64(vals[3]),
		NextOffset: asInt64(vals[4]),
		Size:       asInt64(vals[5]),
	}
	if s.met != nil {
		if r.Removed > 0 {
			s.met.Evicted.WithLabelValues(ev, "heartbeat_timeout").Add(float64(r.Removed))
		}
		if r.Ghosts > 0 {
			s.met.Evicted.WithLabelValues(ev, "ghost").Add(float64(r.Ghosts))
		}
		s.met.QueueSize.WithLabelValues(ev, shardID).Set(float64(r.Size))
	}
	return r, nil
}

// Shards 는 이 이벤트에서 실제로 사용 중인 샤드 목록이다.
//
// 이건 큐 상태가 아니라 발견용 인덱스다. `shards:{event}` 는 이벤트 태그를 쓰므로
// 샤드 키들과 다른 슬롯에 있고, 따라서 샤드 Lua 안에서 함께 다룰 수 없다(§3.3).
// 대신 SADD/SMEMBERS 라는 멱등한 단일 키 연산만 쓴다 — 순번이나 입장 여부처럼
// 원자성이 필요한 값은 여기에 담기지 않는다.
func (s *Store) Shards(ctx context.Context) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, keys.Shards(s.cfg.EventID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list shards: %w", err)
	}
	out := members[:0:0]
	for _, m := range members {
		if shard.Validate(m) == nil {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Sizes 는 샤드별 대기 인원을 읽는다.
//
// 상태를 바꾸지 않는 순수 읽기이므로 Lua 를 거치지 않는다. 애초에 샤드들은 서로
// 다른 슬롯에 있어 한 번에 원자적으로 볼 수도 없다 — 이 값은 "지금 이 순간의
// 정확한 총합"이 아니라 배분 비율과 샤드 확장 판단에 쓰는 근사치다.
// 순번이나 예산처럼 정확성이 필요한 값은 전부 Lua 안에서 다룬다.
func (s *Store) Sizes(ctx context.Context, shards []string) ([]int64, error) {
	out := make([]int64, len(shards))
	if len(shards) == 0 {
		return out, nil
	}

	cmds := make([]*redis.IntCmd, len(shards))
	pipe := s.rdb.Pipeline()
	for i, sh := range shards {
		cmds[i] = pipe.ZCard(ctx, keys.Queue(s.cfg.EventID, sh))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read shard sizes: %w", err)
	}
	for i, cmd := range cmds {
		n, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read size of %s: %w", shards[i], err)
		}
		out[i] = n
	}
	return out, nil
}

// TotalWaiting 은 이벤트 전체의 대기 인원이다. 동적 샤드 확장 판단에 쓴다(§3.1).
func (s *Store) TotalWaiting(ctx context.Context) (int64, error) {
	shards, err := s.Shards(ctx)
	if err != nil {
		return 0, err
	}
	sizes, err := s.Sizes(ctx, shards)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, n := range sizes {
		total += n
	}
	return total, nil
}

// reregisterAfter 는 샤드 등록을 다시 밀어 넣는 주기다.
//
// 메모를 영구히 믿으면, `shards:{event}` 가 사라졌을 때(장애 복구, 운영자의 flush,
// 침해 사고) 이 프로세스는 "이미 등록했다"고 기억한 채 다시 넣지 않는다. 그러면
// Admission 컨트롤러가 배분할 샤드를 못 찾아 **아무도 입장하지 못하는 상태가
// 영구히 지속된다.** 대기열은 멀쩡해 보이는데 줄이 줄지 않는, 가장 알아채기 어려운
// 종류의 고장이다. SADD 는 멱등하고 초당 한 번은 공짜나 다름없으니 주기적으로 되민다.
const reregisterAfter = time.Minute

func (s *Store) registerShard(ctx context.Context, shardID string) error {
	if at, ok := s.registered.Load(shardID); ok {
		if t, valid := at.(time.Time); valid && s.now().Sub(t) < reregisterAfter {
			return nil
		}
	}
	if err := s.rdb.SAdd(ctx, keys.Shards(s.cfg.EventID), shardID).Err(); err != nil {
		return fmt.Errorf("register shard %s: %w", shardID, err)
	}
	s.registered.Store(shardID, s.now())
	return nil
}

func (s *Store) check(shardID, tokenID string) error {
	if tokenID == "" {
		return ErrEmptyToken
	}
	if !validID(tokenID) {
		return ErrInvalidToken
	}
	if err := shard.Validate(shardID); err != nil {
		return err
	}
	return nil
}

// lotteryRank 는 추첨 구간의 순번을 만든다.
//
// CSPRNG 로 뽑는다. 순번을 예측할 수 있으면 봇이 "좋은 번호를 받을 때까지 재진입"하는
// 전략을 쓸 수 있고, 추첨 구간이 봇의 선점 이점을 없앤다는 §3.2 의 목적이 무너진다.
func lotteryRank() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("queue: lottery rank: %w", err)
	}
	// LotteryBand(1e9) 는 2^64 에 비해 극히 작아 모듈로 편향은 무시 가능하다.
	return int64(binary.BigEndian.Uint64(b[:]) % uint64(LotteryBand)), nil
}

// validID 는 Redis 키에 들어갈 식별자를 제한한다.
// base64url 문자 집합(JWT jti 가 쓰는 것)만 허용해 `{`, `}`, `:` 같은
// 키 스키마를 흔들 수 있는 문자를 원천 차단한다.
func validID(s string) bool {
	if s == "" || len(s) > maxIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

func milliOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromMilli(ms int64) time.Time {
	if ms < 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// reply 는 Lua 배열 응답을 길이 검증과 함께 꺼낸다.
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
	switch t := v.(type) {
	case int64:
		return t
	case string:
		var n int64
		if _, err := fmt.Sscan(t, &n); err == nil {
			return n
		}
	}
	return -1
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
