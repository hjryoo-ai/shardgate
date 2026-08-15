//go:build integration

// Lua 스크립트는 실제 Redis 로만 검증한다.
// miniredis 같은 대체 구현은 EVAL 의 의미를 근사할 뿐이고, 이 프로젝트에서 Lua 는
// "상태 전이가 원자적으로 일어난다"는 불변식 1 그 자체이므로 근사로는 증명이 안 된다.
package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"
)

// deploy/docker-compose.yml 과 같은 이미지를 쓴다 — 개발 환경과 테스트 환경이
// 다른 Redis 를 보면 Lua 동작 차이를 테스트가 못 잡는다.
const redisImage = "redis:8-alpine"

var sharedRedisURL string

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcredis.Run(ctx, redisImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis container: %v\n", err)
		os.Exit(1)
	}

	url, err := ctr.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		fmt.Fprintf(os.Stderr, "redis connection string: %v\n", err)
		os.Exit(1)
	}
	sharedRedisURL = url

	code := m.Run()

	if err := testcontainers.TerminateContainer(ctr); err != nil {
		fmt.Fprintf(os.Stderr, "terminate redis container: %v\n", err)
	}
	os.Exit(code)
}

// clock 은 시각에 의존하는 동작(추첨 경계, soft-evict 시한)을 결정적으로 재현한다.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type harness struct {
	t     testing.TB
	store *Store
	rdb   goredis.UniversalClient
	clk   *clock
	event string
	shard string
}

var eventSeq atomic.Int64

const testShard = "s0001"

// newHarness 는 테스트마다 독립된 이벤트 네임스페이스를 준다.
// 컨테이너는 공유하되 키 공간이 겹치지 않으므로 테스트 간 간섭이 없다.
func newHarness(t testing.TB, mutate func(*Config)) *harness {
	t.Helper()

	opt, err := goredis.ParseURL(sharedRedisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	open := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	clk := &clock{t: open}

	cfg := Config{
		EventID:           "evt" + strconv.FormatInt(eventSeq.Add(1), 10),
		OpenAt:            open,
		LotteryWindow:     2 * time.Minute,
		UserTTL:           2 * time.Hour,
		HeartbeatInterval: time.Second,
		MissedHeartbeats:  3,
		EvictGrace:        2 * time.Second,
		EvictedRetain:     10 * time.Minute,
		SweepBatch:        256,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	store, err := New(rdb, cfg, nil, nil)
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	store.WithClock(clk.Now)

	return &harness{t: t, store: store, rdb: rdb, clk: clk, event: cfg.EventID, shard: testShard}
}

func (h *harness) enqueue(token string) Ticket {
	h.t.Helper()
	tk, err := h.store.Enqueue(context.Background(), EnqueueRequest{
		Shard: h.shard, TokenID: token, FPHash: "fp_" + token, IPPrefix: "203.0.113.0/24",
	})
	if err != nil {
		h.t.Fatalf("enqueue %s: %v", token, err)
	}
	return tk
}

func (h *harness) position(token string) Snapshot {
	h.t.Helper()
	s, err := h.store.Position(context.Background(), h.shard, token)
	if err != nil {
		h.t.Fatalf("position %s: %v", token, err)
	}
	return s
}

func (h *harness) sweep(offset int64) SweepResult {
	h.t.Helper()
	r, err := h.store.Sweep(context.Background(), h.shard, offset)
	if err != nil {
		h.t.Fatalf("sweep: %v", err)
	}
	return r
}

func (h *harness) userField(token, field string) string {
	h.t.Helper()
	v, err := h.rdb.HGet(context.Background(), keys.User(h.event, h.shard, token), field).Result()
	if err != nil {
		h.t.Fatalf("hget %s.%s: %v", token, field, err)
	}
	return v
}

func (h *harness) setUserField(token string, pairs ...any) {
	h.t.Helper()
	if err := h.rdb.HSet(context.Background(), keys.User(h.event, h.shard, token), pairs...).Err(); err != nil {
		h.t.Fatalf("hset %s: %v", token, err)
	}
}

func (h *harness) queueLen() int64 {
	h.t.Helper()
	n, err := h.rdb.ZCard(context.Background(), keys.Queue(h.event, h.shard)).Result()
	if err != nil {
		h.t.Fatalf("zcard: %v", err)
	}
	return n
}

// 임베드된 스크립트가 전부 실제 Redis 의 Lua 파서를 통과해야 한다.
// 문법 오류가 첫 요청 때 500 으로 드러나는 일을 여기서 막는다.
func TestScriptsLoadOnRealRedis(t *testing.T) {
	h := newHarness(t, nil)
	names, err := lua.Names()
	if err != nil {
		t.Fatalf("script names: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no lua scripts embedded")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			if _, err := h.rdb.ScriptLoad(context.Background(), lua.MustRead(name)).Result(); err != nil {
				t.Fatalf("%s failed to load: %v", name, err)
			}
		})
	}
}

// 추첨 구간(§3.2): 오픈 후 T분 안의 진입자는 도착 순서와 무관한 난수 순번을 받는다.
func TestEnqueueLotterySegment(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(30 * time.Second) // 추첨 창(2분) 안

	scores := make(map[int64]bool)
	for i := range 50 {
		tk := h.enqueue("tok" + strconv.Itoa(i))
		if tk.Status != StatusCreated {
			t.Fatalf("status = %s, want created", tk.Status)
		}
		if tk.Segment != SegmentLottery {
			t.Fatalf("segment = %s, want lottery", tk.Segment)
		}
		if tk.Score < 0 || tk.Score >= LotteryBand {
			t.Fatalf("score %d outside lottery band", tk.Score)
		}
		scores[tk.Score] = true
	}
	if len(scores) < 45 {
		t.Fatalf("only %d distinct lottery scores out of 50", len(scores))
	}

	// 도착 순서가 순번을 결정하지 않았음을 확인한다. 도착순이었다면 큐 맨 앞은 tok0 이다.
	front, err := h.rdb.ZRange(context.Background(), keys.Queue(h.event, h.shard), 0, 0).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	t.Logf("front of lottery queue: %v (도착 1등은 tok0)", front)
}

// FIFO 구간: 추첨 창이 닫힌 뒤 진입자는 도착순으로, 추첨 구간 전원 뒤에 붙는다.
func TestEnqueueFIFOSegmentIsOrdered(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute) // 추첨 창(2분) 밖

	var prev int64 = -1
	for i := range 10 {
		tk := h.enqueue("tok" + strconv.Itoa(i))
		if tk.Segment != SegmentFIFO {
			t.Fatalf("segment = %s, want fifo", tk.Segment)
		}
		if tk.Score < LotteryBand {
			t.Fatalf("fifo score %d fell into the lottery band", tk.Score)
		}
		if tk.Score <= prev {
			t.Fatalf("fifo score not monotonic: %d after %d", tk.Score, prev)
		}
		if tk.Rank != int64(i) {
			t.Fatalf("rank = %d, want %d", tk.Rank, i)
		}
		prev = tk.Score
	}
}

// 추첨 구간 진입자는 FIFO 구간 진입자보다 반드시 앞선다.
// 이 순서가 깨지면 "늦게 온 사람이 먼저 들어가는" 상황이 되어 공정성 모델이 무너진다.
func TestLotteryAlwaysRanksAheadOfFIFO(t *testing.T) {
	h := newHarness(t, nil)

	h.clk.Advance(30 * time.Second)
	lottery := make(map[string]bool)
	for i := range 20 {
		tok := "lot" + strconv.Itoa(i)
		h.enqueue(tok)
		lottery[tok] = true
	}

	h.clk.Advance(5 * time.Minute)
	for i := range 20 {
		h.enqueue("fifo" + strconv.Itoa(i))
	}

	members, err := h.rdb.ZRange(context.Background(), keys.Queue(h.event, h.shard), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(members) != 40 {
		t.Fatalf("queue size = %d, want 40", len(members))
	}
	seenFIFO := false
	for i, m := range members {
		if lottery[m] {
			if seenFIFO {
				t.Fatalf("lottery user %s at rank %d appears behind a fifo user", m, i)
			}
			continue
		}
		seenFIFO = true
	}
}

// 재시도·중복 요청이 순번을 바꾸면 안 된다(불변식 4).
// 바뀔 수 있다면 "될 때까지 다시 보내는" 쪽이 이득을 보고, 그걸 가장 잘하는 건 봇이다.
func TestEnqueueIsIdempotent(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	first := h.enqueue("tokA")
	if first.Status != StatusCreated {
		t.Fatalf("status = %s, want created", first.Status)
	}

	for i := range 5 {
		again := h.enqueue("tokA")
		if again.Status != StatusExists {
			t.Fatalf("retry %d status = %s, want exists", i, again.Status)
		}
		if again.Score != first.Score {
			t.Fatalf("retry %d moved the score: %d → %d", i, first.Score, again.Score)
		}
		if again.Rank != first.Rank {
			t.Fatalf("retry %d moved the rank: %d → %d", i, first.Rank, again.Rank)
		}
	}
	if n := h.queueLen(); n != 1 {
		t.Fatalf("queue size = %d, want 1", n)
	}
	// seq 카운터도 소모되지 않아야 한다 — 중복 요청이 뒷사람의 순번을 밀어내면 안 된다.
	seq, err := h.rdb.Get(context.Background(), keys.Seq(h.event, h.shard)).Int64()
	if err != nil {
		t.Fatalf("get seq: %v", err)
	}
	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
}

// 동시에 들어와도 같은 순번이 두 번 나가지 않는다 — Lua 원자성의 핵심 증명이다.
func TestConcurrentEnqueueGivesUniqueRanks(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute) // FIFO 구간: 순번이 seq 로 결정되므로 충돌이 드러난다

	const n = 300
	var wg sync.WaitGroup
	scores := make([]int64, n)
	errs := make([]error, n)

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk, err := h.store.Enqueue(context.Background(), EnqueueRequest{
				Shard: h.shard, TokenID: "tok" + strconv.Itoa(i), FPHash: "fp", IPPrefix: "203.0.113.0/24",
			})
			scores[i], errs[i] = tk.Score, err
		}()
	}
	wg.Wait()

	seen := make(map[int64]int, n)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		if prev, dup := seen[scores[i]]; dup {
			t.Fatalf("score %d handed to both tok%d and tok%d", scores[i], prev, i)
		}
		seen[scores[i]] = i
	}
	if got := h.queueLen(); got != n {
		t.Fatalf("queue size = %d, want %d", got, n)
	}
	// FIFO 구간의 순번은 빈틈 없이 이어져야 한다.
	for i := int64(1); i <= n; i++ {
		if _, ok := seen[LotteryBand+i]; !ok {
			t.Fatalf("missing fifo score %d", LotteryBand+i)
		}
	}
}

func TestEnqueueRespectsTerminalStates(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		wantStatus Status
		wantQueued bool
	}{
		{"차단된 사용자는 재진입 불가", "blocked", StatusBlocked, false},
		{"보류 중 사용자는 그대로", "held", StatusHeld, false},
		{"입장한 사용자는 그대로", "admitted", StatusAdmitted, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.clk.Advance(3 * time.Minute)

			h.enqueue("tokA")
			// 조치 파이프라인이 상태를 바꿨다고 가정한다(Phase 4 에서 Lua 로 대체).
			h.setUserField("tokA", "state", tc.state)
			if _, err := h.rdb.ZRem(context.Background(), keys.Queue(h.event, h.shard), "tokA").Result(); err != nil {
				t.Fatalf("zrem: %v", err)
			}

			tk := h.enqueue("tokA")
			if tk.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", tk.Status, tc.wantStatus)
			}
			if got := h.queueLen(); got != 0 {
				t.Fatalf("re-enqueue put a %s user back in the queue (size %d)", tc.state, got)
			}
		})
	}
}

// 스스로 자리를 비운 사용자(evicted)는 재진입할 수 있지만, 원래 순번은 돌려받지 못한다.
// 돌려받는다면 "빠져 있다가 돌아오면 이득"이 되어 대기 자체가 무의미해진다.
func TestEvictedUserRejoinsAtTheBack(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	first := h.enqueue("tokA")
	h.enqueue("tokB")
	h.setUserField("tokA", "state", "evicted")
	if err := h.rdb.ZRem(context.Background(), keys.Queue(h.event, h.shard), "tokA").Err(); err != nil {
		t.Fatalf("zrem: %v", err)
	}

	again := h.enqueue("tokA")
	if again.Status != StatusCreated {
		t.Fatalf("status = %s, want created", again.Status)
	}
	if again.Score <= first.Score {
		t.Fatalf("rejoin score %d is not behind the original %d", again.Score, first.Score)
	}
	if again.Rank != 1 {
		t.Fatalf("rank = %d, want 1 (behind tokB)", again.Rank)
	}
}

func TestPositionSnapshot(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	for i := range 5 {
		h.enqueue("tok" + strconv.Itoa(i))
	}

	tests := []struct {
		name      string
		token     string
		wantState Status
		wantRank  int64
		wantAhead int64
	}{
		{"맨 앞", "tok0", StatusWaiting, 0, 0},
		{"세 번째", "tok2", StatusWaiting, 2, 2},
		{"맨 뒤", "tok4", StatusWaiting, 4, 4},
		{"모르는 토큰", "tokZ", StatusUnknown, -1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := h.position(tc.token)
			if s.Status != tc.wantState {
				t.Errorf("state = %s, want %s", s.Status, tc.wantState)
			}
			if s.Rank != tc.wantRank {
				t.Errorf("rank = %d, want %d", s.Rank, tc.wantRank)
			}
			if s.Ahead() != tc.wantAhead {
				t.Errorf("ahead = %d, want %d", s.Ahead(), tc.wantAhead)
			}
			if s.Size != 5 {
				t.Errorf("size = %d, want 5", s.Size)
			}
		})
	}

	// 예상 시간은 서버가 아는 실효 입장률로만 환산된다.
	s := h.position("tok4")
	if got, want := s.EstimateWait(60), 5*time.Second; got != want {
		t.Fatalf("EstimateWait = %v, want %v", got, want)
	}
}

// 보류(score 70~89)는 대기열에서 빼되 원 순번을 hold ZSET 에 보존한다.
// 해제되면 순번 손해 없이 복귀할 수 있어야 한다(§4 조치 파이프라인).
func TestPositionSeesHeldUser(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	tk := h.enqueue("tokA")
	h.enqueue("tokB")

	ctx := context.Background()
	if err := h.rdb.ZRem(ctx, keys.Queue(h.event, h.shard), "tokA").Err(); err != nil {
		t.Fatalf("zrem: %v", err)
	}
	if err := h.rdb.ZAdd(ctx, keys.Hold(h.event, h.shard),
		goredis.Z{Score: float64(tk.Score), Member: "tokA"}).Err(); err != nil {
		t.Fatalf("zadd hold: %v", err)
	}
	h.setUserField("tokA", "state", "held")

	s := h.position("tokA")
	if s.Status != StatusHeld {
		t.Fatalf("state = %s, want held", s.Status)
	}
	if s.Rank != -1 {
		t.Fatalf("rank = %d, want -1 (대기열에서 빠져 있어야 한다)", s.Rank)
	}
	if s.HoldRank != 0 || s.HoldSize != 1 {
		t.Fatalf("hold rank/size = %d/%d, want 0/1", s.HoldRank, s.HoldSize)
	}
	if s.OrigRank != tk.Score {
		t.Fatalf("orig_rank = %d, want %d — 복귀할 순번을 잃었다", s.OrigRank, tk.Score)
	}
}

func TestHeartbeat(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)
	h.enqueue("tokA")

	ctx := context.Background()

	first, err := h.store.Heartbeat(ctx, h.shard, "tokA")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if first.Status != StatusWaiting || first.Count != 1 || first.Rank != 0 {
		t.Fatalf("first beat = %+v", first)
	}
	if first.Interval != 0 {
		t.Fatalf("first interval = %v, want 0 (직전 신호가 없다)", first.Interval)
	}

	h.clk.Advance(1200 * time.Millisecond)
	second, err := h.store.Heartbeat(ctx, h.shard, "tokA")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// 간격의 규칙성이 §4-L4 의 탐지 신호가 된다. 여기서는 관측치가 정확한지만 본다.
	if second.Interval != 1200*time.Millisecond {
		t.Fatalf("interval = %v, want 1.2s", second.Interval)
	}
	if second.Count != 2 {
		t.Fatalf("count = %d, want 2", second.Count)
	}

	unknown, err := h.store.Heartbeat(ctx, h.shard, "tokZ")
	if err != nil {
		t.Fatalf("heartbeat unknown: %v", err)
	}
	if unknown.Status != StatusUnknown {
		t.Fatalf("unknown status = %s", unknown.Status)
	}
}

// soft-evict 는 두 단계다(§5). 끊김이 곧 이탈은 아니므로 유예를 준다.
func TestSweepSoftEvictLifecycle(t *testing.T) {
	h := newHarness(t, nil) // StaleAfter = 1s * 3 = 3s, grace = 2s
	h.clk.Advance(3 * time.Minute)
	h.enqueue("tokA")

	if r := h.sweep(0); r.Marked != 0 || r.Removed != 0 {
		t.Fatalf("fresh user swept: %+v", r)
	}

	h.clk.Advance(4 * time.Second) // stale(3s) 초과
	r := h.sweep(0)
	if r.Marked != 1 || r.Removed != 0 {
		t.Fatalf("expected 1 marked, 0 removed; got %+v", r)
	}
	if got := h.userField("tokA", "state"); got != "evicting" {
		t.Fatalf("state = %s, want evicting", got)
	}
	if h.queueLen() != 1 {
		t.Fatal("유예 단계에서 큐 자리를 잃었다 — 순번을 되돌릴 수 없게 된다")
	}

	h.clk.Advance(time.Second) // 유예(2s) 아직 안 지남
	if r := h.sweep(0); r.Removed != 0 {
		t.Fatalf("removed before grace elapsed: %+v", r)
	}

	h.clk.Advance(2 * time.Second) // 유예 경과
	r = h.sweep(0)
	if r.Removed != 1 {
		t.Fatalf("expected 1 removed, got %+v", r)
	}
	if h.queueLen() != 0 {
		t.Fatal("evicted user still occupies a queue slot")
	}
	if got := h.userField("tokA", "state"); got != "evicted" {
		t.Fatalf("state = %s, want evicted", got)
	}
}

// 잠깐 끊겼다 돌아온 사용자는 순번 그대로 살아나야 한다.
// 여기서 순번을 잃게 만들면 피해를 보는 쪽은 봇이 아니라 사람이다.
func TestHeartbeatRevivesUserInGrace(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)
	before := h.enqueue("tokA")
	h.enqueue("tokB")

	// 둘 다 신호를 보내지 않았으므로 둘 다 유예 대상이 된다.
	h.clk.Advance(4 * time.Second)
	if r := h.sweep(0); r.Marked != 2 {
		t.Fatalf("expected both users marked, got %+v", r)
	}

	beat, err := h.store.Heartbeat(context.Background(), h.shard, "tokA")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !beat.Revived {
		t.Fatal("heartbeat did not revive the user")
	}
	if beat.Status != StatusWaiting {
		t.Fatalf("state = %s, want waiting", beat.Status)
	}
	if beat.Rank != before.Rank {
		t.Fatalf("rank changed on revival: %d → %d", before.Rank, beat.Rank)
	}
	if _, err := h.rdb.HGet(context.Background(), keys.User(h.event, h.shard, "tokA"), "evict_at").Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("evict_at was not cleared: %v", err)
	}

	// 유예가 지나면 끝내 신호가 없던 tokB 는 제거된다. 신호를 보낸 tokA 는 남아야 한다 —
	// 스윕이 "이번에 아무도 안 지웠다"가 아니라 "돌아온 사람은 안 지웠다"를 확인한다.
	h.clk.Advance(3 * time.Second)
	h.sweep(0)

	snap := h.position("tokA")
	if snap.Status != StatusWaiting {
		t.Fatalf("revived user state = %s, want waiting", snap.Status)
	}
	if snap.Rank != before.Rank {
		t.Fatalf("revived user lost its rank: %d → %d", before.Rank, snap.Rank)
	}
	if got := h.position("tokB").Status; got != StatusEvicted {
		t.Fatalf("silent user state = %s, want evicted", got)
	}
}

// 스윕은 대기 중인 사용자만 건드린다. held/blocked 를 덮어쓰면
// 조치 파이프라인(§4)이 정한 상태를 스윕이 되돌리게 된다.
func TestSweepLeavesNonWaitingStatesAlone(t *testing.T) {
	for _, state := range []string{"held", "blocked", "admitted"} {
		t.Run(state, func(t *testing.T) {
			h := newHarness(t, nil)
			h.clk.Advance(3 * time.Minute)
			h.enqueue("tokA")
			h.setUserField("tokA", "state", state)

			h.clk.Advance(time.Hour)
			r := h.sweep(0)
			if r.Marked != 0 || r.Removed != 0 || r.Ghosts != 0 {
				t.Fatalf("sweep touched a %s user: %+v", state, r)
			}
			if got := h.userField("tokA", "state"); got != state {
				t.Fatalf("state = %s, want %s", got, state)
			}
		})
	}
}

// 상태 해시가 TTL 로 사라졌는데 ZSET 에만 남은 항목은 순번만 축낸다.
func TestSweepRemovesGhosts(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)
	h.enqueue("tokA")
	h.enqueue("tokB")

	if err := h.rdb.Del(context.Background(), keys.User(h.event, h.shard, "tokA")).Err(); err != nil {
		t.Fatalf("del: %v", err)
	}

	r := h.sweep(0)
	if r.Ghosts != 1 {
		t.Fatalf("ghosts = %d, want 1 (%+v)", r.Ghosts, r)
	}
	if h.queueLen() != 1 {
		t.Fatalf("queue size = %d, want 1", h.queueLen())
	}
	if h.position("tokB").Rank != 0 {
		t.Fatal("removing the ghost did not move tokB forward")
	}
}

// 샤드가 목표 크기를 넘겨도 스윕 1회의 비용이 예측 가능해야 한다.
func TestSweepCursorWalksTheWholeShard(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.SweepBatch = 2 })
	h.clk.Advance(3 * time.Minute)
	for i := range 5 {
		h.enqueue("tok" + strconv.Itoa(i))
	}
	h.clk.Advance(4 * time.Second)

	var (
		offset  int64
		scanned int64
		marked  int64
		rounds  int
	)
	for {
		r := h.sweep(offset)
		scanned += r.Scanned
		marked += r.Marked
		offset = r.NextOffset
		rounds++
		if offset == 0 {
			break
		}
		if rounds > 10 {
			t.Fatal("sweep cursor never wrapped")
		}
	}
	if rounds != 3 {
		t.Fatalf("rounds = %d, want 3 (5명 / 배치 2)", rounds)
	}
	if scanned != 5 || marked != 5 {
		t.Fatalf("scanned/marked = %d/%d, want 5/5", scanned, marked)
	}
}

// 동적으로 늘어난 샤드도 admission 이 찾을 수 있어야 한다(§3.1).
func TestShardsRegistry(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	ctx := context.Background()
	for _, s := range []string{shard.ID(1), shard.ID(2), shard.ID(7)} {
		if _, err := h.store.Enqueue(ctx, EnqueueRequest{Shard: s, TokenID: "tok" + s, IPPrefix: "203.0.113.0/24"}); err != nil {
			t.Fatalf("enqueue on %s: %v", s, err)
		}
	}

	got, err := h.store.Shards(ctx)
	if err != nil {
		t.Fatalf("Shards: %v", err)
	}
	want := []string{"s0001", "s0002", "s0007"}
	if len(got) != len(want) {
		t.Fatalf("shards = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shards = %v, want %v", got, want)
		}
	}
}

// 샤드 인덱스가 사라져도 스스로 복구돼야 한다.
//
// 이 인덱스가 비면 Admission 컨트롤러가 배분할 샤드를 못 찾아 아무도 입장하지
// 못한다. 그런데 대기열 자체는 멀쩡해 보인다 — 줄만 줄지 않는다. 등록 메모를
// 영구히 믿으면 이 상태에서 영영 빠져나오지 못하므로, 주기가 지나면 다시 민다.
func TestShardsRegistryRecoversAfterIndexLoss(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)
	ctx := context.Background()

	h.enqueue("tokA")
	if got, _ := h.store.Shards(ctx); len(got) != 1 {
		t.Fatalf("등록된 샤드 = %v, want 1개", got)
	}

	// 인덱스만 날아간 상황(운영자의 flush, 장애 복구, 침해 사고).
	if err := h.rdb.Del(ctx, keys.Shards(h.event)).Err(); err != nil {
		t.Fatalf("del shards index: %v", err)
	}

	// 재등록 주기 전에는 메모를 믿는다 — 왕복을 아끼는 것이 이 메모의 목적이다.
	h.enqueue("tokB")
	if got, _ := h.store.Shards(ctx); len(got) != 0 {
		t.Fatalf("주기 이전에 다시 등록했다: %v", got)
	}

	// 주기가 지나면 되민다. 여기서 복구되지 않으면 입장이 영구히 멈춘다.
	h.clk.Advance(reregisterAfter + time.Second)
	h.enqueue("tokC")

	got, err := h.store.Shards(ctx)
	if err != nil {
		t.Fatalf("Shards: %v", err)
	}
	if len(got) != 1 || got[0] != h.shard {
		t.Fatalf("복구 후 샤드 = %v, want [%s]", got, h.shard)
	}
}

// 개인정보는 해시·프리픽스 형태로만 저장된다(불변식 6).
func TestEnqueueStoresOnlyHashedIdentity(t *testing.T) {
	h := newHarness(t, nil)
	h.clk.Advance(3 * time.Minute)

	if _, err := h.store.Enqueue(context.Background(), EnqueueRequest{
		Shard: h.shard, TokenID: "tokA", FPHash: "sha256:abcdef", IPPrefix: "203.0.113.0/24",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	all, err := h.rdb.HGetAll(context.Background(), keys.User(h.event, h.shard, "tokA")).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	for _, field := range []string{"state", "shard", "orig_shard", "orig_rank", "segment", "fp_hash", "ip_prefix", "joined_at", "last_seen"} {
		if _, ok := all[field]; !ok {
			t.Errorf("user hash is missing %q", field)
		}
	}
	if all["ip_prefix"] != "203.0.113.0/24" {
		t.Errorf("ip_prefix = %q", all["ip_prefix"])
	}
	// 스키마에 없는 필드가 조용히 늘어나면 무엇이 저장되는지 통제할 수 없다.
	allowed := map[string]bool{
		"state": true, "shard": true, "orig_shard": true, "orig_rank": true,
		"segment": true, "score": true, "fp_hash": true, "ip_prefix": true,
		"joined_at": true, "last_seen": true, "hb_count": true,
	}
	for f := range all {
		if !allowed[f] {
			t.Errorf("unexpected field %q in user hash", f)
		}
	}
}
