//go:build integration

// 조치 파이프라인의 상태 전이는 실제 Redis 로만 검증한다.
// 여기서 확인하는 성질(원자성, 슬롯 공유, 순번 보존)은 Lua 가 실제로 실행돼야 나온다.
package botscore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"
)

const redisImage = "redis:8-alpine"

var redisURL string

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcredis.Run(ctx, redisImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis: %v\n", err)
		os.Exit(1)
	}
	if redisURL, err = ctr.ConnectionString(ctx); err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		fmt.Fprintf(os.Stderr, "redis url: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = testcontainers.TerminateContainer(ctr)
	os.Exit(code)
}

var eventSeq atomic.Int64

type harness struct {
	t     *testing.T
	rdb   goredis.UniversalClient
	act   *Actuator
	cfg   config.BotScore
	event string
	shard string
	grey  string
	sink  *recordingSink
}

type recordingSink struct {
	audits []AuditRecord
	blocks []BlockRecord
}

func (s *recordingSink) Audit(_ context.Context, r AuditRecord) { s.audits = append(s.audits, r) }
func (s *recordingSink) Block(_ context.Context, r BlockRecord) { s.blocks = append(s.blocks, r) }

func newHarness(t *testing.T) *harness {
	t.Helper()

	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	full, err := config.LoadFrom(func(string) (string, bool) { return "", false }, "scorer", ":0")
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg := full.BotScore

	event := "bs" + strconv.FormatInt(eventSeq.Add(1), 10)
	sink := &recordingSink{}
	origin := shard.ID(1)
	grey, err := shard.Greylist(origin)
	if err != nil {
		t.Fatalf("greylist id: %v", err)
	}

	return &harness{
		t: t, rdb: rdb, cfg: cfg, event: event, shard: origin, grey: grey, sink: sink,
		act: NewActuator(rdb, event, cfg, time.Hour, sink, nil, nil),
	}
}

// seed 는 대기열에 사용자를 하나 심는다(enqueue.lua 가 만드는 상태와 같은 모양).
func (h *harness) seed(token string, rank int64) {
	h.t.Helper()
	ctx := context.Background()

	if err := h.rdb.ZAdd(ctx, keys.Queue(h.event, h.shard),
		goredis.Z{Score: float64(rank), Member: token}).Err(); err != nil {
		h.t.Fatalf("zadd: %v", err)
	}
	if err := h.rdb.HSet(ctx, keys.User(h.event, h.shard, token),
		"state", "waiting", "shard", h.shard, "orig_shard", h.shard,
		"orig_rank", rank, "score", 0,
		"fp_hash", "fp_"+token, "ip_prefix", "203.0.113.0/24").Err(); err != nil {
		h.t.Fatalf("hset: %v", err)
	}
}

func (h *harness) userField(token, field string) string {
	h.t.Helper()
	v, err := h.rdb.HGet(context.Background(), keys.User(h.event, h.shard, token), field).Result()
	if err != nil {
		h.t.Fatalf("hget %s: %v", field, err)
	}
	return v
}

func (h *harness) rank(key, token string) (float64, bool) {
	h.t.Helper()
	v, err := h.rdb.ZScore(context.Background(), key, token).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, false
	}
	if err != nil {
		h.t.Fatalf("zscore: %v", err)
	}
	return v, true
}

func (h *harness) apply(d Decision) {
	h.t.Helper()
	h.act.Apply(context.Background(), []Decision{d})
}

func decision(shard, token string, score float64, action Action) Decision {
	return Decision{
		Shard: shard, TokenID: token, Score: score, Action: action,
		Signals:      Signals{SignalHeartbeat: 0.9, SignalFingerprint: 0.8},
		Contributing: 2,
	}
}

func TestActionScriptsLoad(t *testing.T) {
	h := newHarness(t)
	for _, name := range []string{"apply_action.lua", "move_shard.lua", "restore_shard.lua"} {
		t.Run(name, func(t *testing.T) {
			if _, err := h.rdb.ScriptLoad(context.Background(), lua.MustRead(name)).Result(); err != nil {
				t.Fatalf("%s failed to load: %v", name, err)
			}
		})
	}
}

// 관찰은 아무것도 바꾸지 않는다. 점수만 남는다.
func TestObserveChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	h.apply(decision(h.shard, "tokA", 25, ActionObserve))

	if got := h.userField("tokA", "state"); got != "waiting" {
		t.Fatalf("state = %s, want waiting", got)
	}
	if r, ok := h.rank(keys.Queue(h.event, h.shard), "tokA"); !ok || r != 42 {
		t.Fatalf("rank = %v (%v), want 42", r, ok)
	}
	if got := h.userField("tokA", "score"); got != "25" {
		t.Fatalf("score = %s, want 25", got)
	}
	// 관찰까지 감사 로그를 남기면 감사 테이블이 대기열보다 커진다.
	if len(h.sink.audits) != 0 {
		t.Fatalf("observe wrote %d audit rows", len(h.sink.audits))
	}
}

// 보류는 대기열에서 빼되 원 순번을 보존한다 — 해제되면 손해 없이 돌아온다.
func TestHoldPreservesRank(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	h.apply(decision(h.shard, "tokA", 75, ActionHold))

	if got := h.userField("tokA", "state"); got != "held" {
		t.Fatalf("state = %s, want held", got)
	}
	if _, ok := h.rank(keys.Queue(h.event, h.shard), "tokA"); ok {
		t.Fatal("held user still occupies a queue slot")
	}
	r, ok := h.rank(keys.Hold(h.event, h.shard), "tokA")
	if !ok || r != 42 {
		t.Fatalf("hold rank = %v (%v), want 42", r, ok)
	}
	if len(h.sink.audits) != 1 || h.sink.audits[0].Action != "hold" {
		t.Fatalf("audit = %+v", h.sink.audits)
	}
}

// 차단은 큐와 보류석 양쪽에서 제거하고 근거를 남긴다.
func TestBlockRemovesEverywhereAndRecordsEvidence(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	h.apply(decision(h.shard, "tokA", 95, ActionBlock))

	if got := h.userField("tokA", "state"); got != "blocked" {
		t.Fatalf("state = %s, want blocked", got)
	}
	if _, ok := h.rank(keys.Queue(h.event, h.shard), "tokA"); ok {
		t.Fatal("blocked user is still in the queue")
	}
	if _, ok := h.rank(keys.Hold(h.event, h.shard), "tokA"); ok {
		t.Fatal("blocked user is still in hold")
	}
	if len(h.sink.blocks) != 1 {
		t.Fatalf("blocks = %+v", h.sink.blocks)
	}
	// 차단은 근거 없이 남기지 않는다 — 나중에 이의를 받을 때 볼 것이 있어야 한다.
	if h.sink.blocks[0].Evidence["contributing"] == nil {
		t.Fatalf("block evidence has no signal breakdown: %+v", h.sink.blocks[0].Evidence)
	}
	// 원본 지문이 아니라 해시만 남는다(불변식 6).
	if h.sink.blocks[0].FPHash != "fp_tokA" {
		t.Fatalf("fp_hash = %q", h.sink.blocks[0].FPHash)
	}
}

// 뒤늦게 도착한 낮은 점수가 차단을 되돌리지 않는다.
func TestBlockIsNotUndoneByLaterActions(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)
	h.apply(decision(h.shard, "tokA", 95, ActionBlock))

	for _, a := range []Action{ActionObserve, ActionHold, ActionGreylist, ActionRestore} {
		h.apply(decision(h.shard, "tokA", 10, a))
		if got := h.userField("tokA", "state"); got != "blocked" {
			t.Fatalf("%s changed a blocked user to %s", a, got)
		}
	}
}

// greylist 이동은 순번을 그대로 들고 간다. 처벌이 아니라 재검증이기 때문이다.
func TestGreylistMovePreservesRank(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	h.apply(decision(h.shard, "tokA", 55, ActionGreylist))

	if got := h.userField("tokA", "state"); got != "greylist" {
		t.Fatalf("state = %s, want greylist", got)
	}
	if got := h.userField("tokA", "shard"); got != h.grey {
		t.Fatalf("shard = %s, want %s", got, h.grey)
	}
	if _, ok := h.rank(keys.Queue(h.event, h.shard), "tokA"); ok {
		t.Fatal("user is still in the origin queue")
	}
	r, ok := h.rank(keys.Queue(h.event, h.grey), "tokA")
	if !ok || r != 42 {
		t.Fatalf("greylist rank = %v (%v), want 42", r, ok)
	}
	// 돌아갈 자리는 처음 그 자리로 남아 있어야 한다.
	if got := h.userField("tokA", "orig_shard"); got != h.shard {
		t.Fatalf("orig_shard = %s, want %s", got, h.shard)
	}
	if got := h.userField("tokA", "orig_rank"); got != "42" {
		t.Fatalf("orig_rank = %s, want 42", got)
	}
}

// greylist 이동은 의심도를 올려 다음 진입의 PoW 난이도를 끌어올린다(§4-L2).
func TestGreylistRaisesPoWDifficulty(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	full, err := config.LoadFrom(func(string) (string, bool) { return "", false }, "gate", ":0")
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	provider := NewDifficulty(h.rdb, h.event, full.Challenge, h.cfg, nil)
	subject := challenge.Subject{FPHash: "fp_tokA", IPPrefix: "203.0.113.0/24"}

	ctx := context.Background()
	before, err := provider.Difficulty(ctx, subject)
	if err != nil {
		t.Fatalf("difficulty: %v", err)
	}
	if before != full.Challenge.BaseDifficulty {
		t.Fatalf("clean subject difficulty = %d, want base %d", before, full.Challenge.BaseDifficulty)
	}

	h.apply(decision(h.shard, "tokA", 55, ActionGreylist))

	after, err := provider.Difficulty(ctx, subject)
	if err != nil {
		t.Fatalf("difficulty: %v", err)
	}
	if after != before+h.cfg.GreylistDifficulty {
		t.Fatalf("difficulty = %d, want %d", after, before+h.cfg.GreylistDifficulty)
	}

	// 아무 관계 없는 사용자는 영향을 받지 않아야 한다 — 그렇지 않으면 한 명의
	// 의심이 샤드 전체의 진입 비용이 된다.
	clean, err := provider.Difficulty(ctx, challenge.Subject{FPHash: "fp_other", IPPrefix: "198.51.100.0/24"})
	if err != nil {
		t.Fatalf("difficulty: %v", err)
	}
	if clean != full.Challenge.BaseDifficulty {
		t.Fatalf("unrelated subject difficulty = %d, want base", clean)
	}
}

// 재검증을 통과하면 원 샤드의 원 순번으로 돌아온다. 이 경로가 없으면
// 격리는 사실상 영구 차단이고, 그러면 의심을 넓게 볼 수 없다.
func TestRestoreReturnsToTheOriginalRank(t *testing.T) {
	tests := []struct {
		name string
		via  Action
	}{
		{"greylist 에서 복귀", ActionGreylist},
		{"보류에서 복귀", ActionHold},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.seed("tokA", 42)
			h.seed("tokB", 7) // 앞사람은 그대로 있어야 한다

			h.apply(decision(h.shard, "tokA", 60, tc.via))
			h.apply(decision(h.shard, "tokA", 5, ActionRestore))

			if got := h.userField("tokA", "state"); got != "waiting" {
				t.Fatalf("state = %s, want waiting", got)
			}
			if got := h.userField("tokA", "shard"); got != h.shard {
				t.Fatalf("shard = %s, want %s", got, h.shard)
			}
			r, ok := h.rank(keys.Queue(h.event, h.shard), "tokA")
			if !ok || r != 42 {
				t.Fatalf("restored rank = %v (%v), want 42", r, ok)
			}
			if _, ok := h.rank(keys.Queue(h.event, h.grey), "tokA"); ok {
				t.Fatal("still in the greylist queue after restore")
			}
			if _, ok := h.rank(keys.Hold(h.event, h.shard), "tokA"); ok {
				t.Fatal("still in hold after restore")
			}
			if r, ok := h.rank(keys.Queue(h.event, h.shard), "tokB"); !ok || r != 7 {
				t.Fatalf("bystander moved: %v (%v)", r, ok)
			}
		})
	}
}

// 사라진 사용자에 대한 뒤늦은 판정이 사용자를 되살리지 않는다.
func TestActionsOnUnknownUserAreNoops(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for _, a := range []Action{ActionObserve, ActionHold, ActionBlock, ActionGreylist, ActionRestore} {
		h.apply(decision(h.shard, "ghost", 95, a))
	}

	n, err := h.rdb.Exists(ctx, keys.User(h.event, h.shard, "ghost")).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if n != 0 {
		t.Fatal("an action resurrected a user that no longer exists")
	}
	if len(h.sink.blocks) != 0 {
		t.Fatalf("recorded a block for a ghost: %+v", h.sink.blocks)
	}
}

// greylist 이동이 Lua 한 번으로 끝나려면 원 샤드와 같은 슬롯에 있어야 한다.
// Cluster 로 올렸을 때 비로소 터지는 종류의 문제라, 여기서 슬롯을 직접 확인한다.
func TestGreylistKeysShareTheOriginSlot(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	touched := []string{
		keys.Queue(h.event, h.shard),
		keys.Queue(h.event, h.grey),
		keys.Hold(h.event, h.shard),
		keys.Score(h.event, h.shard),
		keys.User(h.event, h.shard, "tokA"),
	}

	var want int64 = -1
	for _, k := range touched {
		slot, err := h.rdb.Do(ctx, "CLUSTER", "KEYSLOT", k).Int64()
		if err != nil {
			t.Skipf("CLUSTER KEYSLOT unavailable: %v", err)
		}
		if want < 0 {
			want = slot
			continue
		}
		if slot != want {
			t.Fatalf("key %q hashes to slot %d, want %d — move_shard.lua 는 Cluster 에서 실패한다", k, slot, want)
		}
	}
}
