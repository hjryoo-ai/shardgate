//go:build integration

// 사다리가 실제로 사다리인지 — greylist 에서 위로 올라가고, 재챌린지로 내려온다.
//
// 이 파일의 테스트는 REPORT §3.5 의 결함에서 나왔다. 봇 2,400마리가 전부 greylist
// 에 멈춰 점수 40.2 에서 소수점 한 자리도 움직이지 않았고, 원인은 `move_shard.lua`
// 의 noop 경로가 점수 기록보다 앞에 있었던 것이다. 관측은 살아 있는데 기록이 죽어
// 있으면 격리가 곧 종점이 된다(불변식 7).
package botscore

import (
	"context"
	"testing"

	"github.com/hjr/shardgate/internal/keys"
	lua "github.com/hjr/shardgate/scripts/lua"
)

// 점수는 조치와 무관하게 계속 기록된다(불변식 7).
//
// greylist 로 옮겨진 뒤에도 스코어러의 판정이 Redis 에 반영돼야 한다. 반영되지
// 않으면 사다리의 40~69 칸이 점수 냉동고가 되어 위로도 아래로도 움직이지 못한다.
func TestGreylistKeepsRecordingScore(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)

	// 1) 첫 판정: 실제로 옮겨진다.
	h.apply(decision(h.shard, "tokA", 45, ActionGreylist))
	if got := h.userField("tokA", "state"); got != "greylist" {
		t.Fatalf("state = %s, want greylist", got)
	}
	if got := h.userField("tokA", "score"); got != "45" {
		t.Fatalf("score = %s, want 45", got)
	}

	// 2) 이미 greylist 인 사용자에 대한 판정: 이동은 noop 이지만 점수는 남는다.
	for _, score := range []float64{52, 61, 68} {
		h.apply(decision(h.shard, "tokA", score, ActionGreylist))
	}

	if got := h.userField("tokA", "state"); got != "greylist" {
		t.Fatalf("state = %s, want greylist (noop 이 상태를 바꾸면 안 된다)", got)
	}
	if got := h.userField("tokA", "score"); got != "68" {
		t.Fatalf("score = %s, want 68 — noop 경로가 점수 기록을 건너뛰고 있다", got)
	}
	// 샤드 점수 해시도 같이 따라와야 한다(스코어러가 아니라 운영이 보는 값이다).
	v, err := h.rdb.HGet(context.Background(), keys.Score(h.event, h.shard), "tokA").Result()
	if err != nil {
		t.Fatalf("hget score: %v", err)
	}
	if v != "68" {
		t.Fatalf("score hash = %s, want 68", v)
	}
}

// 점수가 스스로 내려와 열리는 문(ActionRestore)은 관찰 시계를 되감지 **않는다.**
//
// 두 문의 점수는 성격이 다르다. 재챌린지의 클램프(35)는 판정이 아니라 **양보**다 —
// 탐지기의 실제 믿음은 그보다 높았는데 통과를 인정해 낮춰 준 값이라, 참값을 다시
// 세우려면 추가 관찰이 필요하다. 반면 점수 하락은 탐지기가 여러 창을 보고 내린
// **결론**이므로 그 위에 관찰을 더 얹는 것은 이미 한 일을 두 번 하는 것이다.
//
// 이 비대칭이 뚫릴 구멍인지는 부하에서 확인한다: 격리됐던 사용자가 이 경로로 나와
// 입장하면 입장 채널 항등식의 잔차로 나타난다(REPORT §3.8 에서 잔차 0).
func TestNaturalRestoreDoesNotRewindObservationClock(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)
	h.apply(decision(h.shard, "tokA", 45, ActionGreylist))

	h.apply(decision(h.shard, "tokA", 12, ActionRestore))

	if got := h.userField("tokA", "state"); got != "waiting" {
		t.Fatalf("state = %s, want waiting", got)
	}
	if n, err := h.rdb.HExists(context.Background(),
		keys.User(h.event, h.shard, "tokA"), "observe_from").Result(); err != nil || n {
		t.Fatalf("observe_from exists = %v (err=%v) — 점수 하락은 결론이지 양보가 아니다", n, err)
	}
}

// greylist 에 있는 사용자도 보류·차단으로 올라간다.
//
// 올라갈 때 멤버가 greylist ZSET 에 남아 있으면 안 된다 — 남으면 ZCARD 가 부풀고
// 그 값이 예산 배분의 입력이라 남의 자리까지 흔든다.
func TestGreylistClimbsToHoldAndBlock(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 42)
	h.apply(decision(h.shard, "tokA", 45, ActionGreylist))

	if _, ok := h.rank(keys.Queue(h.event, h.grey), "tokA"); !ok {
		t.Fatal("greylist 이동이 안 됐다")
	}

	// 계속 봇처럼 굴면 보류로 올라간다.
	h.apply(decision(h.shard, "tokA", 75, ActionHold))
	if got := h.userField("tokA", "state"); got != "held" {
		t.Fatalf("state = %s, want held", got)
	}
	if _, ok := h.rank(keys.Queue(h.event, h.grey), "tokA"); ok {
		t.Fatal("보류로 올라갔는데 greylist ZSET 에 유령 멤버가 남았다")
	}
	r, ok := h.rank(keys.Hold(h.event, h.shard), "tokA")
	if !ok || r != 42 {
		t.Fatalf("hold rank = %v (%v), want 42 — 보류는 원 순번을 보존한다", r, ok)
	}

	// 더 올라가면 차단이다.
	h.apply(decision(h.shard, "tokA", 95, ActionBlock))
	if got := h.userField("tokA", "state"); got != "blocked" {
		t.Fatalf("state = %s, want blocked", got)
	}
	for _, key := range []string{
		keys.Queue(h.event, h.shard), keys.Queue(h.event, h.grey), keys.Hold(h.event, h.shard),
	} {
		if _, ok := h.rank(key, "tokA"); ok {
			t.Fatalf("차단된 토큰이 %s 에 남아 있다", key)
		}
	}
}

// greylist 를 거쳐 차단까지 간 뒤에도 멤버가 어디에도 남지 않는다 —
// 위 테스트의 보류 단계를 건너뛴 경로(70 을 스치지 않고 90 으로).
func TestGreylistToBlockDirectly(t *testing.T) {
	h := newHarness(t)
	h.seed("tokA", 7)
	h.apply(decision(h.shard, "tokA", 45, ActionGreylist))
	h.apply(decision(h.shard, "tokA", 95, ActionBlock))

	if got := h.userField("tokA", "state"); got != "blocked" {
		t.Fatalf("state = %s, want blocked", got)
	}
	if _, ok := h.rank(keys.Queue(h.event, h.grey), "tokA"); ok {
		t.Fatal("차단된 토큰이 greylist ZSET 에 남았다")
	}
	if len(h.sink.blocks) != 1 {
		t.Fatalf("block records = %d, want 1", len(h.sink.blocks))
	}
}

// 사라진 사용자에 대한 뒤늦은 판정은 아무것도 되살리지 않는다.
// 점수를 남기겠다고 해시를 다시 만들면 evict 가 무의미해진다.
func TestGreylistOnVanishedUserWritesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.apply(decision(h.shard, "ghost", 45, ActionGreylist))

	n, err := h.rdb.Exists(ctx, keys.User(h.event, h.shard, "ghost")).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if n != 0 {
		t.Fatal("사라진 사용자의 해시가 되살아났다")
	}
	if n, err := h.rdb.HExists(ctx, keys.Score(h.event, h.shard), "ghost").Result(); err != nil || n {
		t.Fatalf("score hash gained a ghost entry (%v, %v)", n, err)
	}
}

func TestRechallengeScriptLoads(t *testing.T) {
	h := newHarness(t)
	if _, err := h.rdb.ScriptLoad(context.Background(), lua.MustRead("rechallenge.lua")).Result(); err != nil {
		t.Fatalf("rechallenge.lua failed to load: %v", err)
	}
}
