//go:build integration

// 재챌린지 — greylist 를 벗어나는 문(DESIGN.md §4).
//
// 여기서 확인하는 것은 세 가지다: 원 순번이 손해 없이 돌아오는가, 점수가 0 이
// 아니라 임계 직하로만 내려가는가, 그리고 횟수를 소진하면 보류로 올라가는가.
// 가운데 항목이 이 문이 값싼 출구가 되지 않게 하는 지점이다 — 통과는 재검증
// 1회 통과일 뿐 무죄 판결이 아니다.
package queue

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/shard"
)

// greylist 로 옮긴 상태를 만든다(move_shard.lua 가 만드는 모양과 같다).
func (h *harness) greylist(token string, rank int64, score int) string {
	h.t.Helper()
	ctx := context.Background()

	grey, err := shard.Greylist(h.shard)
	if err != nil {
		h.t.Fatalf("greylist id: %v", err)
	}
	if err := h.rdb.ZRem(ctx, keys.Queue(h.event, h.shard), token).Err(); err != nil {
		h.t.Fatalf("zrem: %v", err)
	}
	if err := h.rdb.ZAdd(ctx, keys.Queue(h.event, grey),
		goredis.Z{Score: float64(rank), Member: token}).Err(); err != nil {
		h.t.Fatalf("zadd grey: %v", err)
	}
	h.setUserField(token,
		"state", "greylist", "shard", grey, "orig_shard", h.shard,
		"orig_rank", rank, "score", score, "greylisted_at", h.clk.Now().UnixMilli())
	return grey
}

func (h *harness) rechallenge(token string, max, pass, hold int) RechallengeResult {
	h.t.Helper()
	res, err := h.store.Rechallenge(context.Background(), RechallengeRequest{
		Shard: h.shard, TokenID: token, MaxAttempts: max, PassScore: pass, HoldScore: hold,
	})
	if err != nil {
		h.t.Fatalf("rechallenge %s: %v", token, err)
	}
	return res
}

func (h *harness) inZSet(key, token string) (float64, bool) {
	h.t.Helper()
	v, err := h.rdb.ZScore(context.Background(), key, token).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, false
	}
	if err != nil {
		h.t.Fatalf("zscore %s: %v", key, err)
	}
	return v, true
}

// 통과하면 원 샤드의 **원 순번**으로 돌아온다. 그 사이에 앞사람이 빠졌다면
// 오히려 앞당겨지고, 뒷사람이 늘었어도 밀리지 않는다.
func TestRechallengeRestoresOriginalRank(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	h.enqueue("tokB")

	origRank := h.position("tokB").Rank
	grey := h.greylist("tokB", origRank, 55)

	res := h.rechallenge("tokB", 2, 35, 70)

	if res.Outcome != RechallengeRestored {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}
	if res.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", res.Status)
	}
	if res.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", res.Attempts)
	}
	if _, ok := h.inZSet(keys.Queue(h.event, grey), "tokB"); ok {
		t.Fatal("복귀했는데 greylist ZSET 에 남아 있다")
	}
	if got, ok := h.inZSet(keys.Queue(h.event, h.shard), "tokB"); !ok || int64(got) != origRank {
		t.Fatalf("restored rank = %v (%v), want %d", got, ok, origRank)
	}
	if got := h.userField("tokB", "shard"); got != h.shard {
		t.Fatalf("shard = %s, want %s", got, h.shard)
	}
}

// **점수는 0 으로 리셋되지 않는다.** 임계 직하로 클램프만 한다 —
// 대행으로 퍼즐만 넘긴 봇이 행동 신호로 곧바로 다시 올라올 수 있어야 한다.
func TestRechallengeClampsScoreInsteadOfResetting(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	h.greylist("tokA", h.position("tokA").Rank, 66)

	res := h.rechallenge("tokA", 2, 35, 70)

	if res.Score != 35 {
		t.Fatalf("score = %d, want 35 (0 으로 리셋하면 사실상 면제권이 된다)", res.Score)
	}
	if got := h.userField("tokA", "score"); got != "35" {
		t.Fatalf("stored score = %s, want 35", got)
	}
	if got := h.position("tokA").BotScore; got != 35 {
		t.Fatalf("snapshot score = %d, want 35", got)
	}
}

// 복귀는 관찰 시계를 되감는다(§3.4). 되감지 않으면 게이트를 이미 통과한 상태로
// 돌아와, 문이 열릴 때마다 §12-7 의 경주가 한 번씩 재개된다.
//
// joined_at 은 건드리지 않는다 — 진입 시각과 관찰 기산점은 복귀 이후로 서로 다른
// 사실이고, 진입 시각은 감사와 추첨 구간 판정의 근거다.
func TestRechallengeRewindsObservationClock(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	joinedAt := h.userField("tokA", "joined_at")

	// 진입 후 시간이 흐르고 생존 신호가 쌓인 상태에서 격리된다.
	for range 4 {
		if _, err := h.store.Heartbeat(context.Background(), h.shard, "tokA"); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}
	h.clk.Advance(10 * time.Minute)
	h.greylist("tokA", h.position("tokA").Rank, 55)

	restoredAt := h.clk.Now().UnixMilli()
	if res := h.rechallenge("tokA", 2, 35, 70); res.Outcome != RechallengeRestored {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}

	if got := h.userField("tokA", "observe_from"); got != strconv.FormatInt(restoredAt, 10) {
		t.Fatalf("observe_from = %s, want %d (복귀 시점으로 되감겨야 한다)", got, restoredAt)
	}
	if got := h.userField("tokA", "hb_base"); got != "4" {
		t.Fatalf("hb_base = %s, want 4 (복귀 전 신호를 다시 세면 안 된다)", got)
	}
	if got := h.userField("tokA", "joined_at"); got != joinedAt {
		t.Fatalf("joined_at %s → %s: 진입 시각은 관찰 시계와 다른 사실이다", joinedAt, got)
	}
	if got := h.position("tokA").ObserveFrom.UnixMilli(); got != restoredAt {
		t.Fatalf("snapshot observe_from = %d, want %d — 화면도 같은 시계를 봐야 한다", got, restoredAt)
	}
}

// **복귀 후 보이는 순번은 뒤로 갈 수 있다 — 그리고 그게 맞는 동작이다.**
//
// 부하 측정에서 복귀 590건 중 137건이 격리 직전보다 **높은** 순번으로 돌아왔다
// (REPORT §3.8). 자리를 잃은 것처럼 보이지만 아니다: 격리된 사람은 원 대기열에서
// 빠져 있으므로, 그 사이에 뒷사람이 보는 순번은 실제보다 **앞당겨져 있다.**
// 앞사람이 돌아오면 그 앞당김이 사라진다.
//
// 보존되는 것은 ZSET 점수(줄에서의 자리)이지 ZRANK(지금 몇 번째인가)가 아니다.
// 둘을 구분하지 않으면 이 관측을 결함으로 오독하고, 있지도 않은 버그를 고치려다
// 진짜 불변식(orig_rank 보존)을 깨게 된다.
func TestRestoredRankRisesWhenSomeoneAheadReturns(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokAhead") // 앞사람
	h.enqueue("tokBehind")

	aheadRank := h.position("tokAhead").Rank
	behindRank := h.position("tokBehind").Rank
	if aheadRank >= behindRank {
		t.Fatalf("전제가 틀렸다: ahead=%d behind=%d", aheadRank, behindRank)
	}

	// 둘 다 격리된다. 앞사람이 빠지면 뒷사람의 순번이 앞당겨진다.
	h.greylist("tokAhead", aheadRank, 55)
	greyBehind := h.greylist("tokBehind", behindRank, 55)
	_ = greyBehind

	// 뒷사람이 먼저 복귀한다 — 앞사람이 아직 없으므로 순번이 당겨져 보인다.
	h.rechallenge("tokBehind", 2, 35, 70)
	pulledUp := h.position("tokBehind").Rank
	if pulledUp >= behindRank {
		t.Fatalf("앞사람이 빠졌는데 순번이 안 당겨졌다: %d → %d", behindRank, pulledUp)
	}

	// 이제 앞사람이 복귀한다. 뒷사람이 **보는** 순번은 원래대로 돌아간다.
	h.rechallenge("tokAhead", 2, 35, 70)
	after := h.position("tokBehind")

	if after.Rank <= pulledUp {
		t.Fatalf("앞사람이 돌아왔는데 순번이 그대로다: %d → %d", pulledUp, after.Rank)
	}
	if after.Rank != behindRank {
		t.Fatalf("순번 = %d, want %d — 원래 자리로 돌아와야 한다", after.Rank, behindRank)
	}
	// 줄에서의 자리(ZSET 점수)는 처음부터 끝까지 한 번도 바뀌지 않았다.
	if got, ok := h.inZSet(keys.Queue(h.event, h.shard), "tokBehind"); !ok || int64(got) != behindRank {
		t.Fatalf("ZSET 점수 = %v (%v), want %d — 보존되는 것은 이쪽이다", got, ok, behindRank)
	}
}

// 클램프는 대입이 아니다 — 이미 더 낮은 점수를 올리지 않는다.
func TestRechallengeNeverRaisesScore(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	h.greylist("tokA", h.position("tokA").Rank, 12)

	res := h.rechallenge("tokA", 2, 35, 70)

	if res.Score != 12 {
		t.Fatalf("score = %d, want 12 (클램프가 점수를 올리면 안 된다)", res.Score)
	}
}

// 횟수를 소진하면 복귀 대신 보류로 올라간다. 계속 걸리고 계속 푸는 것 자체가 신호다.
// 보류도 순번을 보존하므로 이 승급은 되돌릴 수 있다.
func TestRechallengeExhaustionEscalatesToHold(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	origRank := h.position("tokA").Rank
	grey := h.greylist("tokA", origRank, 55)

	// 두 번은 통과한다.
	for i := 1; i <= 2; i++ {
		res := h.rechallenge("tokA", 2, 35, 70)
		if res.Outcome != RechallengeRestored {
			t.Fatalf("attempt %d: outcome = %s, want restored", i, res.Outcome)
		}
		if res.Attempts != int64(i) {
			t.Fatalf("attempt %d: attempts = %d", i, res.Attempts)
		}
		// 다시 격리된다(계속 봇처럼 굴었다).
		h.greylist("tokA", origRank, 55)
	}

	res := h.rechallenge("tokA", 2, 35, 70)

	if res.Outcome != RechallengeExhausted {
		t.Fatalf("outcome = %s, want exhausted", res.Outcome)
	}
	if res.Status != StatusHeld {
		t.Fatalf("status = %s, want held", res.Status)
	}
	if res.Score != 70 {
		t.Fatalf("score = %d, want 70 (보류 임계로 올라가야 한다)", res.Score)
	}
	if _, ok := h.inZSet(keys.Queue(h.event, grey), "tokA"); ok {
		t.Fatal("보류로 올라갔는데 greylist ZSET 에 유령 멤버가 남았다")
	}
	if got, ok := h.inZSet(keys.Hold(h.event, h.shard), "tokA"); !ok || int64(got) != origRank {
		t.Fatalf("hold rank = %v (%v), want %d — 보류도 순번을 보존한다", got, ok, origRank)
	}
}

// greylist 가 아닌 상태는 재챌린지로 풀리지 않는다.
// 특히 held/blocked 는 greylist 보다 위 칸이라, 아래 칸의 통과 조건으로 되돌리면
// 사다리가 사다리가 아니게 된다.
func TestRechallengeOnlyAppliesToGreylist(t *testing.T) {
	cases := []struct {
		name  string
		state string
		want  RechallengeOutcome
	}{
		{"waiting 은 이미 정상이다", "waiting", RechallengeNoop},
		{"보류는 재챌린지로 풀지 않는다", "held", RechallengeNoop},
		{"차단은 되돌리지 않는다", "blocked", RechallengeNoop},
		{"입장한 사용자는 대상이 아니다", "admitted", RechallengeNoop},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.enqueue("tokA")
			h.setUserField("tokA", "state", tc.state)

			res := h.rechallenge("tokA", 2, 35, 70)

			if res.Outcome != tc.want {
				t.Fatalf("outcome = %s, want %s", res.Outcome, tc.want)
			}
			if got := h.userField("tokA", "state"); got != tc.state {
				t.Fatalf("state = %s, want %s (건드리면 안 된다)", got, tc.state)
			}
		})
	}
}

// 사라진 사용자에게는 자리를 만들어 주지 않는다.
func TestRechallengeOnVanishedUser(t *testing.T) {
	h := newHarness(t, nil)

	res := h.rechallenge("ghost", 2, 35, 70)

	if res.Outcome != RechallengeUnknown {
		t.Fatalf("outcome = %s, want unknown", res.Outcome)
	}
}

// 돌아갈 순번을 모르면 상태를 그대로 둔다 — 지어낸 순번은 누군가의 자리를 빼앗는다.
func TestRechallengeWithoutOriginalRank(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	h.greylist("tokA", h.position("tokA").Rank, 55)
	if err := h.rdb.HDel(context.Background(),
		keys.User(h.event, h.shard, "tokA"), "orig_rank").Err(); err != nil {
		t.Fatalf("hdel: %v", err)
	}

	res := h.rechallenge("tokA", 2, 35, 70)

	if res.Outcome != RechallengeNoRank {
		t.Fatalf("outcome = %s, want no_rank", res.Outcome)
	}
	if got := h.userField("tokA", "state"); got != "greylist" {
		t.Fatalf("state = %s, want greylist", got)
	}
}

// 같은 통과를 두 번 보내도 결과가 늘어나지 않는다(불변식 4).
// 두 번째 호출은 이미 waiting 이므로 noop 이고, 시도 횟수도 오르지 않는다.
func TestRechallengeIsIdempotentOnRetry(t *testing.T) {
	h := newHarness(t, nil)
	h.enqueue("tokA")
	origRank := h.position("tokA").Rank
	h.greylist("tokA", origRank, 55)

	first := h.rechallenge("tokA", 2, 35, 70)
	second := h.rechallenge("tokA", 2, 35, 70)

	if first.Outcome != RechallengeRestored {
		t.Fatalf("first = %s, want restored", first.Outcome)
	}
	if second.Outcome != RechallengeNoop {
		t.Fatalf("second = %s, want noop", second.Outcome)
	}
	if second.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 — 재시도가 기회를 태우면 안 된다", second.Attempts)
	}
	if got, ok := h.inZSet(keys.Queue(h.event, h.shard), "tokA"); !ok || int64(got) != origRank {
		t.Fatalf("rank = %v (%v), want %d", got, ok, origRank)
	}
}
