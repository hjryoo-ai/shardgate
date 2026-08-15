//go:build integration

// Phase 4 완료 기준(§10)인 "봇 시뮬레이터 탐지 동작 확인"이다.
//
// 단위 테스트는 점수 계산만 본다. 여기서는 §11 의 혼합 시나리오를 축소해
// **신호 → 점수 → 조치 → Redis 상태 전이**까지 전 구간을 한 번에 돌린다.
// 봇 유형은 §11 이 정한 세 가지다: naive 스크립트 / heartbeat 모사 / 분산 IP.
//
// 임계값은 손대지 않는다. 탐지가 기대에 못 미치면 고칠 곳은 시나리오지 임계값이 아니다.
package botscore

import (
	"context"
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/telemetry"
)

// 시뮬레이션 규모. 실제 샤드(≈1,000명)를 축소한 표본이다.
const (
	simHumans      = 200
	simBotsPerType = 10
	simBots        = simBotsPerType * 3

	// simRounds 는 창(60초)의 개수다. 지수평활(decay 0.9) 때문에 조치는
	// 창 하나로 나오지 않는다 — 시간축을 충분히 줘야 사다리를 오른다.
	simRounds = 30

	simWindow     = 60 * time.Second
	simInterval   = 5 * time.Second
	simBeats      = int(simWindow / simInterval)
	simDifficulty = 18
)

// 봇 유형(§11).
const (
	kindHuman       = "human"
	kindNaive       = "naive"       // 시계처럼 정확한 스크립트
	kindMimic       = "mimic"       // heartbeat 지터까지 흉내 내는 봇
	kindDistributed = "distributed" // 프록시로 IP 대역만 흩은 봇
)

// persona 는 시뮬레이션 참가자 한 명이다.
type persona struct {
	token, fp, ip string
	kind          string

	// shard 는 지금 속한 샤드다. greylist 로 옮겨지면 이후 신호도 그 샤드에서 나온다 —
	// 실제로도 greylist 재챌린지가 shard 클레임이 바뀐 새 토큰을 준다.
	shard string

	at      time.Time // 다음 heartbeat 시각
	prev    time.Time // 직전 heartbeat 시각(간격 계산용)
	jitter  bool
	solveMS int64
}

// beat 는 사람/봇의 리듬대로 heartbeat 을 만든다.
func (p *persona) beat(rng *rand.Rand) (time.Time, int64) {
	d := simInterval.Milliseconds()
	if p.jitter {
		// ±25%. 사람의 손과 네트워크가 만드는 폭이다.
		d += rng.Int64N(2500) - 1250
	}
	p.at = p.at.Add(time.Duration(d) * time.Millisecond)

	var interval int64
	if !p.prev.IsZero() {
		interval = p.at.Sub(p.prev).Milliseconds()
	}
	p.prev = p.at
	return p.at, interval
}

// round 는 창 하나 분량의 신호를 스코어러에 넣는다.
func (p *persona) round(rng *rand.Rand, s *Scorer, event string) {
	for range simBeats {
		at, interval := p.beat(rng)
		s.Observe(telemetry.Event{
			Kind: telemetry.KindHeartbeat, EventID: event, Shard: p.shard, TokenID: p.token,
			At: at, IntervalMS: interval, FPHash: p.fp, IPPrefix: p.ip,
		})
	}
	// PoW 풀이는 진입 때 한 번뿐이지만, 창이 넘어가도 분포에 남아야 비교가 된다.
	s.Observe(telemetry.Event{
		Kind: telemetry.KindChallenge, EventID: event, Shard: p.shard, TokenID: p.token,
		At: p.at, SolveMS: p.solveMS, Difficulty: simDifficulty, FPHash: p.fp, IPPrefix: p.ip,
	})
}

// population 은 정상 사용자와 봇 세 유형을 만든다.
//
// 세 유형은 각각 다른 신호를 피해 간다. 어떤 봇도 모든 신호를 동시에 피하지
// 못한다는 것이 다층 방어(§4)의 요점이고, 여기서 확인하려는 것도 그것이다.
func population(rng *rand.Rand, origin string, start time.Time) []*persona {
	out := make([]*persona, 0, simHumans+simBots)

	for i := range simHumans {
		out = append(out, &persona{
			token: "human" + strconv.Itoa(i), kind: kindHuman, shard: origin,
			fp: "fp_h" + strconv.Itoa(i),
			// 100개 대역에 흩어져 있다. 통신사 NAT 처럼 두어 명이 겹치는 것은 정상이다.
			ip: "198.51." + strconv.Itoa(i%100) + ".0/24",
			// 사람은 각자 다른 순간에 대기실을 연다.
			at:      start.Add(time.Duration(rng.Int64N(simInterval.Milliseconds())) * time.Millisecond),
			jitter:  true,
			solveMS: 300 + rng.Int64N(1200),
		})
	}

	for i := range simBotsPerType {
		id := strconv.Itoa(i)
		// naive: 시계처럼 정확하고, 한 기기에서, 한 대역으로. 전 신호에 걸린다.
		out = append(out, &persona{
			token: "naive" + id, kind: kindNaive, shard: origin,
			fp: "fp_farm", ip: "203.0.113.0/24",
			at: start, jitter: false, solveMS: 25 + rng.Int64N(35),
		})
		// mimic: 지터를 흉내 내 규칙성 신호를 피한다. 기기와 대역은 그대로다.
		// 타이밍 위조는 공짜지만 지문과 PoW 비용은 그렇지 않다.
		out = append(out, &persona{
			token: "mimic" + id, kind: kindMimic, shard: origin,
			fp: "fp_farm", ip: "203.0.113.0/24",
			at:      start.Add(time.Duration(rng.Int64N(simInterval.Milliseconds())) * time.Millisecond),
			jitter:  true,
			solveMS: 25 + rng.Int64N(35),
		})
		// distributed: 레지덴셜 프록시로 대역만 흩는다. 같은 자동화 이미지라 지문은 하나다.
		out = append(out, &persona{
			token: "dist" + id, kind: kindDistributed, shard: origin,
			fp: "fp_proxy", ip: "192.0." + id + ".0/24",
			at: start, jitter: false, solveMS: 25 + rng.Int64N(35),
		})
	}
	return out
}

// seedPersona 는 대기열에 참가자를 심는다(enqueue.lua 가 만드는 상태와 같은 모양).
func (h *harness) seedPersona(p *persona, rank int64) {
	h.t.Helper()
	ctx := context.Background()

	if err := h.rdb.ZAdd(ctx, keys.Queue(h.event, h.shard),
		goredis.Z{Score: float64(rank), Member: p.token}).Err(); err != nil {
		h.t.Fatalf("zadd: %v", err)
	}
	if err := h.rdb.HSet(ctx, keys.User(h.event, h.shard, p.token),
		"state", "waiting", "shard", h.shard,
		"fp_hash", p.fp, "ip_prefix", p.ip).Err(); err != nil {
		h.t.Fatalf("hset: %v", err)
	}
}

// tally 는 유형별 조치 집계다.
type tally struct {
	total, greylist, held, blocked int
	// ever 는 시뮬레이션 도중 한 번이라도 조치를 받은 인원이다.
	// 점수가 임계 근처에서 오르내리면 마지막 창의 스냅샷만으로는 놓친다.
	ever         int
	minS, maxS   float64
	scoresSeeded bool
}

func (t *tally) score(v float64) {
	if !t.scoresSeeded {
		t.minS, t.maxS, t.scoresSeeded = v, v, true
		return
	}
	t.minS = math.Min(t.minS, v)
	t.maxS = math.Max(t.maxS, v)
}

func TestBotSimulatorIsDetected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// 고정 시드. 탐지율이 실행마다 흔들리면 회귀를 볼 수 없다.
	rng := rand.New(rand.NewPCG(0x5EED, 0x9A7E))
	start := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	people := population(rng, h.shard, start)
	byToken := make(map[string]*persona, len(people))
	for i, p := range people {
		h.seedPersona(p, int64(i))
		byToken[p.token] = p
	}

	now := start
	scorer := NewScorer(h.event, h.cfg, nil, nil).WithClock(func() time.Time { return now })

	// everActed 는 도중에 한 번이라도 조치를 받은 토큰이다. 점수가 임계 근처에서
	// 오르내리는 사용자는 마지막 창의 스냅샷만으로 판단할 수 없다.
	everActed := make(map[string]bool, len(people))

	var final []Decision
	for round := range simRounds {
		for _, p := range people {
			p.round(rng, scorer, h.event)
		}
		// 창 끝. 사람의 위상 지연(최대 5초)까지 창 안에 들도록 여유를 둔다.
		now = start.Add(time.Duration(round+1)*simWindow + 10*time.Second)

		final = scorer.Flush()
		h.act.Apply(ctx, final)

		// 조치가 샤드를 옮겼으면 다음 창의 신호도 그 샤드에서 나온다.
		// 의심군만 모인 별도 모집단에서 다시 관찰하는 것이 §4-L5 의 두 번째 이점이고,
		// 풀려난 사용자는 원 샤드로 돌아가 다시 정상 모집단과 비교된다.
		for _, d := range final {
			switch d.Action {
			case ActionGreylist:
				byToken[d.TokenID].shard = h.grey
				everActed[d.TokenID] = true
			case ActionRestore:
				byToken[d.TokenID].shard = h.shard
			case ActionHold, ActionBlock:
				everActed[d.TokenID] = true
			case ActionObserve:
			}
		}
	}

	if len(final) != len(people) {
		t.Fatalf("판정 %d건, 참가자 %d명 — 창 설정이 어긋나 시나리오가 성립하지 않았다",
			len(final), len(people))
	}

	// ── 집계 ────────────────────────────────────────────────────────────
	counts := map[string]*tally{
		kindHuman: {}, kindNaive: {}, kindMimic: {}, kindDistributed: {},
	}
	for _, p := range people {
		counts[p.kind].total++
		if everActed[p.token] {
			counts[p.kind].ever++
		}
	}
	for _, d := range final {
		c := counts[byToken[d.TokenID].kind]
		c.score(d.Score)
		switch d.Action {
		case ActionGreylist:
			c.greylist++
		case ActionHold:
			c.held++
		case ActionBlock:
			c.blocked++
		case ActionObserve, ActionRestore:
		}
	}

	t.Logf("%-12s %5s %9s %5s %6s %6s %12s", "유형", "인원", "greylist", "보류", "차단", "누적조치", "최종점수")
	for _, kind := range []string{kindHuman, kindNaive, kindMimic, kindDistributed} {
		c := counts[kind]
		t.Logf("%-12s %5d %9d %5d %6d %6d %6.1f~%.1f",
			kind, c.total, c.greylist, c.held, c.blocked, c.ever, c.minS, c.maxS)
	}

	botsActed := counts[kindNaive].ever + counts[kindMimic].ever + counts[kindDistributed].ever
	recall := float64(botsActed) / float64(simBots)
	fpr := float64(counts[kindHuman].ever) / float64(simHumans)
	t.Logf("탐지율(recall) %.1f%% (%d/%d), 오탐율(FPR) %.1f%% (%d/%d)",
		recall*100, botsActed, simBots, fpr*100, counts[kindHuman].ever, simHumans)

	// ── 판정 ────────────────────────────────────────────────────────────
	// 봇은 세 유형 모두 조치를 받아야 한다. 한 유형이라도 0이면 그 유형의
	// 회피 전략이 통했다는 뜻이고, 그건 다층 방어가 뚫린 것이다.
	for _, kind := range []string{kindNaive, kindMimic, kindDistributed} {
		if counts[kind].ever == 0 {
			t.Errorf("%s 봇이 한 대도 걸리지 않았다 — 이 유형의 회피가 통했다", kind)
		}
	}
	// 아무 신호도 피하지 않는 naive 는 전원 걸려야 한다. 여기서 한 대라도 빠지면
	// 탐지가 아니라 우연을 보고 있는 것이다.
	if counts[kindNaive].ever != counts[kindNaive].total {
		t.Errorf("naive 봇 %d/%d 만 걸렸다 — 가장 쉬운 표적을 놓쳤다",
			counts[kindNaive].ever, counts[kindNaive].total)
	}
	// 봇과 사람의 점수대가 겹치면 임계값을 어디에 두든 오탐과 미탐을 맞바꿀 뿐이다.
	// 분리 자체가 되는지가 탐지가 성립하는지의 조건이다.
	if counts[kindHuman].maxS >= math.Min(counts[kindMimic].minS,
		math.Min(counts[kindNaive].minS, counts[kindDistributed].minS)) {
		t.Errorf("사람 최고점 %.1f 이 봇 최저점과 겹친다 — 점수대가 분리되지 않았다",
			counts[kindHuman].maxS)
	}
	// mimic 은 타이밍을 위조해 규칙성·상호상관을 피하므로 지문·대역·PoW 만 남고,
	// 그 합은 greylist 선(40) 언저리다. 게다가 먼저 격리된 개체는 비교할 모집단을
	// 잃어 점수가 내려간다 — 상대적 신호의 구조적 한계다(§12). 그래서 이 유형만
	// 부분 탐지를 기대치로 잡는다. 전체 탐지율은 그 손실을 포함한 값이다.
	if recall < 0.8 {
		t.Errorf("탐지율 %.1f%% — 80%% 미만이면 시뮬레이터가 통과한 것이다", recall*100)
	}
	// 오탐율은 이 설계의 핵심 지표다(§11). 사람이 격리되면 대기열의 공정성이 무너진다.
	if fpr > 0.02 {
		t.Errorf("오탐율 %.1f%% — 2%% 를 넘었다", fpr*100)
	}
	if counts[kindHuman].blocked > 0 || counts[kindHuman].held > 0 {
		t.Errorf("정상 사용자 %d명이 보류/차단됐다 — 되돌리기 어려운 조치는 사람에게 닿으면 안 된다",
			counts[kindHuman].held+counts[kindHuman].blocked)
	}
	// 차단은 여러 신호의 합의를 요구한다(불변식 3).
	for _, d := range final {
		if d.Action == ActionBlock && d.Contributing < h.cfg.MinSignalsToBlock {
			t.Errorf("%s 가 신호 %d개로 차단됐다 — 불변식 3 위반", d.TokenID, d.Contributing)
		}
	}

	// ── Redis 상태 ──────────────────────────────────────────────────────
	// 판정이 실제 상태 전이로 이어졌는지 본다. 여기까지 와야 "탐지 동작 확인"이다.
	originQueue := keys.Queue(h.event, h.shard)
	greyQueue := keys.Queue(h.event, h.grey)

	for _, p := range people {
		state := h.userField(p.token, "state")
		_, inOrigin := h.rank(originQueue, p.token)

		if p.kind == kindHuman {
			if state != "waiting" || !inOrigin {
				t.Errorf("정상 사용자 %s: state=%s, 원 대기열 잔류=%v", p.token, state, inOrigin)
			}
			continue
		}
		if state == "waiting" {
			continue // 걸리지 않은 봇은 위의 탐지율 판정이 이미 셌다
		}
		if inOrigin {
			t.Errorf("조치된 봇 %s(state=%s)가 아직 원 대기열에 있다", p.token, state)
		}
		if state == "greylist" {
			// 격리는 처벌이 아니다 — 순번을 그대로 들고 간다.
			rank, ok := h.rank(greyQueue, p.token)
			if !ok {
				t.Errorf("greylist 봇 %s 가 greylist 대기열에 없다", p.token)
				continue
			}
			if got := h.userField(p.token, "orig_rank"); got != strconv.FormatInt(int64(rank), 10) {
				t.Errorf("%s 의 순번이 바뀌었다: greylist %v, orig_rank %s", p.token, rank, got)
			}
		}
	}

	// 의심도가 올라가 있어야 다음 챌린지 난이도가 오른다(§4-L2 적응형 난이도).
	for _, subject := range []string{"fp_farm", "fp_proxy"} {
		n, err := h.rdb.Get(ctx, keys.Suspicion(h.event, subject)).Int64()
		if err != nil || n <= 0 {
			t.Errorf("의심도 %s = %d (%v) — 적응형 난이도의 입력이 비어 있다", subject, n, err)
		}
	}

	// 조치는 근거와 함께 남아야 한다(§6 감사 로그).
	if len(h.sink.audits) == 0 {
		t.Error("감사 기록이 하나도 없다 — 조치의 근거가 남지 않았다")
	}
	for _, r := range h.sink.audits {
		if r.Action == "" || r.TokenID == "" {
			t.Errorf("불완전한 감사 기록: %+v", r)
		}
	}
}
