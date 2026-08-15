package botscore

import (
	"math"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/telemetry"
)

func testConfig() config.BotScore {
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false }, "scorer", ":0")
	if err != nil {
		panic(err)
	}
	return cfg.BotScore
}

var base = time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)

// judgeAt 은 관측이 끝난 직후다. 창(기본 60초) 안에 들어야 판정이 나온다 —
// 여기를 잘못 잡으면 Flush 가 빈 결과를 내고 테스트가 공허하게 통과한다.
var judgeAt = base.Add(70 * time.Second)

// beat 는 heartbeat 신호 하나를 만든다.
func beat(shard, token string, at time.Time, intervalMS int64, fp, ip string) telemetry.Event {
	return telemetry.Event{
		Kind: telemetry.KindHeartbeat, EventID: "evt1", Shard: shard, TokenID: token,
		At: at, IntervalMS: intervalMS, FPHash: fp, IPPrefix: ip,
	}
}

// feedHuman 은 지터가 있는 사람 흉내를 낸다.
func feedHuman(s *Scorer, shard, token string, n int, seed int64) {
	interval := int64(5000)
	at := base
	for i := range n {
		// 결정적이면서도 불규칙한 지터. ±25% 범위.
		jitter := (seed*int64(i*7+3))%2500 - 1250
		d := interval + jitter
		at = at.Add(time.Duration(d) * time.Millisecond)
		s.Observe(beat(shard, token, at, d, "fp_"+token, "198.51.100.0/24"))
	}
}

// feedBot 은 시계처럼 정확하고 서로 동기화된 봇을 흉내 낸다.
func feedBot(s *Scorer, shard, token string, n int, fp, ip string) {
	at := base
	for range n {
		at = at.Add(5 * time.Second) // 분산 0
		s.Observe(beat(shard, token, at, 5000, fp, ip))
	}
}

// 사람은 조치 대상이 아니어야 한다. 오탐율이 이 설계의 핵심 지표다(§11).
func TestHumansAreNotFlagged(t *testing.T) {
	s := NewScorer("evt1", testConfig(), nil, nil).
		WithClock(func() time.Time { return judgeAt })

	const humans = 30
	for i := range humans {
		feedHuman(s, "s0001", "human"+string(rune('a'+i)), 12, int64(i*13+1))
	}

	decisions := s.Flush()
	if len(decisions) != humans {
		t.Fatalf("judged %d of %d users — 창 설정이 어긋나 테스트가 공허하다", len(decisions), humans)
	}
	for _, d := range decisions {
		if d.Action != ActionObserve {
			t.Errorf("%s got %s at score %.1f (signals %v)", d.TokenID, d.Action, d.Score, d.Signals)
		}
	}
}

// 봇팜은 신호가 여러 개 함께 뜬다: 규칙성 + 동기화 + 지문 중복 + 대역 집중.
func TestBotFarmScoresHigherThanHumans(t *testing.T) {
	cfg := testConfig()
	s := NewScorer("evt1", cfg, nil, nil).
		WithClock(func() time.Time { return judgeAt })

	for i := range 20 {
		feedHuman(s, "s0001", "human"+string(rune('a'+i)), 12, int64(i*13+1))
	}
	// 같은 지문·같은 대역을 공유하며 동시에 움직이는 봇 10대.
	for i := range 10 {
		feedBot(s, "s0001", "bot"+string(rune('a'+i)), 12, "fp_farm", "203.0.113.0/24")
	}

	var humanMax, botMin = 0.0, math.MaxFloat64
	botSignals, bots, people := 0, 0, 0
	for _, d := range s.Flush() {
		if len(d.TokenID) > 3 && d.TokenID[:3] == "bot" {
			bots++
			botMin = math.Min(botMin, d.Score)
			botSignals = max(botSignals, d.Contributing)
			continue
		}
		people++
		humanMax = math.Max(humanMax, d.Score)
	}
	if bots != 10 || people != 20 {
		t.Fatalf("judged %d bots and %d humans, want 10 and 20", bots, people)
	}

	if botMin <= humanMax {
		t.Fatalf("bots (min %.1f) did not separate from humans (max %.1f)", botMin, humanMax)
	}
	if botSignals < 2 {
		t.Fatalf("bot farm lit only %d signal(s); a farm should trip several", botSignals)
	}
}

// 표본이 모자라면 판정하지 않는다. 적은 표본의 판정이 사람을 봇으로 만든다.
func TestFewSamplesAreNotJudged(t *testing.T) {
	cfg := testConfig()
	s := NewScorer("evt1", cfg, nil, nil).
		WithClock(func() time.Time { return judgeAt })

	s.Observe(beat("s0001", "newcomer", base.Add(time.Minute), 0, "fp_x", "198.51.100.0/24"))

	if got := s.Flush(); len(got) != 0 {
		t.Fatalf("judged a token with %d samples: %+v", cfg.MinSamples, got)
	}
}

// 불변식 3: 어떤 단일 신호도 즉시 차단의 근거가 될 수 없다.
func TestSingleSignalCannotBlock(t *testing.T) {
	cfg := testConfig()
	// 한 신호만으로 100점이 나오도록 가중치를 몰아 준다 — 즉 "점수만으로는
	// 차단선을 넘는" 최악의 조건을 만들어 놓고, 그래도 차단되지 않음을 본다.
	cfg.Weights = config.Weights{Heartbeat: 1}
	cfg.Decay = 0 // 누적을 걷어내고 한 창의 결과만 본다

	s := NewScorer("evt1", cfg, nil, nil).
		WithClock(func() time.Time { return judgeAt })

	// 지문도 대역도 제각각이라 규칙성 외에는 아무 신호도 뜨지 않는 봇들.
	for i := range 12 {
		tok := "lonebot" + string(rune('a'+i))
		at := base.Add(time.Duration(i) * 137 * time.Millisecond) // 서로 동기화되지 않도록 위상 분산
		for range 12 {
			at = at.Add(5 * time.Second)
			s.Observe(beat("s0001", tok, at, 5000, "fp_"+tok, "203.0.113."+string(rune('0'+i))+"/24"))
		}
	}

	decisions := s.Flush()
	if len(decisions) != 12 {
		t.Fatalf("judged %d of 12 tokens — 시나리오가 성립하지 않았다", len(decisions))
	}
	blocked, capped := 0, 0
	for _, d := range decisions {
		if d.Action == ActionBlock {
			blocked++
		}
		if d.CappedFrom == ActionBlock {
			capped++
			if d.Action != ActionHold {
				t.Errorf("capped action = %s, want hold", d.Action)
			}
		}
	}
	if blocked > 0 {
		t.Fatalf("%d tokens blocked on a single signal — 불변식 3 위반", blocked)
	}
	if capped == 0 {
		t.Skip("시나리오가 차단선에 닿지 않았다 — 상한 동작을 확인하지 못했다")
	}
}

// 점수는 한 창에 튀지 않고 누적된다. "즉시 차단하지 않는다"(§4)의 시간축 구현.
func TestScoreAccumulatesGradually(t *testing.T) {
	cfg := testConfig()
	s := NewScorer("evt1", cfg, nil, nil)

	var score float64
	for i := range 5 {
		score = s.accumulate("tok", 100)
		if score >= float64(cfg.Block) {
			t.Fatalf("reached the block threshold after %d windows (%.1f)", i+1, score)
		}
	}

	// 계속 들어오면 결국 오르긴 한다 — 영영 못 잡으면 그것대로 문제다.
	for range 100 {
		score = s.accumulate("tok", 100)
	}
	if score < float64(cfg.Block) {
		t.Fatalf("score never reached the block threshold: %.1f", score)
	}
}

// 점수가 내려오면 조치를 푼다. 오탐 복구 경로가 없으면 격리는 영구 차단과 같다.
func TestScoreDropRestores(t *testing.T) {
	cfg := testConfig()
	s := NewScorer("evt1", cfg, nil, nil)

	var d Decision
	if got := s.action("tok", float64(cfg.Hold), 2, &d); got != ActionHold {
		t.Fatalf("= %s, want hold", got)
	}
	if got := s.action("tok", 0, 0, &d); got != ActionRestore {
		t.Fatalf("= %s, want restore", got)
	}
	// 이미 풀린 뒤에는 다시 풀지 않는다.
	if got := s.action("tok", 0, 0, &d); got != ActionObserve {
		t.Fatalf("= %s, want observe", got)
	}
}

func TestActionLadder(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name         string
		score        float64
		contributing int
		want         Action
	}{
		{"관찰", 10, 3, ActionObserve},
		{"greylist 하한", float64(cfg.Greylist), 2, ActionGreylist},
		{"greylist 상한", float64(cfg.Hold) - 1, 2, ActionGreylist},
		{"보류 하한", float64(cfg.Hold), 2, ActionHold},
		{"차단 하한", float64(cfg.Block), 2, ActionBlock},
		{"만점", 100, 5, ActionBlock},
		{"신호 하나면 차단 대신 보류", 100, 1, ActionHold},
		{"신호 0개면 차단 대신 보류", 100, 0, ActionHold},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScorer("evt1", cfg, nil, nil)
			var d Decision
			if got := s.action("tok", tc.score, tc.contributing, &d); got != tc.want {
				t.Fatalf("= %s, want %s", got, tc.want)
			}
		})
	}
}

// 창 밖으로 나간 관측치는 버려진다. 아니면 메모리가 이벤트 내내 증가한다.
func TestWindowExpiry(t *testing.T) {
	cfg := testConfig()
	now := base
	s := NewScorer("evt1", cfg, nil, nil).WithClock(func() time.Time { return now })

	feedBot(s, "s0001", "bot", 12, "fp_a", "203.0.113.0/24")
	if got := len(s.Flush()); got == 0 {
		t.Fatal("fresh observations were not judged")
	}

	now = base.Add(time.Hour)
	if got := s.Flush(); len(got) != 0 {
		t.Fatalf("stale observations still judged: %d", len(got))
	}
}
