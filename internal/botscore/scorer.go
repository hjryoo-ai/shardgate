package botscore

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/telemetry"
)

// Action 은 조치 사다리의 한 칸이다(§4).
type Action string

// 조치. 위에서 아래로 갈수록 되돌리기 어려워진다.
const (
	// ActionObserve 는 점수만 남긴다. 사용자는 아무것도 느끼지 못한다.
	ActionObserve Action = "observe"
	// ActionGreylist 는 의심군 샤드로 옮긴다. 순번은 그대로 들고 간다.
	ActionGreylist Action = "greylist"
	// ActionHold 는 대기열에서 빼되 원 순번을 보존한다. admit 대상에서만 제외된다.
	ActionHold Action = "hold"
	// ActionBlock 은 토큰을 무효화한다. 유일하게 되돌릴 수 없는 조치다.
	ActionBlock Action = "block"
	// ActionRestore 는 greylist/hold 를 풀고 원 순번으로 되돌린다.
	ActionRestore Action = "restore"
)

// Decision 은 한 토큰에 대한 판정이다.
type Decision struct {
	Shard   string
	TokenID string
	Score   float64
	Action  Action
	Signals Signals

	// Contributing 은 SignalMinContrib 이상으로 가리킨 신호 수다.
	// 차단은 이 값이 MinSignalsToBlock 이상일 때만 나온다(불변식 3).
	Contributing int
	// CappedFrom 은 신호가 모자라 차단이 보류로 낮춰졌을 때 원래 조치를 담는다.
	CappedFrom Action
}

// Scorer 는 신호를 모아 주기적으로 점수를 내고 조치를 정한다.
//
// 점수 계산과 조치 적용은 분리돼 있다. Flush 는 판정만 만들고, 실제 상태 전이는
// Actuator 가 Lua 로 수행한다 — 판정 로직을 Redis 없이 테스트할 수 있어야
// 임계값을 "테스트가 통과하도록" 만지고 싶은 유혹이 생기지 않는다.
type Scorer struct {
	cfg config.BotScore
	win *windows
	log *slog.Logger
	met *obs.Metrics

	mu     sync.Mutex
	scores map[string]float64 // token_id → 누적 점수(지수평활)
	states map[string]Action  // token_id → 마지막으로 적용한 조치

	eventID string
	now     func() time.Time
}

// NewScorer 는 스코어러를 만든다.
func NewScorer(eventID string, cfg config.BotScore, log *slog.Logger, met *obs.Metrics) *Scorer {
	if log == nil {
		log = slog.Default()
	}
	return &Scorer{
		cfg: cfg, win: newWindows(), log: log, met: met,
		scores:  make(map[string]float64),
		states:  make(map[string]Action),
		eventID: eventID,
		now:     time.Now,
	}
}

// WithClock 은 시계를 갈아 끼운다(테스트용).
func (s *Scorer) WithClock(fn func() time.Time) *Scorer {
	s.now = fn
	return s
}

// Observe 는 신호 한 건을 창에 넣는다. 계산은 하지 않는다.
func (s *Scorer) Observe(e telemetry.Event) {
	if !e.Valid() {
		return
	}
	if s.met != nil {
		s.met.TelemetryEvents.WithLabelValues(string(e.Kind), "in").Inc()
	}
	if e.Kind == telemetry.KindRechallenge {
		// 관측이 아니라 통지다. 창에 넣으면 재챌린지 한 번이 heartbeat 표본처럼
		// 세어져 MinSamples 를 앞당긴다.
		s.Clamp(e.TokenID)
		return
	}
	s.win.observe(e)
}

// Clamp 는 재챌린지를 통과한 토큰의 누적 점수를 임계 직하로 내린다(§4).
//
// **0 으로 되돌리지 않는다.** 통과는 재검증 1회 통과일 뿐 무죄 판결이 아니다.
// CAPTCHA 를 사람이 대신 풀어 주는 봇은 복귀 직후부터 행동 신호로 다시 올라와야
// 하는데, 0 에서 시작하면 지수평활 때문에 그 재상승에 창 스무 개가 더 필요해져
// 사실상 면제권이 된다. 반대로 점수를 그대로 두면 다음 창에서 즉시 재격리돼
// 통과가 의미를 잃는다.
//
// 클램프이지 대입이 아니다 — 이미 더 낮은 점수를 올리는 일은 없다.
// 사다리 상태도 함께 지운다. 실제 복귀는 이미 Lua 가 했으므로, 여기 남겨 두면
// 다음 창에서 "점수가 내려왔으니 복귀시켜라"(ActionRestore)가 한 번 헛돈다.
func (s *Scorer) Clamp(tokenID string) {
	limit := float64(s.cfg.RechallengePassScore)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scores[tokenID] > limit {
		s.scores[tokenID] = limit
	}
	delete(s.states, tokenID)
}

// Flush 는 창을 훑어 판정을 만든다. Redis 를 건드리지 않는다.
func (s *Scorer) Flush() []Decision {
	cutoff := s.now().Add(-s.cfg.Window)
	shards := s.win.snapshot(cutoff)

	var out []Decision
	for shard, obs := range shards {
		st := analyze(obs, s.cfg)
		for _, o := range obs {
			if o.samples() < s.cfg.MinSamples {
				// 표본이 모자라면 판정하지 않는다. 적은 표본으로 내린 판정은
				// 사람을 봇으로 만드는 가장 흔한 경로다.
				continue
			}
			out = append(out, s.decide(shard, o, st))
		}
	}

	// 같은 샤드의 판정이 뭉쳐 나오도록 정렬한다(로그·감사 추적이 쉬워진다).
	sort.Slice(out, func(i, j int) bool {
		if out[i].Shard != out[j].Shard {
			return out[i].Shard < out[j].Shard
		}
		return out[i].TokenID < out[j].TokenID
	})
	return out
}

func (s *Scorer) decide(shard string, o *observation, st shardStats) Decision {
	signals := evaluate(o, st, s.cfg)

	raw, contributing := s.combine(signals)
	score := s.accumulate(o.tokenID, raw)

	d := Decision{
		Shard: shard, TokenID: o.tokenID, Score: score,
		Signals: signals, Contributing: contributing,
	}
	d.Action = s.action(o.tokenID, score, contributing, &d)

	if s.met != nil {
		s.met.ScoreValue.WithLabelValues(s.eventID).Observe(score)
		for name, v := range signals {
			s.met.SignalScore.WithLabelValues(string(name)).Observe(v * 100)
		}
	}
	return d
}

// combine 은 신호를 가중 합산한다.
//
// 값이 없는 신호는 0 으로 둔다 — 즉 점수를 **깎는** 쪽으로 작용한다. 관측되지
// 않은 것을 의심의 근거로 삼지 않겠다는 뜻이고, 그 결과 신호가 하나뿐인 사용자는
// 구조적으로 높은 점수에 도달할 수 없다.
func (s *Scorer) combine(sig Signals) (score float64, contributing int) {
	w := s.cfg.Weights
	weights := map[Signal]float64{
		SignalHeartbeat:   w.Heartbeat,
		SignalCorrelation: w.Correlation,
		SignalFingerprint: w.Fingerprint,
		SignalIPPrefix:    w.IPPrefix,
		SignalPoW:         w.PoW,
	}

	for name, v := range sig {
		score += weights[name] * v
		if v >= s.cfg.SignalMinContrib {
			contributing++
		}
	}
	return clamp01(score) * 100, contributing
}

// accumulate 는 새 관측을 누적 점수에 지수평활로 섞는다.
//
// 한 창의 결과만으로 조치하지 않기 위해서다. decay=0.9 면 점수 100 짜리 신호가
// 계속 들어와도 차단선(90)에 닿기까지 스무 번 넘는 창이 필요하다. 그 시간이
// "즉시 차단하지 않는다"(§4)를 코드로 옮긴 것이다.
func (s *Scorer) accumulate(tokenID string, raw float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.scores[tokenID]
	score := prev*s.cfg.Decay + raw*(1-s.cfg.Decay)
	s.scores[tokenID] = score
	return score
}

// action 은 누적 점수를 조치로 옮긴다.
//
// 차단만은 점수 외에 "여러 신호가 함께 가리켰는가"를 추가로 요구한다.
// 임계값을 넘겼다는 사실 하나로 되돌릴 수 없는 조치를 내리지 않는다(불변식 3).
func (s *Scorer) action(tokenID string, score float64, contributing int, d *Decision) Action {
	s.mu.Lock()
	prev := s.states[tokenID]
	s.mu.Unlock()

	var next Action
	switch {
	case score >= float64(s.cfg.Block):
		next = ActionBlock
		if contributing < s.cfg.MinSignalsToBlock {
			// 신호 하나가 아무리 강해도 차단까지 가지 않는다. 보류로 멈춘다 —
			// 보류는 순번을 보존하므로 오탐이어도 되돌릴 수 있다.
			d.CappedFrom = ActionBlock
			next = ActionHold
		}
	case score >= float64(s.cfg.Hold):
		next = ActionHold
	case score >= float64(s.cfg.Greylist):
		next = ActionGreylist
	default:
		next = ActionObserve
		if prev == ActionGreylist || prev == ActionHold {
			// 점수가 내려왔다. 조치를 풀고 원 순번으로 되돌린다.
			next = ActionRestore
		}
	}

	s.mu.Lock()
	if next == ActionRestore {
		delete(s.states, tokenID)
	} else if next != ActionObserve {
		s.states[tokenID] = next
	}
	s.mu.Unlock()

	return next
}

// Forget 은 토큰의 누적 상태를 버린다(입장·차단 완료 후 정리용).
func (s *Scorer) Forget(tokenID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.scores, tokenID)
	delete(s.states, tokenID)
}

// Run 은 FlushEvery 주기로 판정을 만들어 apply 에 넘긴다.
//
// apply 가 실패해도 루프는 멈추지 않는다. 조치가 늦어질 뿐 대기열은 계속 진행돼야
// 한다 — 탐지 경로와 admit 경로는 결합되지 않는다(불변식 5).
func (s *Scorer) Run(ctx context.Context, apply func(context.Context, []Decision)) {
	ticker := time.NewTicker(s.cfg.FlushEvery)
	defer ticker.Stop()

	s.log.Info("scorer started",
		slog.Duration("window", s.cfg.Window),
		slog.Duration("flush_every", s.cfg.FlushEvery),
		slog.Int("greylist_at", s.cfg.Greylist),
		slog.Int("hold_at", s.cfg.Hold),
		slog.Int("block_at", s.cfg.Block),
		slog.Int("min_signals_to_block", s.cfg.MinSignalsToBlock),
		// 팔의 정의가 여기서 갈리는데 지금까지 어느 로그에도 찍히지 않았다.
		// 이름이 컨테이너에 닿지 않아도 아무것도 실패하지 않으므로(ROADMAP 결함 8),
		// **적용된 값**을 찍어 두어야 측정 하네스가 팔을 확인할 수 있다.
		slog.Bool("robust_baseline", s.cfg.RobustBaseline))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if decisions := s.Flush(); len(decisions) > 0 {
				apply(ctx, decisions)
			}
		}
	}
}
