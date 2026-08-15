// Package botscore 는 샤드를 표본 집단으로 삼아 봇을 찾아내고, 찾아낸 것에
// 단계적 조치를 적용한다(DESIGN.md §4-L5).
//
// # 왜 샤드 단위인가
//
// 전체 모수(수십만) 대상 이상탐지는 비싸고, 큰 모수 안에서 봇은 희석된다.
// 샤드(≈1,000명)는 계산이 싸고, 봇 클러스터가 통계적으로 도드라진다. 여기 있는
// 신호는 전부 "이 사용자가 이상한가"가 아니라 **"이 사용자가 자기 샤드의 다른
// 사람들과 비교해 이상한가"**를 묻는다.
//
// # 무엇을 하지 않는가
//
// 즉시 차단하지 않는다. 어떤 단일 신호도 차단의 근거가 되지 못하도록 값으로
// 강제하고(MinSignalsToBlock), 점수는 창(window)마다 지수평활로 천천히 쌓인다.
// 되돌릴 수 있는 조치(greylist·hold)는 순번을 보존한 채 적용하고, 되돌릴 수 없는
// 조치(block)만 여러 신호의 합의를 요구한다. 오탐은 반드시 생기고, 생겼을 때
// 사용자가 잃는 것이 없어야 의심을 넓게 볼 수 있기 때문이다.
package botscore

import (
	"sync"
	"time"

	"github.com/hjr/shardgate/internal/telemetry"
)

// maxSamplesPerToken 은 한 토큰이 창 안에 들고 있는 관측치 상한이다.
// 창이 길어져도 메모리가 인원수에 선형으로만 늘어나도록 묶어 둔다.
const maxSamplesPerToken = 64

// observation 은 한 토큰의 창 안 관측치다.
type observation struct {
	tokenID  string
	fpHash   string
	ipPrefix string

	// intervals 는 heartbeat 간격(ms)이다. 규칙성 신호의 입력.
	intervals []float64
	// beats 는 heartbeat 수신 시각(ms)이다. 상호상관 신호의 입력.
	beats []int64

	// powCost 는 난이도로 정규화한 PoW 풀이 시간이다(ms / 기대 시도 횟수).
	// 난이도가 사람마다 다르므로 원시 시간을 그대로 비교하면 의미가 없다.
	powCost float64
	hasPoW  bool

	lastSeen time.Time
}

func (o *observation) add(e telemetry.Event) {
	o.lastSeen = e.At
	if e.FPHash != "" {
		o.fpHash = e.FPHash
	}
	if e.IPPrefix != "" {
		o.ipPrefix = e.IPPrefix
	}

	switch e.Kind {
	case telemetry.KindHeartbeat:
		if e.IntervalMS > 0 {
			o.intervals = appendCapped(o.intervals, float64(e.IntervalMS))
		}
		o.beats = appendCapped(o.beats, e.At.UnixMilli())
	case telemetry.KindChallenge:
		if e.SolveMS > 0 && e.Difficulty > 0 {
			// 기대 시도 횟수는 2^difficulty. 난이도 차이를 걷어내야 "GPU 팜은
			// 일관되게 빠르다"를 같은 축에서 비교할 수 있다.
			o.powCost = float64(e.SolveMS) / exp2(e.Difficulty)
			o.hasPoW = true
		}
	case telemetry.KindEnter, telemetry.KindAdmit:
		// 지문·IP 표본으로만 쓴다(위에서 이미 반영).
	case telemetry.KindRechallenge:
		// 관측치가 아니라 통지다. 창에 들어오기 전에 Scorer.Observe 가 걷어낸다.
	}
}

// samples 는 이 토큰이 얼마나 관측됐는지다. MinSamples 미만이면 점수를 매기지 않는다 —
// 표본이 적을 때의 판정은 사람을 봇으로 만드는 가장 흔한 경로다.
func (o *observation) samples() int { return len(o.beats) + len(o.intervals) }

func appendCapped[T any](s []T, v T) []T {
	if len(s) >= maxSamplesPerToken {
		copy(s, s[1:])
		s[len(s)-1] = v
		return s
	}
	return append(s, v)
}

func exp2(n int) float64 {
	v := 1.0
	for range n {
		v *= 2
	}
	return v
}

// shardWindow 는 한 샤드의 관측 창이다.
type shardWindow struct {
	tokens map[string]*observation
}

func newShardWindow() *shardWindow {
	return &shardWindow{tokens: make(map[string]*observation)}
}

func (w *shardWindow) observe(e telemetry.Event) {
	o, ok := w.tokens[e.TokenID]
	if !ok {
		o = &observation{tokenID: e.TokenID}
		w.tokens[e.TokenID] = o
	}
	o.add(e)
}

// expire 는 창 밖으로 나간 토큰을 버린다.
func (w *shardWindow) expire(before time.Time) {
	for id, o := range w.tokens {
		if o.lastSeen.Before(before) {
			delete(w.tokens, id)
		}
	}
}

// windows 는 샤드별 창을 들고 있다.
//
// 스코어러는 파티션 키 = shard_id 로 배분된 이벤트만 받으므로, 한 프로세스가
// 들고 있는 샤드 수는 담당 파티션 수만큼이다. 이벤트 전체 모수를 들 필요가 없다는
// 것이 §4-L5 가 "저렴하다"고 말하는 이유다.
type windows struct {
	mu sync.Mutex
	m  map[string]*shardWindow
}

func newWindows() *windows { return &windows{m: make(map[string]*shardWindow)} }

func (ws *windows) observe(e telemetry.Event) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.m[e.Shard]
	if !ok {
		w = newShardWindow()
		ws.m[e.Shard] = w
	}
	w.observe(e)
}

// snapshot 은 창을 정리하고 샤드별 관측치를 복사해 돌려준다.
// 잠금을 짧게 유지해 수집 경로가 점수 계산에 막히지 않게 한다.
func (ws *windows) snapshot(before time.Time) map[string][]*observation {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	out := make(map[string][]*observation, len(ws.m))
	for shard, w := range ws.m {
		w.expire(before)
		if len(w.tokens) == 0 {
			delete(ws.m, shard)
			continue
		}
		list := make([]*observation, 0, len(w.tokens))
		for _, o := range w.tokens {
			cp := *o
			cp.intervals = append([]float64(nil), o.intervals...)
			cp.beats = append([]int64(nil), o.beats...)
			list = append(list, &cp)
		}
		out[shard] = list
	}
	return out
}
