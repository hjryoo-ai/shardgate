package botscore

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/telemetry"
)

// 기준선 오염 — 의심스럽게 보이는 정상 사용자가 많으면 봇의 상대 신호가 내려간다.
//
// REPORT §3.10 에서 실제로 일어난 일이다: clumsy 100명을 넣자 farm-b(분산 IP 봇)의
// 점수가 42.2 → 38.1 로 내려가 임계(40) 아래에 머물렀고, 탐지율이 97.7 → 85.3% 가
// 됐다. 부하 측정은 **결과**만 보여 주므로(총점), 어느 신호가 얼마나 내려갔는지는
// 여기서 가른다 — 스코어러는 Redis 없이 돌아가므로 신호별로 볼 수 있다.
//
// 왜 신호를 갈라야 하는가: 수리 방향이 신호마다 다르다. 샤드 분포의 평균·표준편차를
// 쓰는 신호(heartbeat 상대항·PoW)는 강건 추정(중앙값/MAD)으로 고칠 수 있지만,
// 그룹 카운트 신호(지문·대역)는 애초에 샤드 분포를 쓰지 않아 오염되지 않는다.
// **어디가 오염되는지 모른 채 고치면 오염되지 않은 곳을 고치게 된다.**

const (
	simBeatsPerWindow = 12
	simIntervalMS     = 5000
)

// cohort 는 한 코호트의 행동 모델이다. loadtest/k6/lib/personas.js 와 같은 모양이다.
type cohort struct {
	name    string
	n       int
	fp      func(i int) string
	ip      func(i int) string
	jitter  int64 // heartbeat 간격 지터 ±ms. 0 이면 시계처럼 정확하다.
	solveMS int64 // PoW 풀이 시간. 봇은 짧다.
}

// feed 는 한 코호트를 창 하나 분량 관측시킨다. 지터는 결정적이다 —
// 실행마다 흔들리면 두 팔의 차이가 신호인지 잡음인지 알 수 없다.
//
// noise 는 **서버 관측 잡음**이다(네트워크 + 스케줄링). 클라이언트 지터와 나눠
// 두는 이유는 둘이 서로 다른 곳에 들어가기 때문이다: `IntervalMS` 는 서버가 재는
// 값이므로(`internal/telemetry/event.go`), 지터가 0 인 봇도 서버에서는 분산이
// 0 이 아니다. 그리고 그 차이가 결정적이다 — CV 가 절대 하한(0.02)을 넘는
// 순간 heartbeat 신호에서 **절대항이 죽고 상대항만 남아** 기준선 오염에 노출된다.
// 난수는 고정 시드의 PCG 다. 초판은 지터를 `seed*(i*31+k*7+1) % 2R - R` 로 만들었는데
// 그건 난수가 아니라 **등차수열**이라 사람들의 CV 가 거의 같은 값에 몰렸다.
// 중앙값/MAD 는 정확히 그 폭을 재는 통계라, 그 상태에서 두 추정기를 비교하면
// 모형의 인공물을 재게 된다.
func (c cohort) feed(s *Scorer, shard string, seed int64, noise int64) {
	rng := rand.New(rand.NewPCG(uint64(seed), 0x5EED))
	for i := range c.n {
		token := fmt.Sprintf("%s-%d", c.name, i)
		// PoW 는 진입 때 한 번이다. **관측 순서대로** 넣어야 한다 —
		// `observation.lastSeen` 은 도착 순서로 갱신되고, 창 만료가 그 값을 보므로
		// 나중에 넣으면 전 토큰이 창 밖으로 밀려 판정이 0건이 된다.
		s.Observe(telemetry.Event{
			Kind: telemetry.KindChallenge, EventID: "evt1", Shard: shard, TokenID: token,
			At: base.Add(time.Second), SolveMS: c.solveMS, Difficulty: 8,
			FPHash: c.fp(i), IPPrefix: c.ip(i),
		})

		at, prev := base, base
		for range simBeatsPerWindow {
			d := int64(simIntervalMS)
			if c.jitter > 0 {
				d += rng.Int64N(2*c.jitter) - c.jitter
			}
			// 클라이언트는 응답을 받은 뒤 자므로 지연이 다음 주기로 누적된다(k6 와 같다).
			at = at.Add(time.Duration(d) * time.Millisecond)
			obsAt := at
			if noise > 0 {
				obsAt = at.Add(time.Duration(rng.Int64N(2*noise)-noise) * time.Millisecond)
			}
			s.Observe(beat(shard, token, obsAt, obsAt.Sub(prev).Milliseconds(), c.fp(i), c.ip(i)))
			prev = obsAt
		}
	}
}

func fixed(v string) func(int) string   { return func(int) string { return v } }
func perUser(f string) func(int) string { return func(i int) string { return fmt.Sprintf(f, i) } }
func spread(f string, m int) func(int) string {
	return func(i int) string { return fmt.Sprintf(f, i%m) }
}

// 한 샤드의 인구. §3.10 의 실행(1,000명 / 2샤드)을 샤드 하나로 축소한 것이다.
//
// humans 를 줄이는 것은 **입장을 모사한다.** 입장한 클라이언트는 heartbeat 을
// 멈추므로 채점 모집단에서 사라지고, 사라지는 것은 언제나 가장 덜 의심스러운
// 쪽이다 — 봇은 입장하지 못하고 남는다.
func cohorts(humans, clumsy int) []cohort {
	return []cohort{
		{name: "human", n: humans, fp: perUser("fp-human-%d"), ip: spread("198.51.%d.0/24", 200),
			jitter: 1250, solveMS: 900},
		// clumsy — 사람인데 지문·대역·규칙성이 겹친다(REPORT §3.10).
		{name: "clumsy", n: clumsy, fp: fixed("fp-corp-golden-image"), ip: fixed("198.51.9.0/24"),
			jitter: 250, solveMS: 900},
		{name: "farm-a", n: 100, fp: fixed("fp-farm-image-a"), ip: fixed("203.0.113.0/24"),
			jitter: 0, solveMS: 40},
		{name: "farm-b", n: 50, fp: fixed("fp-farm-image-b"), ip: spread("192.0.%d.0/24", 50),
			jitter: 0, solveMS: 40},
	}
}

// plainConfig 는 기준선을 평균/표준편차로 잡는 설정이다.
//
// **기본값에 기대지 않고 명시한다.** 이 파일의 실험들은 "어느 추정기가 어떻게
// 움직이는가"를 재는 것이므로, 추정기를 기본값에서 받아 오면 기본값이 바뀌는 순간
// 테스트의 **뜻**이 조용히 바뀐다. 실제로 §3.13·§3.14 측정 뒤 기본값을 켜면서
// 여기가 한 번 깨졌고, 그때 깨진 것이 다행이었다 — 안 깨졌으면 두 팔을 비교한다고
// 믿으면서 같은 팔을 두 번 재고 있었을 것이다.
func plainConfig() config.BotScore {
	cfg := testConfig()
	cfg.RobustBaseline = false
	return cfg
}

func robustConfig() config.BotScore {
	cfg := testConfig()
	cfg.RobustBaseline = true
	return cfg
}

// run 은 한 팔을 돌리고 코호트별 (신호 평균, 점수 평균) 을 돌려준다.
func run(t *testing.T, humans, clumsy int, noise int64) (map[string]Signals, map[string]float64) {
	t.Helper()
	s := NewScorer("evt1", plainConfig(), nil, nil).
		WithClock(func() time.Time { return judgeAt })

	cs := cohorts(humans, clumsy)
	for i, c := range cs {
		c.feed(s, "s0001", int64(i*97+13), noise)
	}

	byToken := map[string]string{}
	for _, c := range cs {
		for i := range c.n {
			byToken[fmt.Sprintf("%s-%d", c.name, i)] = c.name
		}
	}

	sigs := map[string]Signals{}
	scores := map[string]float64{}
	counts := map[string]int{}
	decisions := s.Flush()
	if len(decisions) == 0 {
		t.Fatal("판정이 하나도 나오지 않았다 — 창 설정이 어긋나 테스트가 공허하다")
	}
	for _, d := range decisions {
		name := byToken[d.TokenID]
		if sigs[name] == nil {
			sigs[name] = Signals{}
		}
		for k, v := range d.Signals {
			sigs[name][k] += v
		}
		scores[name] += d.Score
		counts[name]++
	}
	for name, n := range counts {
		for k := range sigs[name] {
			sigs[name][k] /= float64(n)
		}
		scores[name] /= float64(n)
	}
	return sigs, scores
}

// TestPollutionNeedsTheAbsoluteFloorGone — **오염이 통하는 조건을 고정한다.**
//
// REPORT §3.10 초판은 "clumsy 100명이 **지문·대역** 신호의 기준선을 올렸다"고
// 적었다. 그 부분은 틀렸다 — `groupSignal` 은 샤드 분포를 쓰지 않고 고정 상한만
// 쓰므로 오염될 기준선이 없다. 오염될 수 있는 것은 z-score 를 쓰는 쪽,
// 즉 heartbeat 의 **상대항**과 PoW 다.
//
// 그런데 상대항은 평소에 잠자고 있다. `heartbeatSignal` 은 절대항과 상대항의
// **최댓값**이고, 지터 0 인 봇은 절대 하한(CVFloor)에서 이미 1.0 이라 기준선을
// 아무리 흔들어도 값이 변하지 않는다. 오염이 통하려면 먼저 **절대항이 죽어야**
// 하고, 그것을 죽이는 것이 서버 관측 잡음이다(부하가 오르면 같이 오른다).
//
//	잡음 0~50ms  → 봇 CV < 0.02 → 절대항 1.0 → 구성 변화가 신호를 못 건드린다
//	잡음 100ms   → 봇 CV > 0.02 → 상대항만 남는다 → 구성 변화가 신호를 끌어내린다
//
// **즉 부하가 오르면 탐지기가 오염에 열린다.** 방어의 실체는 강건 통계 이전에
// 절대 하한이고, 그 하한을 지키는 것은 관측 잡음을 낮게 유지하는 일이다.
func TestPollutionNeedsTheAbsoluteFloorGone(t *testing.T) {
	order := []Signal{SignalHeartbeat, SignalCorrelation, SignalFingerprint, SignalIPPrefix, SignalPoW}

	// 서버 관측 잡음을 축으로 둔다 — 오염이 성립하는 조건 자체를 이 축이 정한다.
	delta := map[int64]float64{}
	for _, noise := range []int64{0, 25, 50, 100} {
		t.Run(fmt.Sprintf("noise%dms", noise), func(t *testing.T) {
			clean, cleanScore := run(t, 350, 0, noise)
			dirty, dirtyScore := run(t, 300, 50, noise)
			delta[noise] = dirty["farm-b"][SignalHeartbeat] - clean["farm-b"][SignalHeartbeat]

			t.Logf("%-8s %-12s %8s %8s %8s", "코호트", "신호", "clumsy0", "clumsy50", "차이")
			for _, name := range []string{"human", "farm-a", "farm-b", "clumsy"} {
				for _, sig := range order {
					a, b := clean[name][sig], dirty[name][sig]
					t.Logf("%-8s %-12s %8.3f %8.3f %+8.3f", name, sig, a, b, b-a)

					// 지문·대역은 어느 잡음 수준에서도 구성 변화에 흔들리지 않아야 한다.
					// 흔들린다면 `groupSignal` 이 샤드 분포를 보게 바뀐 것이다.
					if (name == "farm-a" || name == "farm-b") &&
						(sig == SignalFingerprint || sig == SignalIPPrefix) && b < a-0.01 {
						t.Errorf("%s %s: 구성 변화만으로 %.3f → %.3f — 그룹 신호에는 "+
							"오염될 기준선이 없어야 한다", name, sig, a, b)
					}
				}
				t.Logf("%-8s %-12s %8.1f %8.1f %+8.1f", name, "점수",
					cleanScore[name], dirtyScore[name], dirtyScore[name]-cleanScore[name])
			}
		})
	}

	t.Logf("잡음별 farm-b 규칙성 신호 변화(구성 변화만): 0ms %+.3f · 25ms %+.3f · 50ms %+.3f · 100ms %+.3f",
		delta[0], delta[25], delta[50], delta[100])

	// 절대 하한이 살아 있는 동안에는 오염이 신호에 닿지 못한다.
	if delta[0] < -0.01 {
		t.Errorf("잡음 0 에서 구성 변화가 신호를 %.3f 만큼 내렸다 — 절대 하한(CVFloor)이 "+
			"제 역할을 하면 여기서는 아무 일도 일어나지 않아야 한다", delta[0])
	}
	// 하한이 무력화되면 통한다. 이것이 §3.10 이 본 방향의 기전이다.
	if delta[100] >= 0 {
		t.Errorf("잡음 100ms 에서도 구성 변화가 신호를 내리지 않았다 (%+.3f) — "+
			"REPORT §3.12 의 기전 설명을 고쳐야 한다", delta[100])
	}
}

// TestAdmissionSharpensRelativeSignals — **입장 자체가 탐지를 날카롭게 한다.**
//
// 상대 신호는 "남아 있는 사람들과 비교해" 이상한가를 묻는다. 그런데 입장은
// 모집단에서 **가장 덜 의심스러운 쪽만** 골라 내보낸다(입장한 클라이언트는
// heartbeat 을 멈춘다). 남는 인구가 줄고 봇 쪽으로 기울수록 같은 행동의 상대
// 신호가 커진다.
//
// REPORT §3.8 의 궤적에서 farm-b 점수가 t≈200(=`ADMIT_MIN_DWELL` 이 열리는 시각)에
// 38 → 42 로 튀어 격리가 시작되는데, 이 성질이 그 도약의 **후보 설명**이다.
//
// **여기서 확인되는 것과 확인되지 않는 것을 나눠 둔다.** 확인되는 것은 방향뿐이다 —
// 인구가 줄면 상관 신호가 커진다. 확인되지 않는 것은 크기다: 이 모형에서는 인구가
// 줄 때 PoW 신호가 반대로 움직여 총점이 거의 상쇄되고, §3.10 두 팔의 실제 이탈
// 속도 차이도 8% 뿐이었다. 그래서 §3.10 의 탐지율 하락을 이것으로 **설명하지
// 않는다.** 방향이 맞는 것과 원인인 것은 다르다.
func TestAdmissionSharpensRelativeSignals(t *testing.T) {
	order := []Signal{SignalHeartbeat, SignalCorrelation, SignalFingerprint, SignalIPPrefix, SignalPoW}
	cases := []struct {
		name           string
		humans, clumsy int
	}{
		{"입장 전", 350, 0},
		{"입장 후", 100, 0},
		{"입장 후+갇힌50", 100, 50},
	}

	t.Logf("%-14s %-10s %8s %8s %8s %8s %8s %8s", "국면", "코호트",
		"hb", "corr", "fp", "ip", "pow", "점수")
	corr := map[string]float64{}
	for _, c := range cases {
		sig, score := run(t, c.humans, c.clumsy, 25)
		for _, name := range []string{"farm-a", "farm-b"} {
			vals := ""
			for _, s := range order {
				vals += fmt.Sprintf(" %8.3f", sig[name][s])
			}
			t.Logf("%-14s %-10s%s %8.1f", c.name, name, vals, score[name])
		}
		corr[c.name] = sig["farm-b"][SignalCorrelation]
	}

	if corr["입장 후"] <= corr["입장 전"] {
		t.Errorf("인구가 줄었는데 상관 신호가 커지지 않았다 (%.3f → %.3f) — "+
			"상대 신호가 모집단에 의존한다는 성질이 깨졌다", corr["입장 전"], corr["입장 후"])
	}
}

// TestBaselineFloodHidesStealthBots — **기준선 오염 공격이 실재하는지 직접 만들어 본다.**
//
// §3.10 의 관찰을 설명하려던 기전(구성 변화 → 기준선 이동)은 위에서 반증됐다.
// 그렇다고 그 기전이 **불가능**하다는 뜻은 아니다 — 거기서는 우연히 겹친 집단이
// 봇과 다른 규칙성 대역에 있었을 뿐이다. 그래서 조건을 갖춰 일부러 만들어 본다:
//
//	공격자는 자기 봇과 **같은 규칙성 대역**의 계정을 다수 밀어 넣는다.
//	heartbeat 상대항은 z-score 이므로(평균·표준편차) 그 평균이 봇 쪽으로 끌려오면
//	봇이 더 이상 특이하지 않게 된다.
//
// 표적은 지터를 적당히 넣은 "은신형" 봇이다. 지터 0 인 봇은 절대 하한(CVFloor)이
// 신호를 고정해서 기준선과 무관하고, 사람만큼 흔드는 봇은 애초에 규칙성 신호가
// 없다. **공격이 통하는 구간은 그 사이뿐이고**, 그 구간이 존재하는지를 여기서 본다.
//
// 지문·대역은 전부 서로 다르게 준다. 그래야 움직이는 것이 heartbeat 하나로 좁혀진다.
func TestBaselineFloodHidesStealthBots(t *testing.T) {
	stealth := cohort{name: "stealth", n: 50, fp: perUser("fp-stealth-%d"),
		ip: spread("203.0.%d.0/24", 50), jitter: 250, solveMS: 40}

	// 표적 봇의 규칙성 신호를 flood 규모별로 잰다.
	measure := func(cfg config.BotScore, flood int) float64 {
		s := NewScorer("evt1", cfg, nil, nil).
			WithClock(func() time.Time { return judgeAt })

		// 정상 인구(지터 큼) + 표적 봇 + 공격자가 밀어 넣은 같은 대역의 계정.
		cohort{name: "human", n: 350, fp: perUser("fp-human-%d"),
			ip: spread("198.51.%d.0/24", 200), jitter: 1250, solveMS: 900}.feed(s, "s0001", 13, 25)
		stealth.feed(s, "s0001", 211, 25)
		cohort{name: "flood", n: flood, fp: perUser("fp-flood-%d"),
			ip: spread("192.0.%d.0/24", 250), jitter: 250, solveMS: 900}.feed(s, "s0001", 307, 25)

		var hb float64
		var n int
		for _, d := range s.Flush() {
			if !strings.HasPrefix(d.TokenID, "stealth-") {
				continue
			}
			hb += d.Signals[SignalHeartbeat]
			n++
		}
		if n == 0 {
			t.Fatal("표적 봇이 판정되지 않았다 — 테스트가 공허하다")
		}
		return hb / float64(n)
	}

	plain, robust := plainConfig(), robustConfig()

	// 정상 인구 350명 + 표적 50명이므로 flood 300 이면 저지터 대역이 정확히 절반이다 —
	// **중앙값/MAD 의 붕괴점(50%)이 거기다.** 그 앞뒤를 같이 재야 손잡이의 성질이 보인다.
	floods := []int{0, 50, 150, 300}
	plainAt := map[int]float64{}
	robustAt := map[int]float64{}
	t.Logf("%-8s %12s %12s   (표적 봇의 규칙성 신호)", "flood", "평균/표준편차", "중앙값/MAD")
	for _, flood := range floods {
		plainAt[flood], robustAt[flood] = measure(plain, flood), measure(robust, flood)
		t.Logf("%-8d %12.3f %12.3f", flood, plainAt[flood], robustAt[flood])
	}

	// (1) 공격이 실재하는가 — 평균 기준선은 밀어낼 수 있다.
	if plainAt[300] >= plainAt[0] {
		t.Errorf("flood 300 에서 신호가 %.3f → %.3f 로 줄지 않았다 — "+
			"기준선 오염이 성립하지 않는다면 REPORT §3.12 를 고쳐야 한다",
			plainAt[0], plainAt[300])
	}
	// (2) 붕괴점 아래에서는 강건 추정이 덜 밀리는가. 이것이 손잡이의 존재 이유다.
	if robustAt[150] <= plainAt[150] {
		t.Errorf("오염 30%%(flood 150)에서 강건 추정이 낫지 않았다 (평균 %.3f vs 중앙값 %.3f) — "+
			"SCORE_ROBUST_BASELINE 을 둘 이유가 없다", plainAt[150], robustAt[150])
	}
	// (3) 붕괴점에서는 오히려 더 나쁘다. 이것은 결함이 아니라 MAD 의 성질이고,
	//     손잡이를 켤 때 알고 있어야 하는 대가다 — 그래서 기록으로 남긴다.
	t.Logf("붕괴점(오염 50%%, flood 300): 평균 %.3f vs 중앙값 %.3f — "+
		"강건 추정은 그 지점을 넘으면 통째로 넘어간다", plainAt[300], robustAt[300])
}
