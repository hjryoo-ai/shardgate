package botscore

import (
	"math"
	"slices"

	"github.com/hjr/shardgate/internal/config"
)

// Signal 은 §4-L5 의 신호 이름이다. 지표 라벨로도 쓴다.
type Signal string

// 신호 5종.
const (
	SignalHeartbeat   Signal = "heartbeat"   // 간격의 분산이 비정상적으로 작음
	SignalCorrelation Signal = "correlation" // 샤드 내 타이밍 동기화
	SignalFingerprint Signal = "fingerprint" // 동일 지문 다계정
	SignalIPPrefix    Signal = "ip_prefix"   // 같은 대역에 다수 토큰
	SignalPoW         Signal = "pow"         // 풀이 시간이 일관되게 빠름
)

// Signals 는 한 토큰의 신호별 부분 점수다. 각 값은 [0,1].
type Signals map[Signal]float64

// shardStats 는 한 샤드의 분포 요약이다. 모든 신호는 이 분포를 기준으로 판단한다 —
// "이상하다"는 절대적인 성질이 아니라 주변과 비교해서만 성립하기 때문이다.
type shardStats struct {
	tokens int

	cvMean, cvStd float64 // heartbeat 간격 변동계수의 샤드 분포
	powMean       float64
	powStd        float64

	fpGroups map[string]int // 지문 해시 → 같은 지문을 쓰는 토큰 수
	ipGroups map[string]int // IP 프리픽스 → 같은 대역의 토큰 수

	// binCounts 는 시간 버킷별 heartbeat 발생 토큰 수다. 동기화 판정의 기준.
	binCounts map[int64]int
	binTotal  int
}

// analyze 는 샤드의 관측치에서 분포를 만든다.
func analyze(obs []*observation, cfg config.BotScore) shardStats {
	st := shardStats{
		tokens:    len(obs),
		fpGroups:  make(map[string]int),
		ipGroups:  make(map[string]int),
		binCounts: make(map[int64]int),
	}

	binMS := cfg.SyncBin.Milliseconds()
	cvs := make([]float64, 0, len(obs))
	pows := make([]float64, 0, len(obs))

	for _, o := range obs {
		if o.fpHash != "" {
			st.fpGroups[o.fpHash]++
		}
		if o.ipPrefix != "" {
			st.ipGroups[o.ipPrefix]++
		}
		if cv, ok := coefficientOfVariation(o.intervals); ok {
			cvs = append(cvs, cv)
		}
		if o.hasPoW {
			pows = append(pows, o.powCost)
		}

		// 한 토큰이 같은 버킷에 여러 번 찍혀도 1 로 센다. 동기화는 "몇 명이
		// 같은 순간에 움직였나"이지 "몇 번 움직였나"가 아니다.
		seen := make(map[int64]bool, len(o.beats))
		for _, b := range o.beats {
			bin := b / binMS
			if seen[bin] {
				continue
			}
			seen[bin] = true
			st.binCounts[bin]++
			st.binTotal++
		}
	}

	st.cvMean, st.cvStd = center(cvs, cfg)
	st.powMean, st.powStd = center(pows, cfg)
	return st
}

// center 는 상대 신호의 기준선(중심, 산포)이다.
//
// 평균/표준편차는 **공격자가 표본을 밀어 넣어 옮길 수 있다.** 상대 신호는 정의상
// "이 샤드의 다른 사람들과 비교해"를 묻는데, 그 다른 사람들 중 다수가 공격자
// 소유이면 비교 자체가 공격자의 것이 된다. 중앙값/MAD 는 오염 비율이 절반을
// 넘기 전까지 버틴다(REPORT §3.12).
//
// 기본값은 평균/표준편차다 — 바꾸면 탐지 로직이 달라져 기존 측정과 같은 시스템이
// 아니게 되므로, 기본값 변경은 부하 측정 뒤에 한다.
func center(xs []float64, cfg config.BotScore) (mid, scale float64) {
	if !cfg.RobustBaseline {
		return meanStd(xs)
	}
	return medianMAD(xs)
}

// evaluate 는 한 토큰의 신호별 부분 점수를 낸다.
func evaluate(o *observation, st shardStats, cfg config.BotScore) Signals {
	s := Signals{}

	if v, ok := heartbeatSignal(o, st, cfg); ok {
		s[SignalHeartbeat] = v
	}
	if v, ok := correlationSignal(o, st, cfg); ok {
		s[SignalCorrelation] = v
	}
	if v, ok := groupSignal(st.fpGroups[o.fpHash], cfg.FPGroupCap, o.fpHash != ""); ok {
		s[SignalFingerprint] = v
	}
	if v, ok := groupSignal(st.ipGroups[o.ipPrefix], cfg.IPGroupCap, o.ipPrefix != ""); ok {
		s[SignalIPPrefix] = v
	}
	if v, ok := powSignal(o, st, cfg); ok {
		s[SignalPoW] = v
	}
	return s
}

// heartbeatSignal — 매크로는 지나치게 정확하고 사람은 지터가 있다(§4-L4).
//
// 두 가지를 함께 본다:
//   - 샤드 분포 대비 z-score: 주변보다 유독 규칙적인가
//   - 절대 하한(CVFloor): 샤드 전체가 봇이면 z-score 는 아무도 못 잡는다.
//     사람 손으로는 낼 수 없는 규칙성은 주변과 무관하게 신호가 된다.
func heartbeatSignal(o *observation, st shardStats, cfg config.BotScore) (float64, bool) {
	cv, ok := coefficientOfVariation(o.intervals)
	if !ok {
		return 0, false
	}

	absolute := 0.0
	if cv < cfg.HeartbeatCVFloor {
		absolute = 1 - cv/cfg.HeartbeatCVFloor
	}

	relative := 0.0
	if st.cvStd > 0 {
		// 낮은 쪽으로 벗어난 만큼만 본다. 지터가 큰 것은 봇의 특징이 아니다.
		relative = clamp01((st.cvMean - cv) / st.cvStd / cfg.SignalZSpan)
	}

	return math.Max(absolute, relative), true
}

// correlationSignal — 봇팜은 동기화되어 움직인다(§4-L5).
//
// 각 시간 버킷에 몇 명이 함께 찍혔는지를 보고, 우연히 겹칠 기대치를 넘는 만큼만
// 신호로 센다. 기대치를 빼지 않으면 사람이 많은 샤드는 전부 동기화된 것으로 보인다.
func correlationSignal(o *observation, st shardStats, cfg config.BotScore) (float64, bool) {
	if len(o.beats) == 0 || st.tokens < 2 || st.binTotal == 0 {
		return 0, false
	}

	// 관측된 버킷 수로 나눈 평균 동시 발생 인원이 우연 기대치다.
	expected := float64(st.binTotal) / float64(len(st.binCounts))
	binMS := cfg.SyncBin.Milliseconds()

	total, n := 0.0, 0
	seen := make(map[int64]bool, len(o.beats))
	for _, b := range o.beats {
		bin := b / binMS
		if seen[bin] {
			continue
		}
		seen[bin] = true

		together := float64(st.binCounts[bin])
		headroom := float64(st.tokens) - expected
		if headroom <= 0 {
			continue
		}
		total += clamp01((together - expected) / headroom)
		n++
	}
	if n == 0 {
		return 0, false
	}
	return total / float64(n), true
}

// groupSignal — 샤드 안에서 같은 지문/대역을 쓰는 토큰이 몇 개인가.
//
// 값이 없으면(지문을 못 만드는 브라우저 등) 신호를 내지 않는다. "모른다"를 0.5 로
// 두면 아무 근거 없이 점수가 오르고, 그 피해는 프라이버시를 챙기는 사람이 본다.
func groupSignal(group, cap int, present bool) (float64, bool) {
	if !present || group <= 1 || cap <= 1 {
		return 0, present && group > 0
	}
	return clamp01(float64(group-1) / float64(cap-1)), true
}

// powSignal — GPU 팜은 일관되게 빠르다(§4-L5).
// 난이도로 정규화한 풀이 비용이 샤드 분포에서 빠른 쪽 이상치인지 본다.
func powSignal(o *observation, st shardStats, cfg config.BotScore) (float64, bool) {
	if !o.hasPoW || st.powStd <= 0 {
		return 0, false
	}
	return clamp01((st.powMean - o.powCost) / st.powStd / cfg.SignalZSpan), true
}

// coefficientOfVariation 은 표준편차/평균이다. 간격의 절대 크기와 무관하게
// "얼마나 들쭉날쭉한가"만 남기려고 정규화한다.
func coefficientOfVariation(xs []float64) (float64, bool) {
	if len(xs) < 2 {
		return 0, false
	}
	mean, std := meanStd(xs)
	if mean <= 0 {
		return 0, false
	}
	return std / mean, true
}

// medianMAD 는 중앙값과 정규화한 중앙값 절대편차다.
//
// 1.4826 을 곱하는 이유는 정규분포에서 MAD × 1.4826 이 표준편차와 같아지기
// 때문이다. 그래야 `SignalZSpan`(z-score 3 이면 신호 1.0)의 의미가 두 추정
// 방식에서 같게 유지된다 — 추정기를 바꾸면서 임계의 뜻까지 바꾸면 무엇이
// 달라졌는지 알 수 없다.
//
// 산포가 0 이면(전원이 같은 값) 상대 신호는 나오지 않는다. 평균/표준편차 쪽도
// 같으므로 동작이 갈리지 않는다 — 그 경우 판단은 절대 하한(CVFloor)이 맡는다.
func medianMAD(xs []float64) (mid, scale float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	mid = medianOf(append([]float64(nil), xs...))
	if len(xs) < 2 {
		return mid, 0
	}
	dev := make([]float64, len(xs))
	for i, x := range xs {
		dev[i] = math.Abs(x - mid)
	}
	return mid, 1.4826 * medianOf(dev)
}

// medianOf 는 입력 슬라이스를 정렬하며 중앙값을 낸다(호출자가 사본을 준다).
func medianOf(xs []float64) float64 {
	slices.Sort(xs)
	n := len(xs)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

func meanStd(xs []float64) (mean, std float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))

	if len(xs) < 2 {
		return mean, 0
	}
	for _, x := range xs {
		d := x - mean
		std += d * d
	}
	return mean, math.Sqrt(std / float64(len(xs)-1))
}

func clamp01(v float64) float64 {
	if v < 0 || math.IsNaN(v) {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
