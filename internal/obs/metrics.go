package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics 는 ShardGate 전 서비스가 공유하는 지표 집합이다.
// §11 검증 계획(탐지율/FPR/처리량/지연)이 여기서 나온 시계열로 계산된다.
type Metrics struct {
	reg *prometheus.Registry

	// HTTP
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight prometheus.Gauge

	// 큐
	Enqueued  *prometheus.CounterVec
	QueueSize *prometheus.GaugeVec
	Evicted   *prometheus.CounterVec

	// 입장 제어
	AdmitAttempts *prometheus.CounterVec // result=admitted|not_yet|held|blocked|...
	AdmitBudget   *prometheus.GaugeVec
	AdmitRate     prometheus.Gauge // 백프레셔 반영 후 실효 admit rate
	Redeemed      *prometheus.CounterVec
	WaitSeconds   *prometheus.HistogramVec // 진입~입장 지연 (P99 측정용)

	// 챌린지
	ChallengeIssued   *prometheus.CounterVec
	ChallengeVerified *prometheus.CounterVec
	PoWDifficulty     *prometheus.HistogramVec
	PoWSolveSeconds   *prometheus.HistogramVec
	// Rechallenge 는 greylist 복귀 시도의 결과다(§4).
	// outcome=issued|restored|exhausted|noop|no_rank|unknown|failed
	Rechallenge *prometheus.CounterVec

	// 토큰
	TokenRejected *prometheus.CounterVec // reason=expired|fp_mismatch|ip_mismatch|reused|...

	// 텔레메트리 / 스코어러
	TelemetryEvents  *prometheus.CounterVec
	TelemetryErrors  *prometheus.CounterVec
	ScorerLagSeconds prometheus.Gauge
	ScoreValue       *prometheus.HistogramVec
	Actions          *prometheus.CounterVec // action=observe|greylist|hold|block|restore
	SignalScore      *prometheus.HistogramVec

	// 주문(mock shop)
	Orders *prometheus.CounterVec
}

var (
	// 서비스마다 라벨 카디널리티를 관리하기 위해 샤드 라벨은 꼭 필요한 지표에만 붙인다.
	durationBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	waitBuckets     = []float64{1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600}
	scoreBuckets    = []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	solveBuckets    = []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30}
)

// NewMetrics 는 지표를 등록한 새 레지스트리를 만든다.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"service": service}
	f := promauto{reg: reg, labels: labels}

	m := &Metrics{
		reg: reg,

		HTTPRequests: f.counterVec("http_requests_total",
			"HTTP 요청 수", "route", "method", "code"),
		HTTPDuration: f.histogramVec("http_request_duration_seconds",
			"HTTP 처리 지연", durationBuckets, "route", "method"),
		HTTPInFlight: f.gauge("http_in_flight_requests", "처리 중인 HTTP 요청 수"),

		Enqueued: f.counterVec("queue_enqueued_total",
			"대기열 진입 수", "event", "segment"),
		QueueSize: f.gaugeVec("queue_size", "샤드별 대기 인원", "event", "shard"),
		Evicted:   f.counterVec("queue_evicted_total", "heartbeat 미수신 제거 수", "event", "reason"),

		AdmitAttempts: f.counterVec("admission_attempts_total",
			"admit 시도 결과", "event", "result"),
		AdmitBudget: f.gaugeVec("admission_budget", "샤드별 잔여 입장 예산", "event", "shard"),
		AdmitRate:   f.gauge("admission_effective_rate_per_min", "백프레셔 반영 실효 admit rate"),
		Redeemed:    f.counterVec("admission_redeem_total", "입장 토큰 교환 결과", "event", "result"),
		WaitSeconds: f.histogramVec("admission_wait_seconds", "진입~입장 대기 시간", waitBuckets, "event"),

		ChallengeIssued: f.counterVec("challenge_issued_total", "PoW 챌린지 발급 수", "event"),
		ChallengeVerified: f.counterVec("challenge_verified_total",
			"PoW 챌린지 검증 결과", "event", "result"),
		PoWDifficulty:   f.histogramVec("challenge_difficulty_bits", "발급된 PoW 난이도", prometheus.LinearBuckets(8, 2, 12), "event"),
		PoWSolveSeconds: f.histogramVec("challenge_solve_seconds", "PoW 풀이 시간", solveBuckets, "event"),
		Rechallenge:     f.counterVec("rechallenge_total", "greylist 재검증 결과", "event", "outcome"),

		TokenRejected: f.counterVec("token_rejected_total", "토큰 거절 사유", "kind", "reason"),

		TelemetryEvents:  f.counterVec("telemetry_events_total", "수집/소비된 텔레메트리 이벤트", "type", "direction"),
		TelemetryErrors:  f.counterVec("telemetry_errors_total", "텔레메트리 파이프라인 오류", "stage"),
		ScorerLagSeconds: f.gauge("scorer_lag_seconds", "이벤트 발생 시각 대비 스코어러 처리 지연"),
		ScoreValue:       f.histogramVec("botscore_value", "산출된 봇 점수 분포", scoreBuckets, "event"),
		Actions:          f.counterVec("botscore_actions_total", "조치 파이프라인 실행 수", "event", "action"),
		SignalScore:      f.histogramVec("botscore_signal", "신호별 부분 점수", scoreBuckets, "signal"),

		Orders: f.counterVec("orders_total", "mock shop 주문 결과", "result"),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Registry 는 내부 레지스트리를 노출한다(테스트/커스텀 컬렉터 등록용).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler 는 /internal/metrics 핸들러를 반환한다.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// promauto 는 공통 라벨을 자동으로 붙여 등록하는 작은 헬퍼다.
type promauto struct {
	reg    *prometheus.Registry
	labels prometheus.Labels
}

const namespace = "shardgate"

func (p promauto) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: name, Help: help, ConstLabels: p.labels,
	}, labels)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: name, Help: help, ConstLabels: p.labels,
	}, labels)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: name, Help: help, ConstLabels: p.labels,
	})
	p.reg.MustRegister(g)
	return g
}

func (p promauto) histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: name, Help: help, Buckets: buckets, ConstLabels: p.labels,
	}, labels)
	p.reg.MustRegister(h)
	return h
}
