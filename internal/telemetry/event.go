// Package telemetry 는 행동 신호를 수집해 Kafka 로 흘려보낸다(DESIGN.md §4-L4).
//
// 여기 흐르는 값은 전부 **신호**이지 판단이 아니다. 클라이언트가 보내는 것이므로
// 위조할 수 있고, 접근성 도구나 저사양 기기에서는 봇과 비슷한 모양이 나오기도 한다.
// 그래서 어떤 단일 값도 차단의 근거가 되지 않는다 — 판단은 스코어러가 샤드 단위
// 분포와 비교해서 하고, 조치는 누적 점수 파이프라인이 한다(불변식 3).
//
// # 이 경로는 대기열을 멈출 수 없다
//
// Kafka 가 죽거나 느려져도 heartbeat 응답이 늦어지거나 실패해서는 안 된다.
// 탐지가 늦어질 뿐 대기열은 진행돼야 한다(불변식 5). 그래서 발행은 버퍼를 거친
// 비동기이고, 버퍼가 차면 **이벤트를 버린다**. 버리는 쪽이 사용자를 세우는 것보다 낫다.
package telemetry

import "time"

// Kind 는 신호의 종류다.
type Kind string

// 수집하는 신호(§4-L4·L5).
const (
	// KindHeartbeat 는 생존 신호다. 간격의 규칙성이 핵심 신호가 된다.
	KindHeartbeat Kind = "heartbeat"
	// KindEnter 는 대기열 진입이다. 지문·IP 분포의 기준 표본이 된다.
	KindEnter Kind = "enter"
	// KindChallenge 는 PoW 풀이 완료다. 풀이 시간 분포가 신호가 된다.
	KindChallenge Kind = "challenge"
	// KindAdmit 은 입장이다. 진입~입장 지연 측정과 사후 분석에 쓴다.
	KindAdmit Kind = "admit"
	// KindRechallenge 는 greylist 사용자가 재챌린지를 통과했다는 사실이다(§4).
	//
	// 다른 Kind 와 달리 이건 관측치가 아니라 **스코어러에게 보내는 통지**다.
	// 누적 점수의 진실은 스코어러의 메모리에 있고 Redis 의 score 는 그 사본이므로,
	// 복귀 시 점수 클램프가 스코어러에 닿지 않으면 다음 창에서 곧바로 재격리된다.
	// 게이트가 Redis 를 거쳐 스코어러 상태를 직접 고치면 탐지 경로와 admit 경로가
	// 결합되므로(불변식 5), 이미 있는 Kafka 경로로 흘려보낸다 — 파티션 키가
	// 원 샤드라 그 토큰을 들고 있는 바로 그 컨슈머에게 간다.
	KindRechallenge Kind = "rechallenge"
)

// Event 는 Kafka 로 흘려보내는 신호 한 건이다.
//
// 개인정보는 해시·프리픽스만 담는다. 원본 지문과 전체 IP 는 이 구조체에 들어올
// 자리가 없다(불변식 6) — 스코어러도 PG 도 그 값을 볼 일이 없기 때문이다.
type Event struct {
	Kind    Kind   `json:"kind"`
	EventID string `json:"event_id"`
	Shard   string `json:"shard"`
	TokenID string `json:"token_id"`

	// At 은 서버가 찍은 수신 시각이다. 클라이언트 시각을 쓰지 않는 이유는
	// 그걸 조작하는 것만으로 간격 분포를 사람처럼 꾸밀 수 있기 때문이다.
	At time.Time `json:"at"`

	FPHash   string `json:"fp_hash,omitempty"`
	IPPrefix string `json:"ip_prefix,omitempty"`

	// IntervalMS 는 직전 heartbeat 과의 간격이다(서버 관측치). 첫 신호면 0.
	// 매크로는 이 값의 분산이 비정상적으로 작다.
	IntervalMS int64 `json:"interval_ms,omitempty"`

	// SolveMS 는 PoW 풀이에 걸린 시간이다(클라이언트 보고값).
	// GPU 팜은 일관되게 빠르다 — 값 자체보다 분포의 이상치가 신호다.
	SolveMS int64 `json:"solve_ms,omitempty"`

	// Difficulty 는 그때 적용된 PoW 난이도다. 풀이 시간을 난이도로 정규화하는 데 쓴다.
	Difficulty int `json:"difficulty,omitempty"`

	// PointerEntropy 는 포인터/터치 이벤트의 다양성이다(클라이언트 보고값).
	PointerEntropy float64 `json:"pointer_entropy,omitempty"`

	// Visible 은 대기 탭이 화면에 떠 있는지다.
	Visible bool `json:"visible,omitempty"`

	// WaitedMS 는 진입~입장 지연이다(KindAdmit 전용).
	WaitedMS int64 `json:"waited_ms,omitempty"`
}

// Valid 는 스코어러가 쓸 수 있는 최소 정보가 있는지 본다.
func (e Event) Valid() bool {
	return e.Kind != "" && e.EventID != "" && e.Shard != "" && e.TokenID != ""
}
