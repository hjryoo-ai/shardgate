package telemetry

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hjr/shardgate/internal/config"
)

var now = time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)

func full() Event {
	return Event{
		Kind: KindHeartbeat, EventID: "evt1", Shard: "s0042", TokenID: "tok_abc",
		At: now, FPHash: "fp_hash", IPPrefix: "198.51.100.0/24", IntervalMS: 5000,
	}
}

func TestEventValid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
		want   bool
	}{
		{"완전한 이벤트", func(*Event) {}, true},
		{"kind 없음", func(e *Event) { e.Kind = "" }, false},
		{"event_id 없음", func(e *Event) { e.EventID = "" }, false},
		{"shard 없음", func(e *Event) { e.Shard = "" }, false},
		{"token_id 없음", func(e *Event) { e.TokenID = "" }, false},
		// 선택 필드가 비어도 신호로서는 성립한다 — 클라이언트가 못 보내는
		// 값 때문에 사용자를 관측 대상에서 지워 버리면 탐지 모수가 왜곡된다.
		{"선택 필드 전부 없음", func(e *Event) {
			e.FPHash, e.IPPrefix, e.IntervalMS = "", "", 0
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := full()
			tc.mutate(&e)
			if got := e.Valid(); got != tc.want {
				t.Fatalf("Valid() = %v, want %v", got, tc.want)
			}
		})
	}
}

// 왕복해도 신호가 변하지 않아야 한다. 발행과 소비가 다른 프로세스라
// 여기서 어긋나면 탐지가 조용히 틀린 값을 본다.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := full()
	want.SolveMS, want.Difficulty, want.PointerEntropy, want.Visible = 420, 18, 0.73, true

	msg, err := encode(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// 파티션 키는 shard_id 다(§7). 이게 아니면 같은 샤드의 이벤트가 여러
	// 컨슈머로 흩어져 "샤드 하나의 창"이라는 전제가 깨진다.
	if string(msg.Key) != want.Shard {
		t.Fatalf("partition key = %q, want %q", msg.Key, want.Shard)
	}

	got, err := Decode(msg.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.At.Equal(want.At) {
		t.Fatalf("at = %s, want %s", got.At, want.At)
	}
	got.At, want.At = time.Time{}, time.Time{}
	if got != want {
		t.Fatalf("round trip changed the event:\n got %+v\nwant %+v", got, want)
	}
}

func TestDecodeRejectsUnusableEvents(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"JSON 아님", `not json`},
		{"빈 객체", `{}`},
		{"shard 없음", `{"kind":"heartbeat","event_id":"evt1","token_id":"tok"}`},
		{"token 없음", `{"kind":"heartbeat","event_id":"evt1","shard":"s0001"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode([]byte(tc.payload)); err == nil {
				t.Fatal("decoded an unusable event")
			}
		})
	}
}

// 불변식 6: 원본 지문과 전체 IP 는 이 구조체에 들어올 자리가 없다.
// 필드를 늘리다 실수로 원본을 실어 보내는 것을 여기서 막는다.
func TestEventCarriesNoRawIdentifiers(t *testing.T) {
	b, err := json.Marshal(full())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, field := range []string{"fingerprint", "user_agent", `"ip"`, "ip_address", "account"} {
		if strings.Contains(string(b), field) {
			t.Errorf("telemetry event carries %s — 불변식 6 위반: %s", field, b)
		}
	}
}

// Discard 는 Kafka 가 꺼져 있을 때의 경로다. 여기서 패닉이 나면
// Kafka 없이 띄운 게이트가 첫 heartbeat 에서 죽는다.
func TestDiscardIsSafe(t *testing.T) {
	var p Publisher = Discard{}
	p.Publish(full())
	p.Publish(Event{}) // 불완전한 이벤트도 그냥 삼킨다
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Kafka 가 꺼져 있으면 발행기 대신 무동작 구현을 준다 — 브로커 주소가
// 비어 있는데 진짜 Writer 를 세우면 배치마다 오류 로그만 쌓인다.
func TestProducerFallsBackToDiscard(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		brokers []string
	}{
		{"비활성", false, []string{"localhost:9092"}},
		{"브로커 없음", true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProducer(config.Kafka{
				Enabled: tc.enabled, Brokers: tc.brokers, Topic: "t", GroupID: "g",
			}, nil, nil)
			if _, ok := p.(Discard); !ok {
				t.Fatalf("got %T, want Discard", p)
			}
			_ = p.Close()
		})
	}
}

// 토픽은 스코어러가 조인하기 **전에** 있어야 한다.
//
// 자동 생성 토픽은 첫 produce 때 만들어진다. 스코어러가 그보다 먼저 뜨면 없는
// 토픽에 조인해 파티션 0개를 배정받고 Stable 이 되며, 그 뒤 토픽이 생겨도
// 리밸런스할 이유가 없어 영원히 0개다. 브로커 헬스체크는 통과하고 ReadMessage 는
// 오류 없이 블록하므로 **아무것도 실패하지 않은 채 탐지만 꺼진다.**
//
// 코드 쪽 방어는 waitForTopic 이고, 배포 쪽 방어가 이것이다. 둘 다 있어야 하는
// 이유는 하나면 지워졌을 때 조용히 원상복귀하기 때문이다.
func TestComposeCreatesTopicBeforeServicesStart(t *testing.T) {
	const compose = "../../deploy/docker-compose.yml"
	b, err := os.ReadFile(compose)
	if err != nil {
		t.Fatalf("%s: %v", compose, err)
	}
	var doc struct {
		ServiceDefaults struct {
			DependsOn map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"x-service-defaults"`
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatalf("%s 파싱: %v", compose, err)
	}

	init, ok := doc.Services["kafka-init"]
	if !ok {
		t.Fatal("kafka-init 서비스가 없다 — 토픽이 첫 produce 때까지 안 생긴다")
	}
	cmd := strings.Join(init.Command, " ")
	for _, want := range []string{"kafka-topics.sh", "--create", "--partitions"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("kafka-init 명령에 %q 가 없다: %s", want, cmd)
		}
	}

	dep, ok := doc.ServiceDefaults.DependsOn["kafka-init"]
	if !ok {
		t.Fatal("서비스들이 kafka-init 을 기다리지 않는다 — 토픽 생성과 조인이 경합한다")
	}
	if dep.Condition != "service_completed_successfully" {
		t.Fatalf("kafka-init 대기 조건 = %q, want service_completed_successfully "+
			"(컨테이너가 뜬 것과 토픽이 만들어진 것은 다르다)", dep.Condition)
	}
}
