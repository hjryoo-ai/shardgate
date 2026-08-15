//go:build integration

// 불변식 5 — "탐지 경로(Kafka consumer)와 admit 경로는 결합 금지. 스코어러가
// 죽어도 대기열은 진행돼야 한다."
//
// 이 성질은 코드를 읽어서는 증명되지 않는다. 발행기가 Discard 로 꽂혀 있으면
// 발행 자체가 없어서 통과하고, Kafka 가 살아 있으면 결합이 있어도 통과한다.
// 그래서 여기서는 **진짜 Producer 를 아무도 듣지 않는 브로커에 물려 놓고**
// 컨슈머는 아예 띄우지 않은 채 진입 → 구매를 완주시킨다.
package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/botscore"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	lua "github.com/hjr/shardgate/scripts/lua"
)

// deadBroker 는 아무도 듣지 않는 주소다. 127.0.0.1:1 은 즉시 connection refused 를
// 돌려주므로 "Kafka 가 죽었다"를 가장 정직하게 재현한다.
const deadBroker = "127.0.0.1:1"

// deadKafka 는 갈 곳 없는 브로커에 물린 진짜 발행기를 만든다.
func deadKafka(t *testing.T) telemetry.Publisher {
	t.Helper()

	p := telemetry.NewProducer(config.Kafka{
		Enabled: true,
		Brokers: []string{deadBroker},
		Topic:   "shardgate.telemetry.separation",
	}, discardLogger(), obs.NewMetrics("separation"))

	// Discard 로 떨어졌다면 이 파일의 테스트는 전부 공허하게 통과한다.
	if _, isNoop := p.(telemetry.Discard); isNoop {
		t.Fatal("got a no-op publisher; 발행이 없으면 분리를 증명하지 못한다")
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// Kafka 가 죽어 있고 스코어러가 한 대도 없는 상태에서 진입부터 구매까지 완주한다.
func TestQueueDrainsWhileTelemetryIsDown(t *testing.T) {
	s := newStack(t, withTelemetry(deadKafka(t)))

	// 1. 게이트 통과 — PoW 검증 성공 시 여기서 telemetry 발행이 일어난다.
	joined := s.enterAndSolve("fp_sep_join")
	if joined.Token == "" || joined.State != string(queue.StatusCreated) {
		t.Fatalf("gate did not queue the user: %+v", joined)
	}

	// 2. 순번 조회.
	status, body := s.do(http.MethodGet, "/api/v1/queue/status", joined.Token, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d: %s", status, body)
	}
	var st StatusResponse
	mustJSON(t, body, &st)
	if st.State != string(queue.StatusWaiting) {
		t.Fatalf("status = %+v", st)
	}

	// 3. heartbeat — 텔레메트리를 가장 많이 쏟아내는 경로다.
	for range 3 {
		status, body = s.do(http.MethodPost, "/api/v1/queue/heartbeat", joined.Token,
			HeartbeatRequest{PointerEntropy: 0.7, Visible: true}, nil)
		if status != http.StatusOK {
			t.Fatalf("heartbeat = %d: %s", status, body)
		}
	}

	// 4. 예산 배분 → 입장 → 구매.
	s.refill()
	entry := s.admit(joined.Token)

	status, body = s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-sep"},
		map[string]string{EntryHeader: entry, IdempotencyHeader: "idem-sep"})
	if status != http.StatusCreated {
		t.Fatalf("order = %d: %s — 탐지 경로가 죽었는데 구매까지 막혔다", status, body)
	}
}

// 발행은 호출자를 붙들지 않는다. 여기서 블로킹하면 Kafka 지연이 그대로
// heartbeat 응답 시간이 되고, 대기열 전체가 탐지 파이프라인 속도에 묶인다.
func TestTelemetryPublishNeverBlocks(t *testing.T) {
	tel := deadKafka(t)

	// 버퍼(8192)를 한참 넘긴다 — 넘치는 만큼은 버려지되, 버리는 데 시간이 들면 안 된다.
	const events = 50_000
	now := time.Now()

	start := time.Now()
	for i := range events {
		tel.Publish(telemetry.Event{
			Kind: telemetry.KindHeartbeat, EventID: "sep", Shard: "s0001",
			TokenID: "tok" + strconv.Itoa(i), At: now, IntervalMS: 5000,
		})
	}
	elapsed := time.Since(start)

	// Kafka 쓰기 타임아웃 하나가 5초다. 발행이 쓰기를 기다린다면 이 상한을 넘는다.
	if elapsed > 2*time.Second {
		t.Fatalf("publishing %d events took %s — 발행이 호출자를 붙들고 있다", events, elapsed)
	}
}

// heartbeat 응답 시간이 Kafka 상태에 좌우되면 안 된다.
func TestHeartbeatLatencyIsUnaffectedByDeadKafka(t *testing.T) {
	s := newStack(t, withTelemetry(deadKafka(t)))
	qtok := s.join("sepHB")

	const beats = 20
	start := time.Now()
	for range beats {
		if status, body := s.do(http.MethodPost, "/api/v1/queue/heartbeat", qtok,
			HeartbeatRequest{PointerEntropy: 0.7, Visible: true}, nil); status != http.StatusOK {
			t.Fatalf("heartbeat = %d: %s", status, body)
		}
	}

	// 배치 쓰기 한 번이 5초 타임아웃이다. 요청이 한 번이라도 그 뒤에 줄을 섰다면 넘는다.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("%d heartbeats took %s — 요청 경로가 Kafka 에 묶여 있다", beats, elapsed)
	}
}

// admit 경로의 Lua 는 탐지 산출물을 읽지 않는다.
//
// 읽는 순간 "스코어러가 아직 쓰지 못한 값" 때문에 입장이 막히거나 늦어질 수 있고,
// 그게 정확히 불변식 5 가 금지하는 결합이다. 조치의 결과는 score 가 아니라
// user 해시의 state(held/blocked)로만 admit 에 닿는다.
func TestAdmitScriptsDoNotReadDetectionState(t *testing.T) {
	// 탐지 파이프라인이 쓰는 키들(§3.3).
	forbidden := []string{"score", "stats", "suspicion"}

	// 입장 여부를 결정하는 세 스크립트.
	for _, name := range []string{"admit.lua", "refill_budget.lua", "redeem.lua"} {
		t.Run(name, func(t *testing.T) {
			src, err := lua.Read(name)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			code := stripLuaComments(src)
			for _, word := range forbidden {
				if strings.Contains(code, word) {
					t.Errorf("%s 가 탐지 상태(%q)를 참조한다 — 불변식 5 위반", name, word)
				}
			}
		})
	}
}

// stripLuaComments 는 주석을 걷어낸다. 주석의 설명 문구까지 검사하면
// "탐지 상태를 읽지 않는다"고 적어 둔 문장 때문에 테스트가 실패한다.
func stripLuaComments(src string) string {
	var b strings.Builder
	for line := range strings.Lines(src) {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i] + "\n"
		}
		b.WriteString(line)
	}
	return b.String()
}

// 스코어러가 도중에 죽어도 대기열은 그대로 흐른다.
//
// 조치는 프로세스가 아니라 Redis 에 남는다. 그래서 (a) 죽기 전에 내린 차단은
// 스코어러 없이도 계속 유효하고, (b) 나머지 사용자는 스코어러의 부재를
// 전혀 느끼지 않는다. 두 경로가 만나는 지점은 Redis 상태 하나뿐이라는 뜻이다.
func TestAdmissionSurvivesAScorerCrash(t *testing.T) {
	s := newStack(t, withTelemetry(deadKafka(t)))

	const users = 6
	tokens := make([]string, users)
	for i := range users {
		tokens[i] = s.join("sepCrash" + strconv.Itoa(i))
	}

	// 스코어러가 살아 있는 동안 sepCrash0 을 차단한다.
	ctx, cancel := context.WithCancel(context.Background())
	act := botscore.NewActuator(s.rdb, s.event, s.cfg.BotScore,
		s.cfg.Token.QueueTTL, nil, discardLogger(), obs.NewMetrics("separation"))
	act.Apply(ctx, []botscore.Decision{{
		Shard: s.shard, TokenID: "sepCrash0", Score: 95,
		Action: botscore.ActionBlock, Contributing: 3,
	}})

	// 스코어러 사망. 이후로는 어떤 신호도 처리되지 않는다.
	cancel()

	s.refill()

	// (a) 죽기 전 조치는 그대로 유효하다. 조치는 프로세스가 아니라 Redis 에 남는다.
	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", tokens[0], nil, nil)
	if status != http.StatusForbidden {
		t.Fatalf("blocked user redeem = %d, want 403: %s", status, body)
	}
	var apiErr struct {
		Code string `json:"code"`
	}
	mustJSON(t, body, &apiErr)
	if apiErr.Code != string(admission.StatusBlocked) {
		t.Fatalf("blocked user code = %q, want %q", apiErr.Code, admission.StatusBlocked)
	}
	if strings.Contains(string(body), "entry_token") {
		t.Fatalf("blocked user received an entry token: %s", body)
	}

	// (b) 나머지는 전원 입장한다.
	admitted := 0
	for _, qt := range tokens[1:] {
		_, body := s.do(http.MethodPost, "/api/v1/admission/redeem", qt, nil, nil)
		var rd RedeemResponse
		mustJSON(t, body, &rd)
		if rd.Status == string(admission.StatusAdmitted) {
			admitted++
		}
	}
	if admitted != users-1 {
		t.Fatalf("admitted %d of %d — 스코어러가 죽자 대기열이 멈췄다", admitted, users-1)
	}
}
