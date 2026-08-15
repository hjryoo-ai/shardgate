//go:build integration

// Phase 2 완료 기준(§10)인 "진입 → 입장 happy path" E2E 다.
// 실제 Redis(Lua)와 실제 PostgreSQL(UNIQUE 제약)로만 증명되는 성질들이라
// 대체 구현으로는 검증되지 않는다.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/botscore"
	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/shard"
	"github.com/hjr/shardgate/internal/store/pg"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

const (
	redisImage    = "redis:8-alpine"
	postgresImage = "postgres:18-alpine"
	schemaPath    = "../../deploy/postgres/init/001_schema.sql"
)

var (
	redisURL string
	pgDSN    string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	rctr, err := tcredis.Run(ctx, redisImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis: %v\n", err)
		os.Exit(1)
	}
	if redisURL, err = rctr.ConnectionString(ctx); err != nil {
		_ = testcontainers.TerminateContainer(rctr)
		fmt.Fprintf(os.Stderr, "redis url: %v\n", err)
		os.Exit(1)
	}

	// 배포에 쓰는 스키마 파일 그대로 올린다. 테스트용 스키마를 따로 두면
	// 제약 조건이 어긋나도 테스트는 통과해 버린다.
	pctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("shardgate"),
		tcpostgres.WithUsername("shardgate"),
		tcpostgres.WithPassword("shardgate"),
		tcpostgres.WithInitScripts(schemaPath),
		// 기본 대기 시한(60초)은 스키마 초기화 + Docker Desktop 의 느린 포트 매핑을
		// 함께 견디기에 빠듯하다. 컨테이너가 뜨는 속도는 검증 대상이 아니므로 넉넉히 준다.
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	if err != nil {
		_ = testcontainers.TerminateContainer(rctr)
		fmt.Fprintf(os.Stderr, "start postgres: %v\n", err)
		os.Exit(1)
	}
	if pgDSN, err = pctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
		_ = testcontainers.TerminateContainer(rctr)
		_ = testcontainers.TerminateContainer(pctr)
		fmt.Fprintf(os.Stderr, "postgres dsn: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = testcontainers.TerminateContainer(rctr)
	_ = testcontainers.TerminateContainer(pctr)
	os.Exit(code)
}

var eventSeq atomic.Int64

type stack struct {
	t          *testing.T
	srv        *httptest.Server
	cfg        *config.Config
	issuer     *token.Issuer
	queue      *queue.Store
	admission  *admission.Store
	controller *admission.Controller
	chal       *challenge.Issuer
	assigner   *shard.Assigner
	rdb        goredis.UniversalClient
	event      string
	shard      string
}

// stackOpt 은 조립 시점의 선택지다.
type stackOpt func(*stackOpts)

type stackOpts struct {
	tel telemetry.Publisher
	env map[string]string
}

// withTelemetry 는 텔레메트리 발행기를 갈아 끼운다.
// 기본값(nil)은 각 API 가 Discard 로 바꾸므로 발행 자체가 일어나지 않는다 —
// 불변식 5 를 검증하려면 "진짜로 발행하는데 갈 곳이 없는" 상태가 필요하다.
func withTelemetry(p telemetry.Publisher) stackOpt {
	return func(o *stackOpts) { o.tel = p }
}

// withEnv 는 기본 환경변수를 덮어쓴다. 설정으로만 켜지는 기능(§3.4 게이트 등)을
// 진짜 설정 경로로 켜기 위한 것이다 — 구조체를 직접 만지면 설정 검증을 건너뛴다.
func withEnv(kv map[string]string) stackOpt {
	return func(o *stackOpts) { o.env = kv }
}

// newStack 은 전 서비스를 한 mux 위에 조립한다(gate·queue·admission·shop).
// 테스트마다 독립된 이벤트 네임스페이스를 쓰므로 컨테이너는 공유해도 간섭이 없다.
func newStack(t *testing.T, opts ...stackOpt) *stack {
	t.Helper()

	var o stackOpts
	for _, fn := range opts {
		fn(&o)
	}

	event := "e2e" + strconv.FormatInt(eventSeq.Add(1), 10)
	env := map[string]string{
		"SG_EVENT_ID":           event,
		"SG_EVENT_SALT":         "00112233445566778899aabbccddeeff",
		"SG_TOKEN_SIGNING_KEY":  "0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		"SG_CHALLENGE_HMAC_KEY": "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf",
		// PoW 는 방어 강도가 아니라 배선을 검증하는 대상이다. 테스트를 느리게 만들 이유가 없다.
		"SG_POW_BASE_DIFFICULTY": "8",
		"SG_POW_MAX_DIFFICULTY":  "20",
		"SG_SECURE_COOKIE":       "false",
		"SG_POSTGRES_DSN":        pgDSN,
		"SG_ADMIT_RATE_PER_MIN":  "600",
		"SG_ADMIT_INTERVAL":      "1s",
		"SG_SHARD_COUNT":         "4",
	}
	for k, v := range o.env {
		env[k] = v
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}, "e2e", ":0")
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	met := obs.NewMetrics("e2e")
	qstore, err := queue.New(rdb, queue.FromConfig(cfg), nil, met)
	if err != nil {
		t.Fatalf("queue store: %v", err)
	}
	astore, err := admission.NewStore(rdb, admission.FromConfig(cfg), nil, met)
	if err != nil {
		t.Fatalf("admission store: %v", err)
	}
	issuer, err := token.NewIssuer(cfg.Token)
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}

	ctx := context.Background()
	pool, err := pg.Open(ctx, cfg.Postgres)
	if err != nil {
		t.Fatalf("pg open: %v", err)
	}
	t.Cleanup(pool.Close)

	assigner, err := shard.NewAssigner(cfg.Event.Salt, cfg.Event.ShardCount, cfg.Event.MaxShardCount)
	if err != nil {
		t.Fatalf("assigner: %v", err)
	}
	// cmd/gate 와 같은 배선을 쓴다. nil(=고정 난이도)로 두면 적응형 난이도가 붙는
	// 경로 전체가 테스트에서 통째로 빠지고, 그 사실이 어디에서도 드러나지 않는다.
	difficulty := botscore.NewDifficulty(rdb, cfg.Event.ID, cfg.Challenge, cfg.BotScore, discardLogger())
	chal, err := challenge.NewIssuer(rdb, cfg.Event.ID, cfg.Challenge, difficulty)
	if err != nil {
		t.Fatalf("challenge issuer: %v", err)
	}

	mux := http.NewServeMux()
	NewGateAPI(chal, assigner, qstore, issuer, o.tel, cfg, discardLogger(), met).Register(mux)
	NewQueueAPI(qstore, issuer, o.tel, cfg, discardLogger(), met).Register(mux)
	NewAdmissionAPI(astore, issuer, o.tel, cfg, discardLogger(), met).Register(mux)
	NewShopAPI(astore, pg.NewOrders(pool), issuer, cfg, discardLogger(), met).Register(mux)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &stack{
		t: t, srv: srv, cfg: cfg, issuer: issuer,
		queue: qstore, admission: astore,
		controller: admission.NewController(astore, qstore, admission.NoopHealth{}, discardLogger(), met),
		chal:       chal, assigner: assigner,
		rdb: rdb, event: event, shard: "s0001",
	}
}

// join 은 사용자를 대기열에 넣고 큐 토큰을 발급한다(gate 가 할 일의 대역).
func (s *stack) join(tokenID string) string {
	s.t.Helper()

	if _, err := s.queue.Enqueue(context.Background(), queue.EnqueueRequest{
		Shard: s.shard, TokenID: tokenID, FPHash: "fp_" + tokenID, IPPrefix: "192.0.2.0/24",
	}); err != nil {
		s.t.Fatalf("enqueue: %v", err)
	}

	jti, err := token.NewID()
	if err != nil {
		s.t.Fatalf("jti: %v", err)
	}
	raw, _, err := s.issuer.Issue(token.Claims{
		Kind: token.KindQueue, EventID: s.event, TokenID: tokenID, JTI: jti,
		Shard: s.shard, FPHash: "fp_" + tokenID,
	})
	if err != nil {
		s.t.Fatalf("issue queue token: %v", err)
	}
	return raw
}

// do 는 요청을 보내고 상태코드와 본문을 돌려준다. 응답 바디는 여기서 닫는다.
func (s *stack) do(method, path, bearerToken string, body any, headers map[string]string) (int, []byte) {
	s.t.Helper()

	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			s.t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(context.Background(), method, s.srv.URL+path, rdr)
	if err != nil {
		s.t.Fatalf("request: %v", err)
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := s.srv.Client().Do(req)
	if err != nil {
		s.t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		s.t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, buf.Bytes()
}

func (s *stack) refill() {
	s.t.Helper()
	if _, err := s.controller.Cycle(context.Background()); err != nil {
		s.t.Fatalf("admission cycle: %v", err)
	}
}

// Phase 2 완료 기준: 진입 → 순번 조회 → 입장 교환 → 구매.
func TestHappyPathEnterToPurchase(t *testing.T) {
	s := newStack(t)
	qtok := s.join("userA")

	// 1. 순번 조회 — 아직 예산이 없어도 자기 위치는 볼 수 있다.
	status, body := s.do(http.MethodGet, "/api/v1/queue/status", qtok, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d: %s", status, body)
	}
	var st StatusResponse
	mustJSON(t, body, &st)
	if st.Rank != 0 || st.State != string(queue.StatusWaiting) {
		t.Fatalf("status = %+v", st)
	}

	// 2. 예산 배분 전에는 입장할 수 없다. 순번이 앞이라는 것만으로는 부족하다.
	status, body = s.do(http.MethodPost, "/api/v1/admission/redeem", qtok, nil, nil)
	var rd RedeemResponse
	mustJSON(t, body, &rd)
	if status != http.StatusOK || rd.Status != string(admission.StatusNotYet) {
		t.Fatalf("expected not_yet before refill, got %d %+v", status, rd)
	}
	if rd.EntryToken != "" {
		t.Fatal("entry token issued without budget")
	}

	// 3. 컨트롤러가 예산을 내려보낸다.
	s.refill()

	status, body = s.do(http.MethodPost, "/api/v1/admission/redeem", qtok, nil, nil)
	mustJSON(t, body, &rd)
	if status != http.StatusOK || rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("expected admitted, got %d %+v", status, rd)
	}
	if rd.EntryToken == "" {
		t.Fatal("admitted without an entry token")
	}

	// 4. 구매.
	status, body = s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-A"},
		map[string]string{EntryHeader: rd.EntryToken, IdempotencyHeader: "idem-1"})
	if status != http.StatusCreated {
		t.Fatalf("order = %d: %s", status, body)
	}
	var ord OrderResponse
	mustJSON(t, body, &ord)
	if ord.OrderID == 0 || ord.Replayed {
		t.Fatalf("order = %+v", ord)
	}
}

// 재시도는 안전해야 한다(불변식 4). 같은 멱등키면 같은 주문이 그대로 돌아온다.
func TestOrderIsIdempotent(t *testing.T) {
	s := newStack(t)
	qtok := s.join("userB")
	s.refill()

	entry := s.admit(qtok)

	_, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-B"},
		map[string]string{EntryHeader: entry, IdempotencyHeader: "idem-B"})
	var first OrderResponse
	mustJSON(t, body, &first)

	status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-B"},
		map[string]string{EntryHeader: entry, IdempotencyHeader: "idem-B"})
	if status != http.StatusOK {
		t.Fatalf("retry = %d: %s", status, body)
	}
	var second OrderResponse
	mustJSON(t, body, &second)
	if second.OrderID != first.OrderID {
		t.Fatalf("retry created a new order: %d then %d", first.OrderID, second.OrderID)
	}
	if !second.Replayed {
		t.Fatal("retry was not marked as replayed")
	}
}

// 입장 토큰은 1회용이다(불변식 2). 멱등키를 바꿔 다시 쓰면 통과해서는 안 된다.
func TestEntryTokenCannotBeReused(t *testing.T) {
	s := newStack(t)
	qtok := s.join("userC")
	s.refill()
	entry := s.admit(qtok)

	if status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-C"},
		map[string]string{EntryHeader: entry, IdempotencyHeader: "idem-C1"}); status != http.StatusCreated {
		t.Fatalf("first order = %d: %s", status, body)
	}

	status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-C2"},
		map[string]string{EntryHeader: entry, IdempotencyHeader: "idem-C2"})
	if status != http.StatusConflict {
		t.Fatalf("reuse = %d, want 409: %s", status, body)
	}
}

// 1인 1구매는 DB 제약이 최종 판정한다.
func TestOneOrderPerAccount(t *testing.T) {
	s := newStack(t)

	// 예산은 줄을 선 뒤에 배분된다 — refill 은 대기 인원을 넘겨 예산을 쌓아 두지 않는다.
	tok1 := s.join("userD1")
	tok2 := s.join("userD2")
	s.refill()

	entry1 := s.admit(tok1)
	entry2 := s.admit(tok2)

	if status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-shared"},
		map[string]string{EntryHeader: entry1, IdempotencyHeader: "idem-D1"}); status != http.StatusCreated {
		t.Fatalf("first order = %d: %s", status, body)
	}

	// 다른 큐 토큰, 다른 입장 토큰, 다른 멱등키 — 그래도 같은 계정이면 막힌다.
	status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-shared"},
		map[string]string{EntryHeader: entry2, IdempotencyHeader: "idem-D2"})
	if status != http.StatusConflict {
		t.Fatalf("second account order = %d, want 409: %s", status, body)
	}
}

// 예산은 정확히 배분된 만큼만 소비돼야 한다.
func TestBudgetCapsAdmissions(t *testing.T) {
	s := newStack(t)

	const users = 20
	tokens := make([]string, users)
	for i := range users {
		tokens[i] = s.join("userE" + strconv.Itoa(i))
	}

	// 600/분 × 1초 = 10명.
	s.refill()

	admitted := 0
	for _, qt := range tokens {
		_, body := s.do(http.MethodPost, "/api/v1/admission/redeem", qt, nil, nil)
		var rd RedeemResponse
		mustJSON(t, body, &rd)
		if rd.Status == string(admission.StatusAdmitted) {
			admitted++
		}
	}
	if admitted != 10 {
		t.Fatalf("admitted %d, want exactly 10 (600/min × 1s)", admitted)
	}
}

// 상태를 바꾸는 핸들러는 토큰 없이 닿을 수 없다(불변식 2).
func TestStateChangingEndpointsRequireTokens(t *testing.T) {
	s := newStack(t)
	valid := s.join("userF")

	tests := []struct {
		name    string
		method  string
		path    string
		token   string
		headers map[string]string
	}{
		{"status 무토큰", http.MethodGet, "/api/v1/queue/status", "", nil},
		{"heartbeat 무토큰", http.MethodPost, "/api/v1/queue/heartbeat", "", nil},
		{"redeem 무토큰", http.MethodPost, "/api/v1/admission/redeem", "", nil},
		{"redeem 위조 토큰", http.MethodPost, "/api/v1/admission/redeem", "not.a.jwt", nil},
		{"orders 무토큰", http.MethodPost, "/api/v1/orders", "", map[string]string{IdempotencyHeader: "x"}},
		{"orders 에 큐 토큰 사용", http.MethodPost, "/api/v1/orders", "",
			map[string]string{EntryHeader: valid, IdempotencyHeader: "x"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var body any
			if tc.path == "/api/v1/orders" {
				body = OrderRequest{AccountID: "acct-F"}
			}
			status, raw := s.do(tc.method, tc.path, tc.token, body, tc.headers)
			if status != http.StatusUnauthorized {
				t.Fatalf("= %d, want 401: %s", status, raw)
			}
		})
	}
}

// 구매에는 멱등키가 반드시 필요하다(불변식 4).
func TestOrderRequiresIdempotencyKey(t *testing.T) {
	s := newStack(t)
	qtok := s.join("userG")
	s.refill()
	entry := s.admit(qtok)

	status, body := s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-G"}, map[string]string{EntryHeader: entry})
	if status != http.StatusBadRequest {
		t.Fatalf("= %d, want 400: %s", status, body)
	}
}

func TestStatusStreamsOverSSE(t *testing.T) {
	s := newStack(t)
	qtok := s.join("userH")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.srv.URL+"/api/v1/queue/status", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+qtok)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := s.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}

	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && n == 0 {
		t.Fatalf("read sse: %v", err)
	}
	frame := string(buf[:n])
	if !strings.Contains(frame, "event: status") || !strings.Contains(frame, `"rank":0`) {
		t.Fatalf("unexpected sse frame: %q", frame)
	}
	cancel()
}

// admit 은 예산이 있는 상태에서 입장 토큰을 받아 온다.
func (s *stack) admit(queueTok string) string {
	s.t.Helper()
	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", queueTok, nil, nil)
	var rd RedeemResponse
	mustJSON(s.t, body, &rd)
	if status != http.StatusOK || rd.EntryToken == "" {
		s.t.Fatalf("admit failed: %d %s", status, body)
	}
	return rd.EntryToken
}

func mustJSON(t *testing.T, body []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
}
