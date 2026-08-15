//go:build integration

// Phase 3 완료 기준(§10)인 공격 시나리오 테스트다.
//
// 여기 있는 것들은 전부 "통과하면 안 되는 요청"이다. 방어가 동작한다는 증거는
// happy path 가 아니라 이 목록이 붉게 실패하지 않는다는 사실에서 나온다.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/token"
)

// solveLimit 은 테스트 난이도(8비트)에서 넉넉한 시도 상한이다.
const solveLimit = 1 << 22

// enter 는 게이트에서 챌린지를 받아 온다.
func (s *stack) enter(fp string) challenge.Challenge {
	s.t.Helper()

	headers := map[string]string{}
	if fp != "" {
		headers[FingerprintHeader] = fp
	}
	status, body := s.do(http.MethodPost, "/api/v1/queue/enter", "", EnterRequest{}, headers)
	if status != http.StatusOK {
		s.t.Fatalf("enter = %d: %s", status, body)
	}
	var resp EnterResponse
	mustJSON(s.t, body, &resp)
	return resp.Challenge
}

// verify 는 풀이를 제출한다.
func (s *stack) verify(c challenge.Challenge, solution, fp string) (int, VerifyResponse, []byte) {
	s.t.Helper()

	headers := map[string]string{}
	if fp != "" {
		headers[FingerprintHeader] = fp
	}
	status, body := s.do(http.MethodPost, "/api/v1/challenge/verify", "",
		VerifyRequest{Challenge: c, Solution: solution}, headers)

	var resp VerifyResponse
	if status == http.StatusOK {
		mustJSON(s.t, body, &resp)
	}
	return status, resp, body
}

// enterAndSolve 는 정상 사용자의 진입 전 과정을 수행한다.
func (s *stack) enterAndSolve(fp string) VerifyResponse {
	s.t.Helper()

	c := s.enter(fp)
	solution, ok := challenge.Solve(c.Nonce, c.Difficulty, solveLimit)
	if !ok {
		s.t.Fatalf("could not solve difficulty %d", c.Difficulty)
	}
	status, resp, body := s.verify(c, solution, fp)
	if status != http.StatusOK {
		s.t.Fatalf("verify = %d: %s", status, body)
	}
	return resp
}

// 게이트를 통과한 사용자는 실제로 대기열에 자리를 갖는다.
func TestGateHappyPath(t *testing.T) {
	s := newStack(t)

	resp := s.enterAndSolve("fp_normal_user")
	if resp.Token == "" || resp.Shard == "" {
		t.Fatalf("verify response is incomplete: %+v", resp)
	}
	if resp.State != string(queue.StatusCreated) {
		t.Fatalf("state = %s, want created", resp.State)
	}
	if resp.Rank != 0 {
		t.Fatalf("rank = %d, want 0", resp.Rank)
	}

	// 발급된 토큰이 실제로 통해야 한다. 통하지 않으면 게이트가 자리만 만들고 끝난 셈이다.
	status, body := s.do(http.MethodGet, "/api/v1/queue/status", resp.Token, nil,
		map[string]string{FingerprintHeader: "fp_normal_user"})
	if status != http.StatusOK {
		t.Fatalf("status = %d: %s", status, body)
	}
	var st StatusResponse
	mustJSON(t, body, &st)
	if st.Shard != resp.Shard {
		t.Fatalf("shard drifted: %s → %s", resp.Shard, st.Shard)
	}
}

// 샤드는 서버가 정한다. 클라이언트가 고를 수 있으면 봇이 한 샤드로 몰려가
// "샤드를 통계 표본으로 쓴다"는 §4-L5 의 전제가 무너진다.
func TestClientCannotChooseItsShard(t *testing.T) {
	s := newStack(t)

	seen := make(map[string]int)
	for range 40 {
		seen[s.enterAndSolve("fp_same_for_everyone").Shard]++
	}
	// 같은 지문·같은 IP 로 반복해도 배정은 흩어져야 한다 — 배정 입력은 서버가 만든
	// token_id 와 비공개 salt 뿐이기 때문이다.
	if len(seen) < 2 {
		t.Fatalf("40 entries all landed on one shard: %v", seen)
	}
}

// 챌린지 1회용(§3.3 challenge 키). 같은 nonce 로 두 번 진입하면 PoW 비용을 한 번만 내고
// 계정을 무한히 만들 수 있다.
func TestChallengeCannotBeReplayed(t *testing.T) {
	s := newStack(t)

	c := s.enter("fp_replayer")
	solution, ok := challenge.Solve(c.Nonce, c.Difficulty, solveLimit)
	if !ok {
		t.Fatal("could not solve")
	}

	if status, _, body := s.verify(c, solution, "fp_replayer"); status != http.StatusOK {
		t.Fatalf("first verify = %d: %s", status, body)
	}

	status, _, body := s.verify(c, solution, "fp_replayer")
	if status != http.StatusForbidden {
		t.Fatalf("replay = %d, want 403: %s", status, body)
	}

	// nonce 는 소각 기록으로 남아 있어야 한다.
	n, err := s.rdb.Exists(context.Background(), keys.Challenge(s.event, c.Nonce)).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if n != 1 {
		t.Fatal("nonce was not burned")
	}
}

// 챌린지 본문 변조. 서명이 난이도·만료·nonce 를 함께 덮고 있어야 한다.
func TestTamperedChallengesAreRejected(t *testing.T) {
	s := newStack(t)

	tests := []struct {
		name   string
		mutate func(*challenge.Challenge)
		// solveWith 는 풀이를 계산할 난이도다. 0 이면 원래 난이도로 푼다.
		solveWith int
		wantCode  int
	}{
		{
			// 서명이 없으면 이 한 줄로 PoW 전체가 무의미해진다.
			name:      "난이도 1로 하향",
			mutate:    func(c *challenge.Challenge) { c.Difficulty = 1 },
			solveWith: 1,
			wantCode:  http.StatusForbidden,
		},
		{
			name:      "만료 연장",
			mutate:    func(c *challenge.Challenge) { c.ExpiresAt += int64(24 * time.Hour / time.Millisecond) },
			solveWith: 0,
			wantCode:  http.StatusForbidden,
		},
		{
			name:      "직접 고른 nonce",
			mutate:    func(c *challenge.Challenge) { c.Nonce = "attacker-chosen-nonce" },
			solveWith: 0,
			wantCode:  http.StatusForbidden,
		},
		{
			name:      "서명 삭제",
			mutate:    func(c *challenge.Challenge) { c.Signature = "" },
			solveWith: 0,
			wantCode:  http.StatusForbidden,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := s.enter("fp_tamper")
			tc.mutate(&c)

			difficulty := c.Difficulty
			if tc.solveWith > 0 {
				difficulty = tc.solveWith
			}
			solution, ok := challenge.Solve(c.Nonce, difficulty, solveLimit)
			if !ok {
				t.Fatal("could not solve")
			}

			status, _, body := s.verify(c, solution, "fp_tamper")
			if status != tc.wantCode {
				t.Fatalf("= %d, want %d: %s", status, tc.wantCode, body)
			}
		})
	}
}

func TestWrongSolutionIsRejected(t *testing.T) {
	s := newStack(t)
	c := s.enter("fp_lazy")

	status, _, body := s.verify(c, "not-the-answer", "fp_lazy")
	if status != http.StatusForbidden {
		t.Fatalf("= %d, want 403: %s", status, body)
	}

	// 틀린 풀이로는 nonce 가 타지 않아야 한다.
	// 아니면 남의 nonce 를 아무 답으로 태워 버리는 공격이 성립한다.
	n, err := s.rdb.Exists(context.Background(), keys.Challenge(s.event, c.Nonce)).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if n != 0 {
		t.Fatal("a wrong solution burned the nonce")
	}

	// 그래서 제대로 풀면 여전히 통과한다.
	solution, ok := challenge.Solve(c.Nonce, c.Difficulty, solveLimit)
	if !ok {
		t.Fatal("could not solve")
	}
	if status, _, body := s.verify(c, solution, "fp_lazy"); status != http.StatusOK {
		t.Fatalf("retry with the right answer = %d: %s", status, body)
	}
}

// 토큰 재사용(§4-L3): 다른 기기·다른 네트워크에서 같은 토큰을 쓰면 걸러진다.
func TestQueueTokenIsBoundToItsHolder(t *testing.T) {
	s := newStack(t)
	resp := s.enterAndSolve("fp_owner")

	tests := []struct {
		name       string
		fp         string
		wantStatus int
	}{
		{"발급받은 기기", "fp_owner", http.StatusOK},
		{"다른 기기로 공유", "fp_thief", http.StatusUnauthorized},
		{"지문 없이", "", http.StatusOK}, // 요청에 지문이 없으면 그 검사만 생략된다
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{}
			if tc.fp != "" {
				headers[FingerprintHeader] = tc.fp
			}
			status, body := s.do(http.MethodGet, "/api/v1/queue/status", resp.Token, nil, headers)
			if status != tc.wantStatus {
				t.Fatalf("= %d, want %d: %s", status, tc.wantStatus, body)
			}
		})
	}
}

// 토큰의 샤드 클레임을 바꿔 남의 샤드 순번을 노리는 시도.
func TestTamperedQueueTokenIsRejected(t *testing.T) {
	s := newStack(t)
	resp := s.enterAndSolve("fp_owner2")

	tests := []struct {
		name  string
		token string
	}{
		{"샤드 클레임 변조", retagShard(resp.Token)},
		{"서명 잘라내기", strings.Join(strings.Split(resp.Token, ".")[:2], ".") + ".AAAA"},
		{"완전한 위조", "eyJhbGciOiJub25lIn0.eyJ0aWQiOiJ4In0."},
		{"빈 토큰", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, body := s.do(http.MethodPost, "/api/v1/queue/heartbeat", tc.token, nil,
				map[string]string{FingerprintHeader: "fp_owner2"})
			if status != http.StatusUnauthorized {
				t.Fatalf("= %d, want 401: %s", status, body)
			}
		})
	}
}

// 만료된 큐 토큰으로는 아무것도 못 한다.
func TestExpiredQueueTokenIsRejected(t *testing.T) {
	s := newStack(t)

	// 이미 만료된 토큰을 발급한다(발급기 시계를 과거로).
	past := time.Now().Add(-3 * time.Hour)
	expired := s.issuer.WithClock(func() time.Time { return past })
	raw, _, err := expired.Issue(token.Claims{
		Kind: token.KindQueue, EventID: s.event, TokenID: "expiredUser",
		JTI: "expiredUser", Shard: s.shard,
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	s.issuer.WithClock(time.Now)

	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", raw, nil, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("= %d, want 401: %s", status, body)
	}
}

// 입장 토큰 탈취: 서명은 유효하지만 발행 기록의 주인이 다른 경우.
// Redis 의 소각 기록이 최종 판정한다(불변식 2).
func TestStolenEntryTokenIsRejected(t *testing.T) {
	s := newStack(t)

	victim := s.enterAndSolve("fp_victim")
	s.refill()

	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", victim.Token, nil,
		map[string]string{FingerprintHeader: "fp_victim"})
	if status != http.StatusOK {
		t.Fatalf("redeem = %d: %s", status, body)
	}
	var rd RedeemResponse
	mustJSON(t, body, &rd)
	if rd.EntryToken == "" {
		t.Fatalf("no entry token: %s", body)
	}

	// 공격자가 victim 의 jti 를 자기 토큰 ID 로 다시 서명한다.
	// (서명키를 알고 있다는, 현실보다 훨씬 강한 가정을 준 것이다.)
	stolen, _, err := s.issuer.Issue(token.Claims{
		Kind: token.KindEntry, EventID: s.event, TokenID: "attackerToken",
		JTI: entryJTI(t, s, rd.EntryToken), Shard: victim.Shard,
	})
	if err != nil {
		t.Fatalf("issue stolen: %v", err)
	}

	status, body = s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-attacker"},
		map[string]string{EntryHeader: stolen, IdempotencyHeader: "idem-attacker"})
	if status != http.StatusForbidden {
		t.Fatalf("= %d, want 403: %s", status, body)
	}

	// 피해자의 입장권은 그대로 살아 있어야 한다.
	status, body = s.do(http.MethodPost, "/api/v1/orders", "",
		OrderRequest{AccountID: "acct-victim"},
		map[string]string{EntryHeader: rd.EntryToken, IdempotencyHeader: "idem-victim"})
	if status != http.StatusCreated {
		t.Fatalf("victim order = %d, want 201: %s", status, body)
	}
}

// 진입 경로는 토큰 없이 열려 있지만, 그 대가로 상태를 만들지 않는다.
// enter 를 아무리 두드려도 Redis 에는 아무것도 쌓이지 않아야 한다.
func TestEnterCreatesNoState(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	before, err := s.rdb.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("dbsize: %v", err)
	}

	for range 50 {
		s.enter("fp_flood")
	}

	after, err := s.rdb.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("dbsize: %v", err)
	}
	if after != before {
		t.Fatalf("50 enter requests created %d keys; the entry path must stay stateless", after-before)
	}
}

// retagShard 는 토큰의 shard 클레임만 바꿔치기한다(서명은 그대로).
func retagShard(raw string) string {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return raw
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return raw
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return raw
	}
	claims["shd"] = "s0000"
	swapped, err := json.Marshal(claims)
	if err != nil {
		return raw
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(swapped)
	return strings.Join(parts, ".")
}

// entryJTI 는 입장 토큰에서 jti 를 꺼낸다.
func entryJTI(t *testing.T, s *stack, raw string) string {
	t.Helper()
	claims, err := s.issuer.Verify(raw, token.KindEntry, s.event)
	if err != nil {
		t.Fatalf("verify entry token: %v", err)
	}
	return claims.JTI
}
