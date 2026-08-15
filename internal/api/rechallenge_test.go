//go:build integration

// 재챌린지 경로의 E2E — greylist 에 걸린 사용자가 실제로 돌아 나오는가.
//
// 이 경로가 없으면 40~69(greylist)와 70~89(보류)가 같아지고, "오탐 보호"라는 말이
// 공허해진다. 점수 40 을 넘은 실제 사람이 재검증 기회 없이 영구 배제되면 그 사람이
// 겪는 것은 긴 대기 끝의 거절 하나다(REPORT §3.5).
package api

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/hjr/shardgate/internal/botscore"
	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

// tokenIDOf 는 발급된 큐 토큰에서 token_id 를 꺼낸다.
func tokenIDOf(t *testing.T, s *stack, raw string) string {
	t.Helper()
	claims, err := s.issuer.Verify(raw, token.KindQueue, s.event)
	if err != nil {
		t.Fatalf("verify queue token: %v", err)
	}
	return claims.TokenID
}

// recordingPublisher 는 발행된 이벤트를 그대로 모은다.
type recordingPublisher struct {
	mu     sync.Mutex
	events []telemetry.Event
}

func (p *recordingPublisher) Publish(e telemetry.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

func (p *recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) has(kind telemetry.Kind, tokenID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if e.Kind == kind && e.TokenID == tokenID {
			return true
		}
	}
	return false
}

// greylist 는 조치 파이프라인을 실제로 태워 사용자를 격리한다.
// 상태를 손으로 만들지 않는 이유는, 손으로 만든 상태는 move_shard.lua 가 실제로
// 만드는 모양과 어긋날 수 있고 그 어긋남 자체가 REPORT §3.5 의 결함이었기 때문이다.
func (s *stack) greylist(shardID, tokenID string, score float64) {
	s.t.Helper()

	act := botscore.NewActuator(s.rdb, s.event, s.cfg.BotScore,
		s.cfg.Token.QueueTTL, nil, discardLogger(), nil)
	act.Apply(context.Background(), []botscore.Decision{{
		Shard: shardID, TokenID: tokenID, Score: score, Action: botscore.ActionGreylist,
		Signals:      botscore.Signals{botscore.SignalHeartbeat: 0.9, botscore.SignalFingerprint: 0.8},
		Contributing: 2,
	}})

	snap, err := s.queue.Position(context.Background(), shardID, tokenID)
	if err != nil {
		s.t.Fatalf("position: %v", err)
	}
	if snap.Status != queue.StatusGreylist {
		s.t.Fatalf("greylist 준비 실패: state = %s", snap.Status)
	}
}

func (s *stack) reissue(queueToken string) (int, ReissueResponse, []byte) {
	return s.reissueAs(queueToken, "")
}

// reissueAs 는 지문 헤더를 실어 재챌린지를 요청한다. 난이도는 지문·대역의 의심도와
// 회차에서 나오므로(botscore.Difficulty), 지문이 빠지면 그 축 하나가 조용히 사라진다.
func (s *stack) reissueAs(queueToken, fp string) (int, ReissueResponse, []byte) {
	s.t.Helper()
	headers := map[string]string{}
	if fp != "" {
		headers[FingerprintHeader] = fp
	}
	status, body := s.do(http.MethodPost, "/api/v1/challenge/reissue", queueToken, struct{}{}, headers)
	var resp ReissueResponse
	if status == http.StatusOK {
		mustJSON(s.t, body, &resp)
	}
	return status, resp, body
}

func (s *stack) reverify(queueToken string, c challenge.Challenge, solution string) (int, ReverifyResponse, []byte) {
	s.t.Helper()
	status, body := s.do(http.MethodPost, "/api/v1/challenge/reverify", queueToken,
		ReverifyRequest{Challenge: c, Solution: solution, SolveMS: 120}, nil)
	var resp ReverifyResponse
	if status == http.StatusOK {
		mustJSON(s.t, body, &resp)
	}
	return status, resp, body
}

// solveReissued 는 재챌린지를 받아 풀어 제출한다.
func (s *stack) passRechallenge(queueToken string) ReverifyResponse {
	s.t.Helper()

	status, issued, body := s.reissue(queueToken)
	if status != http.StatusOK {
		s.t.Fatalf("reissue = %d: %s", status, body)
	}
	solution, ok := challenge.Solve(issued.Challenge.Nonce, issued.Challenge.Difficulty, solveLimit)
	if !ok {
		s.t.Fatalf("could not solve difficulty %d", issued.Challenge.Difficulty)
	}
	status, resp, body := s.reverify(queueToken, issued.Challenge, solution)
	if status != http.StatusOK {
		s.t.Fatalf("reverify = %d: %s", status, body)
	}
	return resp
}

// redeem 이 greylist 사용자에게 409 가 아니라 200 + challenge_required 를 준다.
//
// **판정 이전이거나 되돌릴 수 있는 상태는 오류가 아니다.** observing 과 같은 원칙이다.
// 409 를 주면 재검증 기회가 있다는 사실 자체가 사용자에게 보이지 않는다.
func TestRedeemAsksGreylistedUserToRechallenge(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_grey_user")
	s.greylist(joined.Shard, tokenIDOf(t, s, joined.Token), 45)

	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", joined.Token, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("redeem = %d: %s (greylist 는 오류가 아니라 재검증 대기다)", status, body)
	}
	var resp RedeemResponse
	mustJSON(t, body, &resp)
	if resp.Status != "challenge_required" {
		t.Fatalf("status = %q, want challenge_required", resp.Status)
	}
	if resp.EntryToken != "" {
		t.Fatal("greylist 사용자에게 입장 토큰이 나갔다")
	}
	if resp.RetryMS <= 0 {
		t.Fatal("언제 다시 오라는 안내가 없다")
	}
}

// 전체 경로: greylist → 재챌린지 통과 → 원 순번 복귀 → 입장.
func TestRechallengeRoundTripEndsInAdmission(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_round_trip")
	tokenID := tokenIDOf(t, s, joined.Token)
	origRank := joined.Rank

	s.greylist(joined.Shard, tokenID, 55)

	res := s.passRechallenge(joined.Token)
	if res.Outcome != string(queue.RechallengeRestored) {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}
	if res.AttemptsLeft != s.cfg.BotScore.RechallengeMaxAttempts-1 {
		t.Fatalf("attempts_left = %d", res.AttemptsLeft)
	}

	// 점수는 0 이 아니라 임계 직하다.
	snap, err := s.queue.Position(context.Background(), joined.Shard, tokenID)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if snap.Status != queue.StatusWaiting {
		t.Fatalf("state = %s, want waiting", snap.Status)
	}
	if snap.Rank != origRank {
		t.Fatalf("rank = %d, want %d — 재검증을 통과한 사람은 순번을 잃지 않는다", snap.Rank, origRank)
	}
	if snap.BotScore != int64(s.cfg.BotScore.RechallengePassScore) {
		t.Fatalf("score = %d, want %d (0 으로 리셋하면 면제권이 된다)",
			snap.BotScore, s.cfg.BotScore.RechallengePassScore)
	}

	// 이제 입장할 수 있다.
	s.refill()
	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", joined.Token, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("redeem = %d: %s", status, body)
	}
	var resp RedeemResponse
	mustJSON(t, body, &resp)
	if resp.Status != "admitted" || resp.EntryToken == "" {
		t.Fatalf("redeem status = %q, token empty = %v", resp.Status, resp.EntryToken == "")
	}
}

// 회차마다 난이도가 오른다. 재챌린지가 값싼 출구가 되지 않게 하는 축 하나다.
func TestRechallengeDifficultyRisesEachRound(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_difficulty")
	tokenID := tokenIDOf(t, s, joined.Token)

	base := s.enter("fp_difficulty").Difficulty

	s.greylist(joined.Shard, tokenID, 55)
	_, first, _ := s.reissueAs(joined.Token, "fp_difficulty")
	if first.Challenge.Difficulty <= base {
		t.Fatalf("1회차 난이도 %d <= 기본 %d", first.Challenge.Difficulty, base)
	}
	if first.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", first.Attempt)
	}

	s.passRechallenge(joined.Token)
	s.greylist(joined.Shard, tokenID, 55)
	_, second, _ := s.reissueAs(joined.Token, "fp_difficulty")

	if second.Challenge.Difficulty <= first.Challenge.Difficulty {
		t.Fatalf("2회차 난이도 %d <= 1회차 %d", second.Challenge.Difficulty, first.Challenge.Difficulty)
	}
	if second.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", second.Attempt)
	}
}

// 횟수를 소진하면 복귀 대신 보류로 올라간다.
func TestRechallengeExhaustionEscalates(t *testing.T) {
	s := newStack(t, withEnv(map[string]string{
		"SG_RECHALLENGE_MAX_ATTEMPTS": "1",
		// 난이도가 회차마다 오르므로 테스트에서는 상향폭을 1비트로 줄인다.
		"SG_GREYLIST_DIFFICULTY_BUMP": "1",
	}))
	joined := s.enterAndSolve("fp_exhaust")
	tokenID := tokenIDOf(t, s, joined.Token)

	s.greylist(joined.Shard, tokenID, 55)
	if res := s.passRechallenge(joined.Token); res.Outcome != string(queue.RechallengeRestored) {
		t.Fatalf("1회차 outcome = %s, want restored", res.Outcome)
	}

	s.greylist(joined.Shard, tokenID, 55)
	res := s.passRechallenge(joined.Token)

	if res.Outcome != string(queue.RechallengeExhausted) {
		t.Fatalf("2회차 outcome = %s, want exhausted", res.Outcome)
	}
	if res.Status != string(queue.StatusHeld) {
		t.Fatalf("status = %s, want held", res.Status)
	}
	if res.AttemptsLeft != 0 {
		t.Fatalf("attempts_left = %d, want 0", res.AttemptsLeft)
	}

	// 보류는 admit 대상이 아니다. 그리고 여기서는 403 이 맞다 —
	// 되돌릴 수 있지만 사용자가 스스로 되돌릴 수 있는 상태는 아니기 때문이다.
	s.refill()
	status, _ := s.do(http.MethodPost, "/api/v1/admission/redeem", joined.Token, nil, nil)
	if status != http.StatusForbidden {
		t.Fatalf("redeem = %d, want 403", status)
	}
}

// greylist 가 아닌 사용자는 재챌린지를 받아 갈 수 없다.
// 아무나 nonce 를 받아 갈 수 있으면 그 자체가 자원이 된다.
func TestReissueRejectsNonGreylistedUser(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_not_grey")

	status, _, body := s.reissue(joined.Token)
	if status != http.StatusConflict {
		t.Fatalf("reissue = %d: %s, want 409", status, body)
	}
}

// **다른 곳에서 받은 싼 챌린지로 재검증을 통과할 수 없다.**
//
// 서명은 난이도가 변조되지 않았음만 보장하고 어디서 받았는지는 말해 주지 않는다.
// 이 검사가 없으면 greylist 사용자가 `/queue/enter` 에서 기본 난이도 챌린지를 받아
// 여기에 내는 것으로 회차 상향(가드레일 2)이 통째로 무력화된다.
func TestReverifyRejectsCheapChallengeFromEnter(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_smuggle")
	tokenID := tokenIDOf(t, s, joined.Token)
	s.greylist(joined.Shard, tokenID, 55)

	// 깨끗한 지문·대역으로 진입 경로를 두드려 기본 난이도 챌린지를 받아 온다.
	// 봇이 실제로 쓸 수 있는 수단이다 — enter 는 토큰을 요구하지 않는다.
	status, body := s.do(http.MethodPost, "/api/v1/queue/enter", "", EnterRequest{}, map[string]string{
		"X-Forwarded-For": "198.51.100.7",
	})
	if status != http.StatusOK {
		t.Fatalf("enter = %d: %s", status, body)
	}
	var clean EnterResponse
	mustJSON(t, body, &clean)
	cheap := clean.Challenge

	_, issued, _ := s.reissueAs(joined.Token, "fp_smuggle")
	if cheap.Difficulty >= issued.Challenge.Difficulty {
		t.Fatalf("전제가 성립하지 않는다: enter %d >= reissue %d",
			cheap.Difficulty, issued.Challenge.Difficulty)
	}

	solution, ok := challenge.Solve(cheap.Nonce, cheap.Difficulty, solveLimit)
	if !ok {
		t.Fatalf("could not solve difficulty %d", cheap.Difficulty)
	}
	rstatus, _, rbody := s.reverify(joined.Token, cheap, solution)
	if rstatus != http.StatusConflict {
		t.Fatalf("reverify = %d: %s, want 409 (싼 챌린지가 통과했다)", rstatus, rbody)
	}

	snap, err := s.queue.Position(context.Background(), joined.Shard, tokenID)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if snap.Status != queue.StatusGreylist {
		t.Fatalf("state = %s, want greylist", snap.Status)
	}
}

// **남 때문에 오른 의심도가 내 재검증을 막지 않는다.**
//
// 하한을 회차가 아니라 의심도로 잡으면, 발급과 제출 사이에 같은 지문·대역의 다른
// 참가자가 격리되는 것만으로 방금 푼 답이 거절된다. 봇팜은 지문을 공유하므로 그
// 일이 자주 일어나고, 사람도 같은 /24 를 쓰는 누군가 때문에 걸린다. 요구치가 푸는
// 속도보다 빨리 오르면 아무리 풀어도 통과하지 못하는 교착이 된다 — 되돌아올 길을
// 만들려던 것이 되돌아올 수 없는 길이 된다.
func TestReverifySurvivesSuspicionRisingMidFlight(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_shared_farm")
	tokenID := tokenIDOf(t, s, joined.Token)
	s.greylist(joined.Shard, tokenID, 55)

	// 챌린지를 먼저 받는다.
	status, issued, body := s.reissueAs(joined.Token, "fp_shared_farm")
	if status != http.StatusOK {
		t.Fatalf("reissue = %d: %s", status, body)
	}

	// 푸는 동안 같은 지문을 쓰는 다른 참가자들이 줄줄이 격리된다.
	for range 5 {
		other := s.enterAndSolve("fp_shared_farm")
		s.greylist(other.Shard, tokenIDOf(t, s, other.Token), 55)
	}

	solution, ok := challenge.Solve(issued.Challenge.Nonce, issued.Challenge.Difficulty, solveLimit)
	if !ok {
		t.Fatalf("could not solve difficulty %d", issued.Challenge.Difficulty)
	}
	rstatus, res, rbody := s.reverify(joined.Token, issued.Challenge, solution)
	if rstatus != http.StatusOK {
		t.Fatalf("reverify = %d: %s — 남의 격리가 내 재검증을 막았다", rstatus, rbody)
	}
	if res.Outcome != string(queue.RechallengeRestored) {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}
}

// 토큰 없이는 재챌린지 경로에 닿을 수 없다(불변식 2).
func TestRechallengeRequiresQueueToken(t *testing.T) {
	s := newStack(t)
	for _, path := range []string{"/api/v1/challenge/reissue", "/api/v1/challenge/reverify"} {
		t.Run(path, func(t *testing.T) {
			status, _ := s.do(http.MethodPost, path, "", struct{}{}, nil)
			if status != http.StatusUnauthorized {
				t.Fatalf("%s = %d, want 401", path, status)
			}
		})
	}
}

// 틀린 풀이는 상태를 바꾸지 않는다. 검증이 먼저다.
func TestReverifyRejectsBadSolution(t *testing.T) {
	s := newStack(t)
	joined := s.enterAndSolve("fp_bad_solution")
	tokenID := tokenIDOf(t, s, joined.Token)
	s.greylist(joined.Shard, tokenID, 55)

	_, issued, _ := s.reissue(joined.Token)
	status, _, _ := s.reverify(joined.Token, issued.Challenge, "not-a-solution")
	if status != http.StatusForbidden {
		t.Fatalf("reverify = %d, want 403", status)
	}

	snap, err := s.queue.Position(context.Background(), joined.Shard, tokenID)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if snap.Status != queue.StatusGreylist {
		t.Fatalf("state = %s, want greylist — 틀린 풀이가 상태를 바꿨다", snap.Status)
	}
}

// 통과 사실은 텔레메트리로 스코어러에 전달된다.
//
// 누적 점수의 진실은 스코어러의 메모리에 있고 Redis 의 score 는 사본이다.
// 클램프가 스코어러에 닿지 않으면 다음 창에서 곧바로 재격리돼 통과가 무의미해진다.
func TestRechallengePublishesClampNotice(t *testing.T) {
	rec := &recordingPublisher{}
	s := newStack(t, withTelemetry(rec))
	joined := s.enterAndSolve("fp_notice")
	tokenID := tokenIDOf(t, s, joined.Token)
	s.greylist(joined.Shard, tokenID, 55)

	s.passRechallenge(joined.Token)

	if !rec.has(telemetry.KindRechallenge, tokenID) {
		t.Fatal("재챌린지 통과가 스코어러에게 통지되지 않았다")
	}
}
