//go:build integration

// 최소 관찰 게이트(§3.4)의 E2E.
//
// 이 게이트가 없으면 §12-7 의 경주에서 진다 — 점수가 오르기 전에 입장해 버린
// 사용자는 격리될 기회 자체를 얻지 못한다. 검증할 성질은 두 가지이고,
// 두 번째가 이 게이트를 ADMIT_AFTER_LOTTERY 와 구분 짓는 지점이다.
//
//  1. 관찰이 끝나기 전에는 순번이 와도 입장하지 못한다.
//  2. 그때 **자리는 사라지지 않는다.** 게이트 검사가 DECR 앞에 있기 때문이다.
//     주기를 통째로 건너뛰는 ADMIT_AFTER_LOTTERY 는 막힌 주기의 자리를 잃는다.
package api

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/queue"
)

// budget 은 샤드에 남은 예산을 읽는다. 게이트가 자리를 태우는지 보는 유일한 방법이다.
func (s *stack) budget() int64 { return s.budgetOf(s.shard) }

// budgetOf 는 샤드를 지정해 읽는다. 실제 진입(enterAndSolve)은 HMAC 배정을 타므로
// 기본 샤드에 떨어진다는 보장이 없다.
func (s *stack) budgetOf(shardID string) int64 {
	s.t.Helper()
	v, err := s.rdb.Get(context.Background(), keys.Budget(s.event, shardID)).Int64()
	if err != nil {
		s.t.Fatalf("budget %s: %v", shardID, err)
	}
	return v
}

// redeem 은 입장을 시도하고 결과를 돌려준다.
func (s *stack) redeem(qtok string) (int, RedeemResponse) {
	s.t.Helper()
	status, body := s.do(http.MethodPost, "/api/v1/admission/redeem", qtok, nil, nil)
	var rd RedeemResponse
	mustJSON(s.t, body, &rd)
	return status, rd
}

func TestObservationGateDefersAdmissionWithoutBurningTheSeat(t *testing.T) {
	const dwell = 30 * time.Second

	s := newStack(t, withEnv(map[string]string{
		"SG_ADMIT_MIN_DWELL": dwell.String(),
	}))
	qtok := s.join("obsA")
	s.refill()

	before := s.budget()

	// 진입 직후: 순번은 1등이고 예산도 남아 있지만 아직 판정할 만큼 보지 못했다.
	status, rd := s.redeem(qtok)
	if status != http.StatusOK {
		t.Fatalf("redeem status = %d, want 200 (거절이 아니라 유예다)", status)
	}
	if rd.Status != string(admission.StatusObserving) {
		t.Fatalf("status = %q, want %q", rd.Status, admission.StatusObserving)
	}
	if rd.EntryToken != "" {
		t.Fatal("관찰 중인데 입장 토큰이 나왔다")
	}
	if rd.RetryMS <= 0 {
		t.Fatalf("retry_after_ms = %d, want > 0 (언제 다시 올지 알려 줘야 한다)", rd.RetryMS)
	}
	if got := s.budget(); got != before {
		t.Fatalf("예산 %d → %d: 관찰 중인 사용자가 자리를 태웠다", before, got)
	}

	// 관찰 시간이 지나면 같은 자리로 그대로 입장한다.
	s.admission.WithClock(func() time.Time { return time.Now().Add(dwell + time.Second) })

	status, rd = s.redeem(qtok)
	if status != http.StatusOK || rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("관찰 후 redeem = %d/%q, want 200/admitted", status, rd.Status)
	}
	if rd.EntryToken == "" {
		t.Fatal("입장했는데 입장 토큰이 없다")
	}
	if got := s.budget(); got != before-1 {
		t.Fatalf("예산 %d → %d, want %d (딱 한 자리만 써야 한다)", before, got, before-1)
	}
}

// 게이트가 걸린 동안 자리는 없어지지 않고 쌓인다. 자리 수가 줄지 않는다는 것이
// ADMIT_AFTER_LOTTERY 와의 차이이고, §3.5 에서 재는 값이기도 하다.
//
// 쌓이는 데에는 상한이 둘 있다(refill_budget.lua): 샤드당 상한과 **대기 인원**.
// 그래서 인원보다 적게 쌓이는 구간에서 재야 "미뤘을 뿐"이 보인다.
func TestObservationGateKeepsSeatsForLater(t *testing.T) {
	const (
		users  = 30
		cycles = 3
		// 600/분 × 1초 배분 주기 = 주기당 10명분.
		perCycle = 10
	)

	s := newStack(t, withEnv(map[string]string{
		"SG_ADMIT_MIN_DWELL": (30 * time.Second).String(),
	}))
	tokens := make([]string, users)
	for i := range users {
		tokens[i] = s.join("obsB" + strconv.Itoa(i))
	}

	// 세 주기를 돌리는 동안 아무도 입장하지 못한다.
	for range cycles {
		s.refill()
		for _, qt := range tokens {
			if _, rd := s.redeem(qt); rd.Status != string(admission.StatusObserving) {
				t.Fatalf("status = %q, want observing", rd.Status)
			}
		}
	}
	if got, want := s.budget(), int64(cycles*perCycle); got != want {
		t.Fatalf("예산 = %d, want %d (게이트는 자리를 없애지 않고 미룬다)", got, want)
	}

	// 관찰이 끝나면 미뤄 둔 자리가 그대로 나간다.
	s.admission.WithClock(func() time.Time { return time.Now().Add(time.Minute) })
	admitted := 0
	for _, qt := range tokens {
		if _, rd := s.redeem(qt); rd.Status == string(admission.StatusAdmitted) {
			admitted++
		}
	}
	if admitted != cycles*perCycle {
		t.Fatalf("입장 %d명, want %d (미뤄 둔 자리가 사라졌다)", admitted, cycles*perCycle)
	}
}

// 생존 신호 수로도 게이트를 걸 수 있다. 위조할 수 있는 신호지만(더 자주 보내면 된다)
// 그렇게 하면 규칙성 신호가 선명해져 스스로 손해다 — 그래서 강제력은 dwell 쪽에 둔다.
func TestObservationGateCountsHeartbeats(t *testing.T) {
	const beats = 3

	s := newStack(t, withEnv(map[string]string{
		"SG_ADMIT_MIN_BEATS": strconv.Itoa(beats),
	}))
	qtok := s.join("obsC")
	s.refill()

	for i := range beats {
		if _, rd := s.redeem(qtok); rd.Status != string(admission.StatusObserving) {
			t.Fatalf("신호 %d회에서 status = %q, want observing", i, rd.Status)
		}
		if status, _ := s.do(http.MethodPost, "/api/v1/queue/heartbeat", qtok, nil, nil); status != http.StatusOK {
			t.Fatalf("heartbeat = %d", status)
		}
	}

	if _, rd := s.redeem(qtok); rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("신호 %d회 뒤 status = %q, want admitted", beats, rd.Status)
	}
}

// **문으로 나온 사용자는 관찰을 처음부터 다시 받는다.**
//
// 이 게이트가 재는 것이 진입 시각이면, 복귀한 사용자는 조건을 이미 채운 채로
// 돌아온다 — 문이 열릴 때마다 §12-7 의 경주가 복귀 1회당 한 번씩 재개되고,
// 클램프(35)에서 임계까지 다시 오르는 동안 복귀 봇은 관찰 없이 선두에서 redeem 을
// 두드린다. 게이트가 첫 입장만 지키면 그 뒤의 재진입은 전부 무방비다.
//
// 이 결함은 §3.7 측정을 통과했다. 탐지율은 그대로였고(격리는 최초 관측 시점에
// 기록된다) 새는 것은 봇 입장 쪽이었기 때문이다.
func TestObservationGateRestartsAfterRechallenge(t *testing.T) {
	const dwell = 30 * time.Second

	s := newStack(t, withEnv(map[string]string{
		"SG_ADMIT_MIN_DWELL": dwell.String(),
	}))

	// 두 시계를 함께 움직인다. 관찰 시계를 찍는 것은 queue(rechallenge.lua)이고
	// 그 경과를 재는 것은 admission(admit.lua)이라, 따로 놀면 재는 대상이 달라진다.
	now := time.Now()
	tick := func() time.Time { return now }
	s.queue.WithClock(tick)
	s.admission.WithClock(tick)

	joined := s.enterAndSolve("fp_obs_regate")
	tokenID := tokenIDOf(t, s, joined.Token)

	// 관찰을 다 채운 뒤에 격리한다. 여기서부터 진입 시각 기준으로는 게이트가 열려 있다.
	now = now.Add(dwell + time.Second)
	s.greylist(joined.Shard, tokenID, 55)

	if res := s.passRechallenge(joined.Token); res.Outcome != string(queue.RechallengeRestored) {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}

	s.refill()
	before := s.budgetOf(joined.Shard)

	status, rd := s.redeem(joined.Token)
	if status != http.StatusOK {
		t.Fatalf("redeem = %d", status)
	}
	if rd.Status != string(admission.StatusObserving) {
		t.Fatalf("복귀 직후 status = %q, want observing — 문이 게이트를 우회했다", rd.Status)
	}
	if got := s.budgetOf(joined.Shard); got != before {
		t.Fatalf("예산 %d → %d: 되감긴 관찰이 자리를 태웠다", before, got)
	}

	// 되감긴 시계로 다시 채우면 그제서야 열린다. 자리는 그대로 기다리고 있다.
	now = now.Add(dwell + time.Second)

	if _, rd = s.redeem(joined.Token); rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("재관찰 후 status = %q, want admitted", rd.Status)
	}
	if got := s.budgetOf(joined.Shard); got != before-1 {
		t.Fatalf("예산 %d → %d, want %d", before, got, before-1)
	}
}

// 생존 신호 쪽도 같이 되감긴다. 누적 hb_count 를 그대로 보면 복귀한 사용자는
// 신호 조건도 이미 채운 상태다 — 시계만 되감고 신호를 놓치면 게이트의 절반이 샌다.
func TestObservationGateResetsHeartbeatBaselineAfterRechallenge(t *testing.T) {
	const beats = 3

	s := newStack(t, withEnv(map[string]string{
		"SG_ADMIT_MIN_BEATS": strconv.Itoa(beats),
	}))
	joined := s.enterAndSolve("fp_obs_beats")
	tokenID := tokenIDOf(t, s, joined.Token)

	for range beats {
		if status, _ := s.do(http.MethodPost, "/api/v1/queue/heartbeat", joined.Token, nil, nil); status != http.StatusOK {
			t.Fatal("heartbeat 실패")
		}
	}
	s.greylist(joined.Shard, tokenID, 55)
	if res := s.passRechallenge(joined.Token); res.Outcome != string(queue.RechallengeRestored) {
		t.Fatalf("outcome = %s, want restored", res.Outcome)
	}
	s.refill()

	if _, rd := s.redeem(joined.Token); rd.Status != string(admission.StatusObserving) {
		t.Fatalf("복귀 직후 status = %q, want observing (누적 신호를 그대로 인정했다)", rd.Status)
	}
	for range beats {
		if status, _ := s.do(http.MethodPost, "/api/v1/queue/heartbeat", joined.Token, nil, nil); status != http.StatusOK {
			t.Fatal("heartbeat 실패")
		}
	}
	if _, rd := s.redeem(joined.Token); rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("신호를 다시 채운 뒤 status = %q, want admitted", rd.Status)
	}
}

// 게이트를 끄면(기본값) 예전 동작 그대로다. 기본값을 바꾸지 않았다는 회귀 방지.
func TestObservationGateOffByDefault(t *testing.T) {
	s := newStack(t)
	qtok := s.join("obsD")
	s.refill()

	if _, rd := s.redeem(qtok); rd.Status != string(admission.StatusAdmitted) {
		t.Fatalf("status = %q, want admitted (게이트는 기본으로 꺼져 있어야 한다)", rd.Status)
	}
}
