package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

// 재챌린지 — greylist 를 벗어나는 유일한 문(DESIGN.md §4).
//
// # 왜 이 문이 있어야 하는가
//
// 조치 사다리에 40~69(greylist)와 70~89(보류)가 따로 있는 이유는 앞 칸이 **되돌릴
// 수 있는 검문**이기 때문이다. 나가는 길이 없으면 두 칸은 같아지고, 칸을 나눈 의미가
// 사라진다. 더 중요한 것은 오탐 쪽이다 — 점수 40 을 넘은 실제 사람이 재검증 기회
// 없이 영구 배제되면, 그 사람이 겪는 것은 긴 대기 끝의 거절 하나다. 오탐율이 낮다는
// 것은 그런 일이 드물다는 뜻이지 없다는 뜻이 아니고(§12-5), 사다리는 정확히 그
// 가능성 때문에 존재한다.
//
// # 두 단계인 이유
//
// PoW 는 발급과 검증이 나뉜다. reissue 는 Redis 를 건드리지 않고(§4-L2 무상태 발급),
// reverify 만 상태를 바꾼다. 둘 다 큐 토큰을 요구한다 — 상태를 바꾸는 핸들러는 예외
// 없이 토큰 검증을 먼저 통과한다(불변식 2).
//
// # 값싼 출구가 되지 않게 하는 것들
//
//   - 통과해도 점수는 0 이 아니라 임계 직하로 클램프된다(rechallenge.lua).
//   - 회차마다 난이도가 오른다(botscore.Difficulty 가 Attempt 를 받는다).
//   - 횟수 상한을 넘겨 오면 복귀 대신 보류로 승급된다.

// attemptOf 는 이번이 몇 번째 재검증인지다(1부터).
//
// 실패하면 0 이 아니라 **상한+1** 을 돌려준다. 회차를 못 읽었다는 것은 난이도를
// 얼마로 요구해야 할지 모른다는 뜻이고, 모를 때 싼 쪽으로 여는 것은 조회를
// 실패시키는 것이 곧 우회가 된다는 뜻이다. 난이도는 clamp 로 다시 잘리므로
// 이 값이 커도 사람이 못 푸는 값이 나가지는 않는다.
func (g *GateAPI) attemptOf(ctx context.Context, c token.Claims) int {
	snap, err := g.queue.Position(ctx, c.Shard, c.TokenID)
	if err != nil {
		g.log.Warn("rechallenge attempt lookup failed", slog.Any("error", err))
		return g.rechalMax + 1
	}
	return int(snap.Rechallenges) + 1
}

// ReissueResponse 는 재챌린지 발급 결과다.
type ReissueResponse struct {
	Event     string              `json:"event"`
	Challenge challenge.Challenge `json:"challenge"`
	// Attempt 는 이번이 몇 번째 재검증인지다(1부터).
	Attempt int `json:"attempt"`
	// AttemptsLeft 는 이번 회차를 포함해 남은 기회다. 0 이면 다음은 보류다.
	AttemptsLeft int   `json:"attempts_left"`
	PollAfter    int64 `json:"poll_after_ms"`
}

// reissue 는 greylist 사용자에게 상향된 난이도의 챌린지를 내려보낸다.
//
// 발급은 무상태다. greylist 인지 아닌지만 확인하고 서명된 퍼즐을 돌려준다 —
// 실제 상태 전이는 풀이를 가져온 reverify 에서만 일어난다.
func (g *GateAPI) reissue(w http.ResponseWriter, r *http.Request) error {
	claims, err := g.auth.verify(r, queueToken(r), token.KindQueue)
	if err != nil {
		return err
	}

	snap, err := g.queue.Position(r.Context(), claims.Shard, claims.TokenID)
	if err != nil {
		g.log.Error("position lookup failed", slog.Any("error", err), slog.String("shard", claims.Shard))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "queue is temporarily unavailable")
	}
	if snap.Status != queue.StatusGreylist {
		// 재검증할 것이 없다. 상태를 알려 주되 퍼즐은 주지 않는다 —
		// 아무나 난이도 상향된 nonce 를 받아 갈 수 있으면 그 자체가 자원이 된다.
		return httpx.NewAPIError(http.StatusConflict, "not_greylisted",
			"this token is not awaiting re-verification")
	}

	attempt := int(snap.Rechallenges) + 1
	if attempt > g.rechalMax {
		// 기회는 이미 소진됐다. 그래도 문을 완전히 닫지는 않는다 — reverify 가
		// 보류로 승급시키는 것이 다음 단계이고, 그 판단은 Lua 가 원자적으로 한다.
		// 여기서 미리 거절하면 두 곳이 같은 정책을 각자 판단하게 된다.
		attempt = g.rechalMax + 1
	}

	c, err := g.chal.Issue(r.Context(), challenge.Subject{
		FPHash:   g.fingerprint(r, ""),
		IPPrefix: httpx.IPPrefix(httpx.ClientIP(r), g.v4bits, g.v6bits),
		Attempt:  attempt,
	})
	if err != nil {
		g.log.Error("rechallenge issue failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "could not issue a challenge")
	}

	if g.met != nil {
		g.met.ChallengeIssued.WithLabelValues(g.eventID).Inc()
		g.met.PoWDifficulty.WithLabelValues(g.eventID).Observe(float64(c.Difficulty))
		g.met.Rechallenge.WithLabelValues(g.eventID, "issued").Inc()
	}

	left := g.rechalMax - int(snap.Rechallenges)
	if left < 0 {
		left = 0
	}
	httpx.WriteJSON(w, http.StatusOK, ReissueResponse{
		Event: g.eventID, Challenge: c, Attempt: attempt,
		AttemptsLeft: left, PollAfter: g.pollHint.Milliseconds(),
	})
	return nil
}

// ReverifyRequest 는 재챌린지 풀이 제출이다.
type ReverifyRequest struct {
	Challenge challenge.Challenge `json:"challenge"`
	Solution  string              `json:"solution"`
	SolveMS   int64               `json:"solve_ms,omitempty"`
}

// ReverifyResponse 는 재검증 결과다.
type ReverifyResponse struct {
	Event  string `json:"event"`
	Status string `json:"status"`
	// Outcome 은 restored | exhausted | noop | no_rank 다.
	Outcome  string `json:"outcome"`
	Shard    string `json:"shard"`
	Rank     int64  `json:"rank"`
	Attempts int64  `json:"attempts"`
	// AttemptsLeft 는 남은 재검증 기회다.
	AttemptsLeft int   `json:"attempts_left"`
	PollAfter    int64 `json:"poll_after_ms"`
}

// reverify 는 풀이를 검증하고 원 샤드의 원 순번으로 되돌린다.
func (g *GateAPI) reverify(w http.ResponseWriter, r *http.Request) error {
	claims, err := g.auth.verify(r, queueToken(r), token.KindQueue)
	if err != nil {
		return err
	}

	var body ReverifyRequest
	if err := httpx.DecodeJSON(w, r, &body); err != nil {
		return err
	}

	// **싼 챌린지를 다른 곳에서 받아 오는 길을 먼저 막는다.**
	//
	// 서명은 난이도가 변조되지 않았음만 보장한다. 어디서 받았는지는 서명에 없으므로,
	// greylist 사용자가 `/queue/enter` 에서 기본 난이도(깨끗한 지문·대역으로 두드리면
	// 더 싸다) 챌린지를 받아 여기에 내면 회차 상향이 통째로 무력화된다. 이전 회차에
	// 받아 둔 챌린지를 TTL 안에 아껴 쓰는 것도 같은 우회다.
	//
	// **하한은 회차만으로 계산한다.** 지문·대역을 비워 의심도를 일부러 빼는 것이다.
	// 의심도는 **남이 격리될 때도 오른다** — 봇팜은 지문을 공유하므로 특히 자주 오르고,
	// 사람도 같은 /24 를 쓰는 누군가 때문에 오른다. 그 값을 하한에 넣으면 발급과 제출
	// 사이에 요구치가 올라가 방금 푼 답이 거절되고, 요구치가 푸는 속도보다 빨리 오르면
	// 아무리 풀어도 통과하지 못하는 교착이 된다. 되돌아올 길을 만들려던 것이 되돌아올
	// 수 없는 길이 되는 셈이다.
	//
	// 회차는 그 토큰 자신의 이력이라 한 라운드 동안 변하지 않는다. 그래서 하한으로
	// 안전하고, 우회를 막는 데는 이것으로 충분하다 — 다른 데서 받아 온 챌린지에는
	// 이 토큰의 회차가 반영돼 있을 수 없다.
	want := g.chal.Required(r.Context(), challenge.Subject{
		Attempt: g.attemptOf(r.Context(), claims),
	})
	if body.Challenge.Difficulty < want {
		if g.met != nil {
			g.met.Rechallenge.WithLabelValues(g.eventID, "too_easy").Inc()
		}
		return httpx.NewAPIError(http.StatusConflict, "challenge_stale",
			"this challenge is below the required difficulty, request a new one")
	}

	// 검증이 먼저다. 풀이가 틀린 요청이 상태를 건드리는 일은 없다.
	if err := g.chal.Verify(r.Context(), body.Challenge, body.Solution); err != nil {
		reason := challenge.Reason(err)
		if g.met != nil {
			g.met.ChallengeVerified.WithLabelValues(g.eventID, reason).Inc()
			g.met.Rechallenge.WithLabelValues(g.eventID, "failed").Inc()
		}
		if errors.Is(err, challenge.ErrExpired) {
			return httpx.NewAPIError(http.StatusGone, "challenge_expired", "challenge expired, request a new one")
		}
		return httpx.NewAPIError(http.StatusForbidden, "challenge_failed", "challenge verification failed")
	}
	if g.met != nil {
		g.met.ChallengeVerified.WithLabelValues(g.eventID, "ok").Inc()
		if body.SolveMS > 0 {
			g.met.PoWSolveSeconds.WithLabelValues(g.eventID).Observe(float64(body.SolveMS) / 1000)
		}
	}

	res, err := g.queue.Rechallenge(r.Context(), queue.RechallengeRequest{
		Shard: claims.Shard, TokenID: claims.TokenID,
		MaxAttempts: g.rechalMax, PassScore: g.rechalPass, HoldScore: g.rechalHold,
	})
	if err != nil {
		g.log.Error("rechallenge failed", slog.Any("error", err), slog.String("shard", claims.Shard))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "queue is temporarily unavailable")
	}
	if g.met != nil {
		g.met.Rechallenge.WithLabelValues(g.eventID, string(res.Outcome)).Inc()
	}

	switch res.Outcome {
	case queue.RechallengeRestored:
		// 스코어러의 누적 점수는 그 프로세스의 메모리에 있고 Redis 의 score 는
		// 사본이다. 클램프가 스코어러에 닿지 않으면 다음 창에서 곧바로 재격리돼
		// 통과가 무의미해진다. Redis 로 찔러 넣지 않고 이미 있는 Kafka 경로로
		// 보내는 이유는 불변식 5 다 — 게이트는 스코어러를 모르는 채로 있어야 한다.
		g.tel.Publish(telemetry.Event{
			Kind: telemetry.KindRechallenge, EventID: g.eventID,
			Shard: claims.Shard, TokenID: claims.TokenID, At: time.Now(),
			FPHash: claims.FPHash, IPPrefix: claims.IPPrefix,
		})
		// 풀이 시간은 별도 신호로도 남긴다(§4-L5 PoW 분포).
		if body.SolveMS > 0 {
			g.tel.Publish(telemetry.Event{
				Kind: telemetry.KindChallenge, EventID: g.eventID,
				Shard: claims.Shard, TokenID: claims.TokenID, At: time.Now(),
				FPHash: claims.FPHash, IPPrefix: claims.IPPrefix,
				SolveMS: body.SolveMS, Difficulty: body.Challenge.Difficulty,
			})
		}

	case queue.RechallengeUnknown:
		return httpx.NewAPIError(http.StatusConflict, "unknown", "no queue position for this token")
	}

	left := g.rechalMax - int(res.Attempts)
	if left < 0 {
		left = 0
	}
	httpx.WriteJSON(w, http.StatusOK, ReverifyResponse{
		Event: g.eventID, Status: string(res.Status), Outcome: string(res.Outcome),
		Shard: claims.Shard, Rank: res.Rank, Attempts: res.Attempts,
		AttemptsLeft: left, PollAfter: g.pollHint.Milliseconds(),
	})
	return nil
}
