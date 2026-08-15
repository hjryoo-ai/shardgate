package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/shard"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

// FingerprintHeader 는 클라이언트가 계산한 디바이스 지문 **해시**를 싣는 헤더다.
// 원본 지문은 서버로 오지 않는다(불변식 6).
const FingerprintHeader = "X-Device-Fingerprint"

// GateAPI 는 대기열의 입구다.
//
//	POST /api/v1/queue/enter        진입 → PoW 챌린지 발급
//	POST /api/v1/challenge/verify   풀이 검증 → 샤드 배정 → 큐 토큰 발급
//	POST /api/v1/challenge/reissue  재챌린지 발급 (greylist 복귀용)
//	POST /api/v1/challenge/reverify 재챌린지 검증 → 원 샤드·원 순번 복귀
//
// 앞의 두 개만 토큰 없이 닿을 수 있다. enter 는 Redis 에 아무것도 쓰지 않고,
// verify 는 PoW 를 통과한 요청만 상태를 만든다 — "토큰 검증 없이 상태를 바꾸지
// 않는다"(불변식 2)의 입구 버전이다.
//
// 뒤의 두 개는 큐 토큰을 요구한다. greylist 는 조치이지 종점이 아니고(§4),
// 나가는 문이 여기다. 구현과 근거는 rechallenge.go 에 있다.
type GateAPI struct {
	chal     *challenge.Issuer
	assigner *shard.Assigner
	queue    *queue.Store
	tokens   *token.Issuer
	auth     authenticator
	tel      telemetry.Publisher
	log      *slog.Logger
	met      *obs.Metrics
	eventID  string
	v4bits   int
	v6bits   int
	pollHint time.Duration
	secure   bool

	// 재챌린지 정책. 값의 근거는 config.BotScore 의 각 필드 주석에 있다.
	rechalMax  int
	rechalPass int
	rechalHold int
}

// NewGateAPI 는 게이트 API 를 만든다.
func NewGateAPI(chal *challenge.Issuer, assigner *shard.Assigner, store *queue.Store,
	tokens *token.Issuer, tel telemetry.Publisher, cfg *config.Config, log *slog.Logger, met *obs.Metrics,
) *GateAPI {
	if tel == nil {
		tel = telemetry.Discard{}
	}
	return &GateAPI{
		chal: chal, assigner: assigner, queue: store, tokens: tokens, tel: tel,
		auth: authenticator{
			issuer: tokens, eventID: cfg.Event.ID,
			v4bits: cfg.Token.IPv4PrefixBits, v6bits: cfg.Token.IPv6PrefixBits, met: met,
		},
		log: log, met: met, eventID: cfg.Event.ID,
		v4bits: cfg.Token.IPv4PrefixBits, v6bits: cfg.Token.IPv6PrefixBits,
		pollHint: cfg.Queue.StatusPollHint,
		secure:   cfg.Token.SecureCookie,

		rechalMax:  cfg.BotScore.RechallengeMaxAttempts,
		rechalPass: cfg.BotScore.RechallengePassScore,
		rechalHold: cfg.BotScore.Hold,
	}
}

// Register 는 라우트를 mux 에 붙인다.
func (g *GateAPI) Register(mux *http.ServeMux) {
	mux.Handle("POST /api/v1/queue/enter", httpx.Handler(g.enter))
	mux.Handle("POST /api/v1/challenge/verify", httpx.Handler(g.verify))
	mux.Handle("POST /api/v1/challenge/reissue", httpx.Handler(g.reissue))
	mux.Handle("POST /api/v1/challenge/reverify", httpx.Handler(g.reverify))
}

// EnterRequest 는 진입 요청이다. 본문은 선택이다.
type EnterRequest struct {
	// FingerprintHash 는 클라이언트가 계산한 지문 해시다. 헤더로도 받는다.
	FingerprintHash string `json:"fingerprint_hash,omitempty"`
}

// EnterResponse 는 진입 응답이다.
type EnterResponse struct {
	Event     string              `json:"event"`
	Challenge challenge.Challenge `json:"challenge"`
	PollAfter int64               `json:"poll_after_ms"`
}

// enter 는 챌린지를 발급한다.
//
// 이 경로는 오픈 순간 초당 수십만 건이 들어오는 곳이다. 그래서 여기서는 Redis 를
// 건드리지 않는다 — 챌린지는 서명만 붙어 나가고, 쓰기는 풀이를 가져온 요청에만 생긴다.
func (g *GateAPI) enter(w http.ResponseWriter, r *http.Request) error {
	var body EnterRequest
	if r.ContentLength > 0 {
		if err := httpx.DecodeJSON(w, r, &body); err != nil {
			return err
		}
	}

	subj := challenge.Subject{
		FPHash:   g.fingerprint(r, body.FingerprintHash),
		IPPrefix: httpx.IPPrefix(httpx.ClientIP(r), g.v4bits, g.v6bits),
	}

	c, err := g.chal.Issue(r.Context(), subj)
	if err != nil {
		g.log.Error("challenge issue failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "could not issue a challenge")
	}

	if g.met != nil {
		g.met.ChallengeIssued.WithLabelValues(g.eventID).Inc()
		g.met.PoWDifficulty.WithLabelValues(g.eventID).Observe(float64(c.Difficulty))
	}

	httpx.WriteJSON(w, http.StatusOK, EnterResponse{
		Event: g.eventID, Challenge: c, PollAfter: g.pollHint.Milliseconds(),
	})
	return nil
}

// VerifyRequest 는 챌린지 풀이 제출이다. 발급받은 챌린지를 그대로 되돌려 보낸다.
type VerifyRequest struct {
	Challenge       challenge.Challenge `json:"challenge"`
	Solution        string              `json:"solution"`
	FingerprintHash string              `json:"fingerprint_hash,omitempty"`
	// SolveMS 는 클라이언트가 잰 풀이 시간이다. 신호일 뿐 판단 근거가 아니다 —
	// GPU 팜의 일관되게 빠른 풀이 시간은 §4-L5 의 탐지 신호 중 하나다.
	SolveMS int64 `json:"solve_ms,omitempty"`
}

// VerifyResponse 는 큐 토큰 발급 결과다.
type VerifyResponse struct {
	Event      string `json:"event"`
	Token      string `json:"token"`
	Shard      string `json:"shard"`
	State      string `json:"state"`
	Segment    string `json:"segment,omitempty"`
	Rank       int64  `json:"rank"`
	ShardSize  int64  `json:"shard_size"`
	ExpiresAt  int64  `json:"expires_at_ms"`
	PollAfter  int64  `json:"poll_after_ms"`
	AlreadyIn  bool   `json:"already_queued"`
	ShardCount int    `json:"shard_count"`
}

// verify 는 풀이를 검증하고 대기열에 넣은 뒤 큐 토큰을 발급한다.
func (g *GateAPI) verify(w http.ResponseWriter, r *http.Request) error {
	var body VerifyRequest
	if err := httpx.DecodeJSON(w, r, &body); err != nil {
		return err
	}

	if err := g.chal.Verify(r.Context(), body.Challenge, body.Solution); err != nil {
		reason := challenge.Reason(err)
		if g.met != nil {
			g.met.ChallengeVerified.WithLabelValues(g.eventID, reason).Inc()
		}
		// 어떤 검사에서 걸렸는지 정확히 알려 주면 공격자에게 고칠 곳을 알려주는 셈이다.
		// 상세 사유는 지표로만 남긴다.
		var apiErr error = httpx.NewAPIError(http.StatusForbidden, "challenge_failed", "challenge verification failed")
		if errors.Is(err, challenge.ErrExpired) {
			// 만료만은 구분해 준다 — 정상 사용자가 탭을 오래 열어 둔 흔한 경우이고,
			// 여기서 "다시 받으세요"를 알려주지 않으면 그냥 못 들어간다.
			apiErr = httpx.NewAPIError(http.StatusGone, "challenge_expired", "challenge expired, request a new one")
		}
		return apiErr
	}

	if g.met != nil {
		g.met.ChallengeVerified.WithLabelValues(g.eventID, "ok").Inc()
		if body.SolveMS > 0 {
			g.met.PoWSolveSeconds.WithLabelValues(g.eventID).Observe(float64(body.SolveMS) / 1000)
		}
	}

	tokenID, err := token.NewID()
	if err != nil {
		g.log.Error("token id generation failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusInternalServerError, "internal", "could not issue a queue token")
	}

	// 샤드는 서버가 정한다. 배정 입력은 서버가 만든 token_id 와 비공개 event_salt 뿐이라
	// 클라이언트가 원하는 샤드로 갈 방법이 없다(§3.1).
	shardID := g.assigner.Assign(tokenID)

	fp := g.fingerprint(r, body.FingerprintHash)
	ipp := httpx.IPPrefix(httpx.ClientIP(r), g.v4bits, g.v6bits)

	ticket, err := g.queue.Enqueue(r.Context(), queue.EnqueueRequest{
		Shard: shardID, TokenID: tokenID, FPHash: fp, IPPrefix: ipp,
	})
	if err != nil {
		g.log.Error("enqueue failed", slog.Any("error", err), slog.String("shard", shardID))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "queue is temporarily unavailable")
	}

	signed, claims, err := g.tokens.Issue(token.Claims{
		Kind: token.KindQueue, EventID: g.eventID, TokenID: tokenID,
		JTI: tokenID, Shard: shardID, FPHash: fp, IPPrefix: ipp,
	})
	if err != nil {
		g.log.Error("queue token signing failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusInternalServerError, "internal", "could not issue a queue token")
	}

	// SSE(EventSource)는 커스텀 헤더를 못 붙인다. 쿠키로도 실어 보내되,
	// 쿼리스트링에는 절대 싣지 않는다 — 액세스 로그와 리퍼러에 그대로 남는다.
	//nolint:gosec // Secure 는 설정값이다. 기본이 true 이고, 로컬 http 개발에서만 끈다.
	http.SetCookie(w, &http.Cookie{
		Name:     TokenCookie,
		Value:    signed,
		Path:     "/",
		HttpOnly: true,
		Secure:   g.secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  claims.ExpiresAt,
	})

	// 진입 표본과 PoW 풀이 시간을 스코어러에 흘려보낸다(§4-L5).
	// 발행은 절대 블로킹하지 않으므로 여기서 실패해도 진입은 영향받지 않는다.
	g.tel.Publish(telemetry.Event{
		Kind: telemetry.KindEnter, EventID: g.eventID, Shard: shardID, TokenID: tokenID,
		At: time.Now(), FPHash: fp, IPPrefix: ipp,
	})
	if body.SolveMS > 0 {
		g.tel.Publish(telemetry.Event{
			Kind: telemetry.KindChallenge, EventID: g.eventID, Shard: shardID, TokenID: tokenID,
			At: time.Now(), FPHash: fp, IPPrefix: ipp,
			SolveMS: body.SolveMS, Difficulty: body.Challenge.Difficulty,
		})
	}

	httpx.WriteJSON(w, http.StatusOK, VerifyResponse{
		Event: g.eventID, Token: signed, Shard: shardID,
		State: string(ticket.Status), Segment: string(ticket.Segment),
		Rank: ticket.Rank, ShardSize: ticket.Size,
		ExpiresAt:  claims.ExpiresAt.UnixMilli(),
		PollAfter:  g.pollHint.Milliseconds(),
		AlreadyIn:  ticket.Status == queue.StatusExists,
		ShardCount: g.assigner.Count(),
	})
	return nil
}

// fingerprint 는 헤더와 본문 중 있는 쪽의 지문 해시를 고른다.
// 지문이 없어도 진입은 막지 않는다 — 지문을 못 만드는 브라우저 설정을 봇으로
// 취급하면, 걸러지는 쪽은 봇이 아니라 프라이버시를 챙기는 사람이다.
func (g *GateAPI) fingerprint(r *http.Request, fromBody string) string {
	if h := r.Header.Get(FingerprintHeader); h != "" {
		return h
	}
	return fromBody
}
