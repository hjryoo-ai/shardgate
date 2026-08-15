package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

// AdmissionAPI 는 순번이 도달한 사용자가 입장 토큰을 받아 가는 엔드포인트다.
//
//	POST /api/v1/admission/redeem
type AdmissionAPI struct {
	auth  authenticator
	store *admission.Store
	issue *token.Issuer
	tel   telemetry.Publisher
	log   *slog.Logger
	met   *obs.Metrics
	// minDwell·pollHint 는 관찰 게이트에 걸린 사용자에게 "언제 다시 오라"를
	// 알려 주는 데만 쓴다. 강제는 Lua 가 하고, 여기 값이 틀려도 게이트는 안 뚫린다.
	minDwell time.Duration
	pollHint time.Duration
}

// NewAdmissionAPI 는 입장 API 를 만든다.
func NewAdmissionAPI(store *admission.Store, issuer *token.Issuer, tel telemetry.Publisher,
	cfg *config.Config, log *slog.Logger, met *obs.Metrics,
) *AdmissionAPI {
	if tel == nil {
		tel = telemetry.Discard{}
	}
	return &AdmissionAPI{
		auth: authenticator{
			issuer: issuer, eventID: cfg.Event.ID,
			v4bits: cfg.Token.IPv4PrefixBits, v6bits: cfg.Token.IPv6PrefixBits, met: met,
		},
		store:    store,
		issue:    issuer,
		tel:      tel,
		log:      log,
		met:      met,
		minDwell: cfg.Admission.MinDwell,
		pollHint: cfg.Queue.StatusPollHint,
	}
}

// observeRetry 는 관찰이 끝날 때까지 남은 시간이다.
// 최소한 폴링 주기만큼은 두어, 게이트가 열리기 직전인 사용자가 남은 몇 ms 를
// 재시도로 채우며 서버를 두드리지 않게 한다.
func (a *AdmissionAPI) observeRetry(waited time.Duration) time.Duration {
	if left := a.minDwell - waited; left > a.pollHint {
		return left
	}
	return a.pollHint
}

// Register 는 라우트를 mux 에 붙인다.
func (a *AdmissionAPI) Register(mux *http.ServeMux) {
	mux.Handle("POST /api/v1/admission/redeem", httpx.Handler(a.redeem))
}

// RedeemResponse 는 입장 교환 결과다.
type RedeemResponse struct {
	Status string `json:"status"`
	// EntryToken 은 status 가 admitted 일 때만 들어 있다. 1회용이며 짧은 TTL 을 갖는다.
	EntryToken string `json:"entry_token,omitempty"`
	Rank       int64  `json:"rank,omitempty"`
	Budget     int64  `json:"budget"`
	WaitedMS   int64  `json:"waited_ms,omitempty"`
	RetryMS    int64  `json:"retry_after_ms,omitempty"`
}

// redeem 은 순번이 예산 안에 들면 큐 토큰을 입장 토큰으로 바꿔 준다.
//
// jti 는 서버가 만든다. 클라이언트가 고른 값을 받으면 입장 토큰의 1회성이
// 클라이언트 손에 넘어가고, 같은 jti 로 여러 번 발행받는 길이 열린다.
func (a *AdmissionAPI) redeem(w http.ResponseWriter, r *http.Request) error {
	claims, err := a.auth.verify(r, queueToken(r), token.KindQueue)
	if err != nil {
		return err
	}

	jti, err := token.NewID()
	if err != nil {
		a.log.Error("jti generation failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusInternalServerError, "internal", "could not issue entry token")
	}

	res, err := a.store.Admit(r.Context(), claims.Shard, claims.TokenID, jti)
	if err != nil {
		a.log.Error("admit failed", slog.Any("error", err), slog.String("shard", claims.Shard))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "admission is temporarily unavailable")
	}

	switch res.Status {
	case admission.StatusAdmitted:
		// 멱등 경로: 이미 입장했다면 Lua 가 원래 jti 를 돌려준다. 그 jti 로 다시 서명해
		// 돌려주므로, 재시도해도 새 입장권이 생기지 않는다(불변식 4).
		signed, _, err := a.issue.Issue(token.Claims{
			Kind: token.KindEntry, EventID: claims.EventID, TokenID: claims.TokenID,
			JTI: res.JTI, Shard: claims.Shard, FPHash: claims.FPHash, IPPrefix: claims.IPPrefix,
		})
		if err != nil {
			a.log.Error("entry token signing failed", slog.Any("error", err))
			return httpx.NewAPIError(http.StatusInternalServerError, "internal", "could not issue entry token")
		}
		a.tel.Publish(telemetry.Event{
			Kind: telemetry.KindAdmit, EventID: claims.EventID, Shard: claims.Shard,
			TokenID: claims.TokenID, At: time.Now(),
			FPHash: claims.FPHash, IPPrefix: claims.IPPrefix,
			WaitedMS: res.Waited.Milliseconds(),
		})
		httpx.WriteJSON(w, http.StatusOK, RedeemResponse{
			Status: string(res.Status), EntryToken: signed,
			Rank: res.Rank, Budget: res.Budget, WaitedMS: res.Waited.Milliseconds(),
		})
		return nil

	case admission.StatusNotYet:
		// 아직 차례가 아니다. 오류가 아니라 정상적인 대기 상태다.
		httpx.WriteJSON(w, http.StatusOK, RedeemResponse{
			Status: string(res.Status), Rank: res.Rank, Budget: res.Budget,
		})
		return nil

	case admission.StatusObserving:
		// 순번은 왔지만 아직 판정할 만큼 보지 못했다(§3.4). not_yet 과 마찬가지로
		// 정상적인 대기이고, 자리는 없어지지 않았다 — 다시 오면 된다.
		// 403 이 아닌 이유: 조치 파이프라인이 내린 판정이 아니라 판정 이전 상태다.
		httpx.WriteJSON(w, http.StatusOK, RedeemResponse{
			Status: string(res.Status), Rank: res.Rank, Budget: res.Budget,
			RetryMS: a.observeRetry(res.Waited).Milliseconds(),
		})
		return nil

	case admission.StatusGreylist:
		// 재검증 대기다. observing 과 같은 원칙으로 200 을 준다 —
		// **판정 이전이거나 되돌릴 수 있는 상태는 오류가 아니다.** 여기서 409 를
		// 주면 재검증 기회가 있다는 사실 자체가 사용자에게 보이지 않고, 오탐으로
		// 걸린 사람이 보는 것은 긴 대기 끝의 거절 하나가 된다(§4 오탐 보호).
		// 다음 행동은 상태 이름이 아니라 할 일로 알려 준다: 재챌린지를 받아 오라.
		httpx.WriteJSON(w, http.StatusOK, RedeemResponse{
			Status: "challenge_required", Rank: res.Rank, Budget: res.Budget,
			RetryMS: a.pollHint.Milliseconds(),
		})
		return nil

	case admission.StatusHeld, admission.StatusBlocked:
		// 조치 파이프라인이 정한 상태다. 여기서 뒤집지 않는다(불변식 3).
		return httpx.NewAPIError(http.StatusForbidden, string(res.Status), "this token is not eligible for admission")

	default:
		// unknown / evicted / evicting: 대기열에 자리가 없다. 다시 줄을 서야 한다.
		return httpx.NewAPIError(http.StatusConflict, string(res.Status), "no queue position for this token")
	}
}
