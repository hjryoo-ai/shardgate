// Package api 는 HTTP 핸들러다.
//
// cmd/* 는 조립만 하므로 핸들러는 여기 모인다. 이 패키지의 규칙은 하나다:
// **상태를 바꾸는 핸들러는 예외 없이 토큰 검증을 먼저 통과한다**(불변식 2).
// 검증되지 않은 요청이 닿을 수 있는 것은 읽기뿐이고, 그마저도 자기 토큰이
// 가리키는 샤드·토큰 ID 밖은 볼 수 없다.
package api

import (
	"net/http"
	"strings"

	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/token"
)

// TokenCookie 는 큐 토큰을 담는 쿠키 이름이다.
//
// SSE(EventSource)는 커스텀 헤더를 붙이지 못한다. 그렇다고 토큰을 쿼리스트링에
// 실으면 액세스 로그·리퍼러·프록시 캐시에 그대로 남는다. 쿠키는 두 문제를 모두 피한다.
const TokenCookie = "sg_queue_token" //nolint:gosec // 쿠키 이름이지 자격증명이 아니다

// EntryHeader 는 입장 토큰을 싣는 헤더다.
const EntryHeader = "X-Entry-Token"

// IdempotencyHeader 는 멱등키 헤더다(불변식 4).
const IdempotencyHeader = "Idempotency-Key"

// bearer 는 Authorization: Bearer <token> 에서 토큰을 꺼낸다.
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// queueToken 은 요청에서 큐 토큰을 찾는다. 헤더가 우선이고 쿠키가 대체 경로다.
func queueToken(r *http.Request) string {
	if t := bearer(r); t != "" {
		return t
	}
	if c, err := r.Cookie(TokenCookie); err == nil {
		return c.Value
	}
	return ""
}

// binding 은 이 요청의 현재 맥락(지문 해시·IP 프리픽스)이다.
// 원본 지문과 전체 IP 는 여기서도 다루지 않는다(불변식 6).
func binding(r *http.Request, v4bits, v6bits int) token.Binding {
	return token.Binding{
		FPHash:   r.Header.Get("X-Device-Fingerprint"),
		IPPrefix: httpx.IPPrefix(httpx.ClientIP(r), v4bits, v6bits),
	}
}

// authenticator 는 핸들러들이 공유하는 토큰 검증기다.
type authenticator struct {
	issuer  *token.Issuer
	eventID string
	v4bits  int
	v6bits  int
	met     *obs.Metrics
}

// verify 는 토큰을 검증하고 실패를 지표에 남긴다.
// 실패 사유는 지표로만 집계하고 응답에는 뭉뚱그려 내보낸다 — 어떤 검사에서 걸렸는지
// 정확히 알려주면 공격자에게 어디를 고쳐야 하는지 알려주는 셈이 된다.
func (a *authenticator) verify(r *http.Request, raw string, kind token.Kind) (token.Claims, error) {
	if raw == "" {
		if a.met != nil {
			a.met.TokenRejected.WithLabelValues(string(kind), "missing").Inc()
		}
		return token.Claims{}, httpx.NewAPIError(http.StatusUnauthorized, "token_required", "queue token is required")
	}

	claims, err := a.issuer.VerifyBound(raw, kind, a.eventID, binding(r, a.v4bits, a.v6bits))
	if err != nil {
		if a.met != nil {
			a.met.TokenRejected.WithLabelValues(string(kind), token.Reason(err)).Inc()
		}
		return claims, httpx.NewAPIError(http.StatusUnauthorized, "token_invalid", "token is not valid for this request")
	}
	return claims, nil
}
