// Package token 은 큐 토큰과 입장 토큰의 발급·검증을 담당한다(DESIGN.md §4-L3).
//
// 토큰이 하는 일은 "이 요청이 누구의 것인지"를 서버가 서명으로 확인하는 것이다.
// 그래서 상태를 바꾸는 핸들러는 예외 없이 여기를 먼저 통과해야 한다(불변식 2).
//
// 탈취·공유를 어렵게 만들기 위해 토큰에 발급 당시의 맥락을 묶어 둔다:
//   - fp_hash   : 디바이스 지문 해시 (원본 지문은 저장하지도 전달하지도 않는다, 불변식 6)
//   - ip_prefix : IP 대역 (전체 IP 가 아니다)
//
// 이 바인딩은 "다른 기기·다른 네트워크에서 같은 토큰을 쓰는" 사용을 걸러내되,
// 그 자체로 차단 근거가 되지는 않는다. 모바일 네트워크 전환처럼 정상적인 변화도
// 있기 때문에, 불일치는 신호로 점수 파이프라인에 넘긴다(불변식 3).
package token

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/hjr/shardgate/internal/config"
)

// idBytes 는 토큰 ID 의 엔트로피다. 128비트면 이벤트 규모(수백만)에서 충돌도
// 추측도 현실적으로 불가능하다.
const idBytes = 16

// NewID 는 큐 토큰 ID / jti 로 쓸 무작위 식별자를 만든다.
//
// base64url 문자만 나오므로 그대로 Redis 키에 넣어도 키 스키마를 흔들지 않는다
// (internal/queue 의 식별자 검증이 허용하는 문자 집합과 같다).
func NewID() (string, error) {
	var b [idBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("token: new id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// Kind 는 토큰의 용도다. 큐 토큰으로 구매를 시도하는 식의 혼용을 막는다.
type Kind string

// 토큰 종류.
const (
	KindQueue Kind = "queue"
	KindEntry Kind = "entry"
)

// 검증 실패 사유. obs.Metrics.TokenRejected 의 reason 라벨과 대응한다.
var (
	ErrMalformed   = errors.New("token: malformed")
	ErrSignature   = errors.New("token: bad signature")
	ErrExpired     = errors.New("token: expired")
	ErrKind        = errors.New("token: wrong kind")
	ErrEvent       = errors.New("token: wrong event")
	ErrFPMismatch  = errors.New("token: fingerprint mismatch")
	ErrIPMismatch  = errors.New("token: ip prefix mismatch")
	ErrIncomplete  = errors.New("token: missing required claim")
	ErrNoSigingKey = errors.New("token: signing key is required")
)

// Reason 은 오류를 지표 라벨로 쓸 짧은 문자열로 바꾼다.
func Reason(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrExpired):
		return "expired"
	case errors.Is(err, ErrSignature):
		return "bad_signature"
	case errors.Is(err, ErrKind):
		return "wrong_kind"
	case errors.Is(err, ErrEvent):
		return "wrong_event"
	case errors.Is(err, ErrFPMismatch):
		return "fp_mismatch"
	case errors.Is(err, ErrIPMismatch):
		return "ip_mismatch"
	case errors.Is(err, ErrIncomplete):
		return "incomplete"
	default:
		return "malformed"
	}
}

// Claims 는 이 시스템이 토큰에 담는 값이다.
type Claims struct {
	Kind      Kind
	EventID   string
	TokenID   string // 큐 토큰 ID. 입장 토큰에서는 "누구의 입장권인가"를 가리킨다.
	JTI       string // 이 토큰 자체의 1회성 ID
	Shard     string
	FPHash    string
	IPPrefix  string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// registered 는 JWT 로 직렬화되는 실제 클레임 구조다.
type registered struct {
	jwt.RegisteredClaims
	Kind     Kind   `json:"knd"`
	EventID  string `json:"evt"`
	TokenID  string `json:"tid"`
	Shard    string `json:"shd"`
	FPHash   string `json:"fph,omitempty"`
	IPPrefix string `json:"ipp,omitempty"`
}

// Issuer 는 토큰을 발급하고 검증한다. 여러 고루틴에서 동시에 써도 안전하다.
type Issuer struct {
	key      []byte
	issuer   string
	queueTTL time.Duration
	entryTTL time.Duration
	now      func() time.Time
}

// NewIssuer 는 설정에서 발급기를 만든다.
func NewIssuer(cfg config.Token) (*Issuer, error) {
	if cfg.SigningKey.IsZero() {
		return nil, ErrNoSigingKey
	}
	if cfg.QueueTTL <= 0 || cfg.EntryTTL <= 0 {
		return nil, errors.New("token: ttl must be > 0")
	}
	return &Issuer{
		key:      cfg.SigningKey.Bytes(),
		issuer:   cfg.Issuer,
		queueTTL: cfg.QueueTTL,
		entryTTL: cfg.EntryTTL,
		now:      time.Now,
	}, nil
}

// WithClock 은 만료 검증을 테스트에서 재현하기 위한 훅이다.
func (i *Issuer) WithClock(fn func() time.Time) *Issuer {
	i.now = fn
	return i
}

// TTL 은 종류별 유효 기간이다.
func (i *Issuer) TTL(kind Kind) time.Duration {
	if kind == KindEntry {
		return i.entryTTL
	}
	return i.queueTTL
}

// Issue 는 클레임에 발급 시각·만료를 채워 서명한다.
func (i *Issuer) Issue(c Claims) (string, Claims, error) {
	if c.EventID == "" || c.TokenID == "" || c.JTI == "" || c.Shard == "" {
		return "", Claims{}, ErrIncomplete
	}
	if c.Kind != KindQueue && c.Kind != KindEntry {
		return "", Claims{}, ErrKind
	}

	now := i.now().UTC().Truncate(time.Second)
	c.IssuedAt = now
	c.ExpiresAt = now.Add(i.TTL(c.Kind))

	claims := registered{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    i.issuer,
			Subject:   c.TokenID,
			ID:        c.JTI,
			IssuedAt:  jwt.NewNumericDate(c.IssuedAt),
			ExpiresAt: jwt.NewNumericDate(c.ExpiresAt),
		},
		Kind:     c.Kind,
		EventID:  c.EventID,
		TokenID:  c.TokenID,
		Shard:    c.Shard,
		FPHash:   c.FPHash,
		IPPrefix: c.IPPrefix,
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(i.key)
	if err != nil {
		return "", Claims{}, fmt.Errorf("token: sign: %w", err)
	}
	return signed, c, nil
}

// Verify 는 서명·만료·종류·이벤트를 확인한다.
func (i *Issuer) Verify(raw string, kind Kind, eventID string) (Claims, error) {
	var rc registered
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(i.now),
	)
	_, err := parser.ParseWithClaims(raw, &rc, func(*jwt.Token) (any, error) { return i.key, nil })
	if err != nil {
		switch {
		case errors.Is(err, jwt.ErrTokenExpired):
			return Claims{}, ErrExpired
		case errors.Is(err, jwt.ErrTokenSignatureInvalid), errors.Is(err, jwt.ErrTokenUnverifiable):
			return Claims{}, ErrSignature
		default:
			return Claims{}, fmt.Errorf("%w: %w", ErrMalformed, err)
		}
	}

	if rc.Kind != kind {
		return Claims{}, ErrKind
	}
	if eventID != "" && rc.EventID != eventID {
		return Claims{}, ErrEvent
	}
	if rc.TokenID == "" || rc.Shard == "" || rc.ID == "" {
		return Claims{}, ErrIncomplete
	}

	c := Claims{
		Kind:     rc.Kind,
		EventID:  rc.EventID,
		TokenID:  rc.TokenID,
		JTI:      rc.ID,
		Shard:    rc.Shard,
		FPHash:   rc.FPHash,
		IPPrefix: rc.IPPrefix,
	}
	if rc.IssuedAt != nil {
		c.IssuedAt = rc.IssuedAt.Time
	}
	if rc.ExpiresAt != nil {
		c.ExpiresAt = rc.ExpiresAt.Time
	}
	return c, nil
}

// Binding 은 요청이 들고 온 현재 맥락이다.
type Binding struct {
	FPHash   string
	IPPrefix string
}

// VerifyBound 는 Verify 에 더해 발급 당시의 맥락과 지금 맥락이 같은지 본다.
//
// 토큰에 해당 클레임이 비어 있으면 그 항목은 검사하지 않는다 — 바인딩 없이 발급된
// 토큰(예: 지문을 못 구한 클라이언트)을 검증 단계에서 일괄 거절하면,
// 저사양·프라이버시 설정 사용자를 봇으로 취급하게 된다.
func (i *Issuer) VerifyBound(raw string, kind Kind, eventID string, b Binding) (Claims, error) {
	c, err := i.Verify(raw, kind, eventID)
	if err != nil {
		return Claims{}, err
	}
	if c.FPHash != "" && b.FPHash != "" && c.FPHash != b.FPHash {
		return c, ErrFPMismatch
	}
	if c.IPPrefix != "" && b.IPPrefix != "" && c.IPPrefix != b.IPPrefix {
		return c, ErrIPMismatch
	}
	return c, nil
}
