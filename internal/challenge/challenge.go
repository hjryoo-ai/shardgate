// Package challenge 는 진입 챌린지(PoW)를 발급하고 검증한다(DESIGN.md §4-L2).
//
// 목적은 봇을 "막는" 것이 아니라 **비싸게 만드는** 것이다. 사람 한 명은 0.2~1초를
// 한 번 쓰고 끝이지만, 계정 1만 개를 돌리는 봇팜은 그 비용을 1만 배로 문다.
//
// # 챌린지는 상태를 남기지 않는다
//
// 발급 시점에 Redis 에 쓰지 않고, 챌린지 자체에 서버 HMAC 서명을 붙여 보낸다.
// 진입 경로는 폭주가 가장 심한 곳이다 — 여기서 발급 1건마다 Redis 쓰기가 생기면
// "원 서버를 보호하는 관문"이 스스로 병목이 된다. 쓰기는 검증에 성공한 요청만 한다.
//
// 서명이 지키는 것은 난이도다. 서명이 없으면 클라이언트가 difficulty=1 로 고쳐 보내
// 챌린지 전체를 무력화할 수 있다. 그래서 난이도·만료·nonce 를 한 덩어리로 서명한다.
//
// # 난이도는 이 패키지가 정하지 않는다
//
// 적응형 난이도(§4-L2)의 입력은 의심도이고, 의심도는 점수 파이프라인의 산출물이다.
// 그래서 난이도는 DifficultyProvider 로 주입받기만 한다 — 이 패키지 안에는
// "얼마나 어렵게 할지"를 정하는 코드가 없다.
package challenge

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
)

// Algorithm 은 클라이언트에게 알려 주는 풀이 규칙 식별자다.
//
//	SHA256(nonce + "." + solution) 의 상위 difficulty 비트가 전부 0
const Algorithm = "sha256-leading-zeros"

// 검증 실패 사유. gate 는 이걸 지표로만 집계하고 응답에는 뭉뚱그린다.
var (
	ErrSignature  = errors.New("challenge: bad signature")
	ErrExpired    = errors.New("challenge: expired")
	ErrDifficulty = errors.New("challenge: difficulty out of range")
	ErrSolution   = errors.New("challenge: solution does not meet difficulty")
	ErrReplay     = errors.New("challenge: already solved")
	ErrMalformed  = errors.New("challenge: malformed")
	ErrNoKey      = errors.New("challenge: hmac key is required")
)

// Reason 은 오류를 지표 라벨용 짧은 문자열로 바꾼다.
func Reason(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrSignature):
		return "bad_signature"
	case errors.Is(err, ErrExpired):
		return "expired"
	case errors.Is(err, ErrDifficulty):
		return "bad_difficulty"
	case errors.Is(err, ErrSolution):
		return "bad_solution"
	case errors.Is(err, ErrReplay):
		return "replay"
	default:
		return "malformed"
	}
}

// Challenge 는 클라이언트에게 내려보내는 퍼즐이다.
// 클라이언트는 이걸 그대로 되돌려 보내야 한다 — 서버는 아무것도 기억하지 않는다.
type Challenge struct {
	Nonce      string `json:"nonce"`
	Difficulty int    `json:"difficulty"`
	ExpiresAt  int64  `json:"expires_at_ms"`
	Signature  string `json:"signature"`
	Algorithm  string `json:"algorithm"`
}

// Subject 는 난이도 산정의 대상이다. 원본 지문과 전체 IP 는 여기 들어오지 않는다(불변식 6).
type Subject struct {
	FPHash   string
	IPPrefix string

	// Attempt 는 재챌린지 회차다(0 = 최초 진입, 1 = 첫 재검증 …).
	//
	// 이 패키지는 이 값으로 무엇을 할지 정하지 않는다. 난이도를 정하는 코드는
	// botscore 에만 있다는 규칙 그대로다 — 여기서는 입력을 실어 나르기만 한다.
	Attempt int
}

// DifficultyProvider 는 이 요청에 적용할 난이도를 알려준다.
//
// 이 인터페이스가 존재하는 이유는 "난이도는 botscore 가 주는 값만 사용한다"는 규칙을
// 주석이 아니라 구조로 강제하기 위해서다. Phase 4 의 botscore 가 의심도를 반영한
// 구현을 끼워 넣는다.
type DifficultyProvider interface {
	Difficulty(ctx context.Context, s Subject) (int, error)
}

// Fixed 는 설정된 기본 난이도를 그대로 쓰는 구현이다(의심도 신호가 없을 때).
type Fixed int

// Difficulty 는 고정 난이도를 돌려준다.
func (f Fixed) Difficulty(context.Context, Subject) (int, error) { return int(f), nil }

// Issuer 는 챌린지를 발급하고 검증한다.
type Issuer struct {
	rdb        redis.UniversalClient
	key        []byte
	eventID    string
	ttl        time.Duration
	nonceBytes int
	base       int
	maxDiff    int
	provider   DifficultyProvider
	now        func() time.Time
}

// NewIssuer 는 챌린지 발급기를 만든다. provider 가 nil 이면 기본 난이도를 쓴다.
func NewIssuer(rdb redis.UniversalClient, eventID string, cfg config.Challenge, provider DifficultyProvider) (*Issuer, error) {
	if cfg.HMACKey.IsZero() {
		return nil, ErrNoKey
	}
	if eventID == "" {
		return nil, errors.New("challenge: event id is required")
	}
	if provider == nil {
		provider = Fixed(cfg.BaseDifficulty)
	}
	return &Issuer{
		rdb:        rdb,
		key:        cfg.HMACKey.Bytes(),
		eventID:    eventID,
		ttl:        cfg.TTL,
		nonceBytes: cfg.NonceBytes,
		base:       cfg.BaseDifficulty,
		maxDiff:    cfg.MaxDifficulty,
		provider:   provider,
		now:        time.Now,
	}, nil
}

// WithClock 은 만료 검증을 테스트에서 재현하기 위한 훅이다.
func (i *Issuer) WithClock(fn func() time.Time) *Issuer {
	i.now = fn
	return i
}

// Issue 는 이 요청자에게 맞는 난이도의 챌린지를 만든다. Redis 를 건드리지 않는다.
func (i *Issuer) Issue(ctx context.Context, s Subject) (Challenge, error) {
	difficulty, err := i.provider.Difficulty(ctx, s)
	if err != nil {
		// 난이도를 못 구했다고 진입을 막지는 않는다. 의심도 조회 실패가 곧
		// 대기실 장애가 되면, 스코어러가 죽을 때 이벤트 전체가 멈춘다(불변식 5).
		difficulty = i.base
	}
	difficulty = i.clamp(difficulty)

	raw := make([]byte, i.nonceBytes)
	if _, err := rand.Read(raw); err != nil {
		return Challenge{}, fmt.Errorf("challenge: nonce: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(raw)
	expires := i.now().Add(i.ttl).UnixMilli()

	return Challenge{
		Nonce:      nonce,
		Difficulty: difficulty,
		ExpiresAt:  expires,
		Signature:  i.sign(nonce, difficulty, expires),
		Algorithm:  Algorithm,
	}, nil
}

// Required 는 지금 이 주체에게 적용되는 난이도다(clamp 후).
//
// 서명은 난이도가 **바뀌지 않았음**을 보장할 뿐, 그 난이도가 이 요청에 **맞는
// 값인지**는 말해 주지 않는다. 같은 이벤트의 서명이면 어디서 받은 챌린지든
// 어디에나 낼 수 있으므로, 싼 곳에서 받아 비싼 곳에 내는 길이 열린다.
// 호출자가 그 길을 막아야 할 때 쓴다(재챌린지 — rechallenge.go 참고).
func (i *Issuer) Required(ctx context.Context, s Subject) int {
	d, err := i.provider.Difficulty(ctx, s)
	if err != nil {
		return i.base
	}
	return i.clamp(d)
}

// Verify 는 되돌아온 챌린지와 풀이를 검사하고, 통과하면 nonce 를 소각한다.
//
// 검사 순서가 중요하다. 서명을 가장 먼저 보는 이유는, 서명이 깨진 챌린지의
// 난이도·만료 값은 애초에 서버가 정한 값이 아니기 때문이다. 소각은 마지막에 —
// 풀이가 틀린 요청에도 nonce 를 태우면, 남의 nonce 를 아무 답으로 태워 버리는
// 공격이 성립한다.
func (i *Issuer) Verify(ctx context.Context, c Challenge, solution string) error {
	if c.Nonce == "" || c.Signature == "" || solution == "" {
		return ErrMalformed
	}
	if !hmac.Equal([]byte(c.Signature), []byte(i.sign(c.Nonce, c.Difficulty, c.ExpiresAt))) {
		return ErrSignature
	}
	if c.Difficulty < 1 || c.Difficulty > i.maxDiff {
		return ErrDifficulty
	}
	if i.now().UnixMilli() > c.ExpiresAt {
		return ErrExpired
	}
	if !Check(c.Nonce, solution, c.Difficulty) {
		return ErrSolution
	}
	return i.burn(ctx, c)
}

// burn 은 nonce 를 1회용으로 만든다.
//
// SET NX 한 번이면 충분하다 — 단일 키에 대한 원자 연산이고, "먼저 도착한 하나만
// 성공"이라는 성질이 그 자체로 보장된다. 여러 명령으로 쪼갤 이유가 없으므로
// Lua 를 거치지 않는다(불변식 1 은 큐 상태 전이에 대한 규칙이고, 이건 큐가 아니다).
func (i *Issuer) burn(ctx context.Context, c Challenge) error {
	ttl := time.UnixMilli(c.ExpiresAt).Sub(i.now())
	if ttl <= 0 {
		return ErrExpired
	}

	ok, err := i.rdb.SetNX(ctx, keys.Challenge(i.eventID, c.Nonce), "1", ttl).Result()
	if err != nil {
		return fmt.Errorf("challenge: burn nonce: %w", err)
	}
	if !ok {
		return ErrReplay
	}
	return nil
}

// clamp 는 난이도를 설정된 범위 안으로 잘라 낸다.
// 잘못 배선된 provider 가 사람이 못 푸는 난이도를 내려보내는 것을 막는다.
func (i *Issuer) clamp(d int) int {
	if d < i.base {
		return i.base
	}
	if d > i.maxDiff {
		return i.maxDiff
	}
	return d
}

// sign 은 챌린지 본문에 대한 HMAC 이다. 난이도가 여기 포함돼 있어 클라이언트가
// 난이도만 낮춰 되돌려 보내는 것이 불가능하다.
func (i *Issuer) sign(nonce string, difficulty int, expires int64) string {
	mac := hmac.New(sha256.New, i.key)
	_, _ = mac.Write([]byte(i.eventID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strconv.Itoa(difficulty)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strconv.FormatInt(expires, 10)))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Check 는 풀이가 난이도를 만족하는지 본다.
func Check(nonce, solution string, difficulty int) bool {
	return LeadingZeroBits(Digest(nonce, solution)) >= difficulty
}

// Digest 는 챌린지 해시다.
func Digest(nonce, solution string) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(nonce))
	_, _ = h.Write([]byte{'.'})
	_, _ = h.Write([]byte(solution))
	var out [sha256.Size]byte
	h.Sum(out[:0])
	return out
}

// LeadingZeroBits 는 다이제스트 앞쪽의 연속된 0 비트 수를 센다.
func LeadingZeroBits(d [sha256.Size]byte) int {
	n := 0
	for _, b := range d {
		if b != 0 {
			return n + bits.LeadingZeros8(b)
		}
		n += 8
	}
	return n
}

// Solve 는 챌린지를 푼다. 클라이언트가 할 일이지만, 테스트와 부하 시뮬레이터에도
// 같은 규칙이 필요하므로 여기에 둔다.
func Solve(nonce string, difficulty, limit int) (string, bool) {
	for i := range limit {
		s := strconv.Itoa(i)
		if Check(nonce, s, difficulty) {
			return s, true
		}
	}
	return "", false
}
