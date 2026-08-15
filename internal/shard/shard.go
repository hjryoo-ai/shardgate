// Package shard 는 대기열 샤드 배정을 담당한다.
//
//	shard_id = HMAC-SHA256(event_salt, queue_token_id) mod N   (DESIGN.md §3.1)
//
// 배정은 결정적이면서 **예측 불가능**해야 한다. 봇이 배정 규칙을 미리 계산할 수 있으면
// 특정 샤드로 몰려가 그 샤드를 통째로 오염시킬 수 있고, "샤드를 통계 표본으로 삼아
// 봇 클러스터를 도드라지게 본다"(§4-L5)는 이 설계의 전제가 무너진다.
// 그래서 이벤트마다 새로 만든 event_salt 를 키로 쓰고, 오픈 전까지 비공개로 둔다.
//
// 이 패키지는 salt 를 어떤 출력 경로로도 내보내지 않는다. salt 는 config.Secret 으로만
// 보관하며(String/JSON/slog 전부 redact), 원문 바이트는 HMAC 초기화 시점에만 쓰인다.
package shard

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hjr/shardgate/internal/config"
)

// 샤드 ID 표기. 일반 샤드는 "s0042", greylist 샤드는 "g0042".
// 이 문자열은 Redis 해시태그(`{event:shard}`) 안에 그대로 들어가므로
// 숫자와 접두사 한 글자 외의 문자를 허용하지 않는다.
const (
	normalPrefix   = 's'
	greylistPrefix = 'g'
	idWidth        = 4
	zeroPad        = "0000"

	// maxIDDigits 는 ParseID 의 정수 변환이 넘치지 않도록 자릿수를 제한한다.
	maxIDDigits = 9
)

// 이 패키지가 반환하는 오류.
var (
	ErrNoSalt     = errors.New("shard: event salt is required")
	ErrBadCount   = errors.New("shard: shard count out of range")
	ErrInvalidID  = errors.New("shard: invalid shard id")
	ErrNoCapacity = errors.New("shard: max shard count reached")
)

// Assigner 는 이벤트 하나의 샤드 배정기다. 여러 고루틴에서 동시에 써도 안전하다.
type Assigner struct {
	salt  config.Secret
	max   int
	count atomic.Int64
	pool  sync.Pool
}

// NewAssigner 는 salt 와 샤드 수로 배정기를 만든다.
func NewAssigner(salt config.Secret, count, max int) (*Assigner, error) {
	if salt.IsZero() {
		return nil, ErrNoSalt
	}
	if max <= 0 || count <= 0 || count > max {
		return nil, ErrBadCount
	}
	a := &Assigner{salt: salt, max: max}
	a.count.Store(int64(count))
	// HMAC 은 Reset() 하면 키가 유지된 초기 상태로 돌아간다 → 진입 경로에서
	// 매번 키 스케줄을 다시 만들지 않도록 풀링한다(§10 Phase 1: 10만 enqueue 벤치).
	a.pool.New = func() any { return hmac.New(sha256.New, a.salt.Bytes()) }
	return a, nil
}

// Count 는 현재 유효한 샤드 수 N 이다.
func (a *Assigner) Count() int { return int(a.count.Load()) }

// Max 는 샤드 수 상한이다(Kafka 파티션 수와 맞춰야 한다).
func (a *Assigner) Max() int { return a.max }

// Assign 은 큐 토큰 ID 를 샤드 ID 로 매핑한다.
func (a *Assigner) Assign(tokenID string) string { return ID(a.Index(tokenID)) }

// Index 는 배정된 샤드 번호를 반환한다.
func (a *Assigner) Index(tokenID string) int {
	h, _ := a.pool.Get().(hash.Hash)
	h.Reset()
	_, _ = io.WriteString(h, tokenID)
	var buf [sha256.Size]byte
	digest := h.Sum(buf[:0])
	a.pool.Put(h)

	// 상위 8바이트만 써도 N ≤ 4096 에서 모듈로 편향은 무시 가능한 수준이다.
	n := binary.BigEndian.Uint64(digest[:8])
	return int(n % uint64(a.Count())) //nolint:gosec // Count() > 0 은 생성 시 보장된다
}

// Grow 는 샤드 수를 n 이상으로 올린다(단조 증가).
//
// 이미 배정된 사용자는 재배치하지 않는다. 발급된 큐 토큰의 shard 클레임과
// user 해시의 orig_shard 가 그대로 진실로 남고, 늘어난 N 은 이후 발급되는
// 토큰에만 적용된다(§3.1 "기존 샤드는 재배치하지 않음 — 순번 안정성").
func (a *Assigner) Grow(n int) (int, error) {
	if n > a.max {
		return a.Count(), ErrNoCapacity
	}
	for {
		cur := a.count.Load()
		if int64(n) <= cur {
			return int(cur), nil
		}
		if a.count.CompareAndSwap(cur, int64(n)) {
			return n, nil
		}
	}
}

// EnsureCapacity 는 지금까지 진입한 인원을 목표 샤드 크기로 나눠 필요한 N 을 확보한다.
// 상한에 걸리면 상한까지만 늘리고 ErrNoCapacity 를 함께 돌려준다 — 진입 자체를
// 막지는 않는다(샤드가 목표보다 커질 뿐이다).
func (a *Assigner) EnsureCapacity(joined, shardSize int) (int, error) {
	if shardSize <= 0 {
		return a.Count(), ErrBadCount
	}
	need := (joined + shardSize - 1) / shardSize
	if need < 1 {
		need = 1
	}
	if need > a.max {
		n, _ := a.Grow(a.max)
		return n, ErrNoCapacity
	}
	return a.Grow(need)
}

// String 은 salt 를 담지 않는다 — 배정기가 로그에 찍혀도 새어 나갈 것이 없다.
func (a *Assigner) String() string {
	return "shard.Assigner{count=" + strconv.Itoa(a.Count()) + ",max=" + strconv.Itoa(a.max) + "}"
}

// LogValue 는 slog 구조화 로깅용 표현이다(§3.1: salt 는 절대 로그에 남기지 않는다).
func (a *Assigner) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("shard_count", a.Count()),
		slog.Int("max_shard_count", a.max),
	)
}

// ID 는 샤드 번호를 일반 샤드 ID 로 만든다.
func ID(n int) string { return string(normalPrefix) + pad(n) }

// GreylistID 는 같은 번호의 greylist 샤드 ID 를 만든다(§4 조치 파이프라인).
func GreylistID(n int) string { return string(greylistPrefix) + pad(n) }

func pad(n int) string {
	s := strconv.Itoa(n)
	if len(s) >= idWidth {
		return s
	}
	return zeroPad[:idWidth-len(s)] + s
}

// Validate 는 외부에서 들어온 샤드 ID 가 키 스키마를 오염시키지 않는지 확인한다.
// 샤드 문자열은 해시태그 안에 그대로 들어가므로, `:` 나 `}` 가 섞이면 다른 샤드의
// 키를 가리키게 만들 수 있다.
func Validate(id string) error {
	if len(id) < 2 || len(id)-1 > maxIDDigits {
		return ErrInvalidID
	}
	if id[0] != normalPrefix && id[0] != greylistPrefix {
		return ErrInvalidID
	}
	for i := 1; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return ErrInvalidID
		}
	}
	return nil
}

// ParseID 는 샤드 ID 에서 번호와 greylist 여부를 뽑는다.
func ParseID(id string) (index int, greylist bool, err error) {
	if err := Validate(id); err != nil {
		return 0, false, err
	}
	n, err := strconv.Atoi(id[1:])
	if err != nil {
		return 0, false, ErrInvalidID
	}
	return n, id[0] == greylistPrefix, nil
}

// IsGreylist 는 격리 관찰용 샤드인지 알려준다.
func IsGreylist(id string) bool { return len(id) > 0 && id[0] == greylistPrefix }

// Greylist 는 일반 샤드 ID 를 같은 번호의 greylist 샤드 ID 로 바꾼다.
func Greylist(id string) (string, error) {
	if err := Validate(id); err != nil {
		return "", err
	}
	return string(greylistPrefix) + id[1:], nil
}

// Origin 은 greylist 샤드 ID 를 원 샤드 ID 로 되돌린다.
// 재검증을 통과한 정상 사용자를 원 샤드 순번으로 복귀시킬 때 쓴다(§4).
func Origin(id string) (string, error) {
	if err := Validate(id); err != nil {
		return "", err
	}
	return string(normalPrefix) + id[1:], nil
}
