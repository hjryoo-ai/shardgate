package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
)

// redactedMark 는 비밀 값이 문자열화될 때 원문 대신 나가는 표식이다.
const redactedMark = "[REDACTED]"

// ErrEmptySecret 은 필수 비밀 값이 비어 있을 때 반환된다.
var ErrEmptySecret = errors.New("secret is empty")

// Secret 은 로그·에러 메시지·JSON 어디로도 원문이 새지 않도록 마스킹되는 비밀 값이다.
// event_salt(DESIGN.md §3.1), JWT 서명키(§4-L3), PoW 챌린지 HMAC 키가 이 타입을 사용한다.
//
// fmt 의 %v/%s, slog 의 구조화 필드, encoding/json 세 경로 모두에서 redactedMark 로
// 치환되므로 "실수로 salt 를 로그에 찍는" 사고가 타입 수준에서 차단된다.
type Secret struct {
	b []byte
}

// NewSecret 은 바이트 슬라이스를 복사해 비밀 값을 만든다.
func NewSecret(b []byte) Secret {
	cp := make([]byte, len(b))
	copy(cp, b)
	return Secret{b: cp}
}

// ParseSecretHex 는 16진 문자열을 비밀 값으로 디코딩한다.
func ParseSecretHex(s string) (Secret, error) {
	if s == "" {
		return Secret{}, ErrEmptySecret
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		// err 에 원문 조각이 포함될 수 있으므로 절대 래핑하지 않는다.
		return Secret{}, errors.New("secret is not valid hex")
	}
	return Secret{b: b}, nil
}

// Bytes 는 비밀 값의 복사본을 반환한다. 호출자가 수정해도 원본은 안전하다.
func (s Secret) Bytes() []byte {
	cp := make([]byte, len(s.b))
	copy(cp, s.b)
	return cp
}

// Len 은 비밀 값의 바이트 길이를 반환한다. 길이는 비밀이 아니므로 노출해도 된다.
func (s Secret) Len() int { return len(s.b) }

// IsZero 는 비밀 값이 설정되지 않았는지 보고한다.
func (s Secret) IsZero() bool { return len(s.b) == 0 }

func (s Secret) String() string               { return redactedMark }
func (s Secret) GoString() string             { return redactedMark }
func (s Secret) LogValue() slog.Value         { return slog.StringValue(redactedMark) }
func (s Secret) MarshalText() ([]byte, error) { return []byte(redactedMark), nil }
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redactedMark + `"`), nil }

// Format 은 %x, %q 같은 동사로도 원문이 새지 않도록 모든 동사를 가로챈다.
func (s Secret) Format(f fmt.State, verb rune) {
	_, _ = f.Write([]byte(redactedMark))
	_ = verb
}
