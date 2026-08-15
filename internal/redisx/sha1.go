package redisx

import (
	"crypto/sha1" //nolint:gosec // Redis 스크립트 캐시 키 규격이 SHA-1 이다. 보안 용도 아님.
	"encoding/hex"
)

// sha1Hex 는 Redis SCRIPT LOAD 가 돌려주는 것과 동일한 스크립트 해시를 계산한다.
func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s)) //nolint:gosec // 위와 동일
	return hex.EncodeToString(sum[:])
}
