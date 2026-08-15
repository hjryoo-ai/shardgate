package token

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/hjr/shardgate/internal/config"
)

func testIssuer(t *testing.T, now func() time.Time) *Issuer {
	t.Helper()
	i, err := NewIssuer(config.Token{
		SigningKey: config.NewSecret([]byte("test-signing-key-32-bytes-long!!")),
		Issuer:     "shardgate",
		QueueTTL:   2 * time.Hour,
		EntryTTL:   5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if now != nil {
		i.WithClock(now)
	}
	return i
}

func sampleClaims(kind Kind) Claims {
	return Claims{
		Kind: kind, EventID: "evt1", TokenID: "tok_abc", JTI: "jti_xyz",
		Shard: "s0042", FPHash: "fp_hash_1", IPPrefix: "203.0.113.0/24",
	}
}

func TestNewIssuerValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Token
	}{
		{"서명키 없음", config.Token{QueueTTL: time.Hour, EntryTTL: time.Minute}},
		{"큐 TTL 0", config.Token{SigningKey: config.NewSecret([]byte("k")), EntryTTL: time.Minute}},
		{"입장 TTL 0", config.Token{SigningKey: config.NewSecret([]byte("k")), QueueTTL: time.Hour}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewIssuer(tc.cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestIssueAndVerifyRoundTrip(t *testing.T) {
	i := testIssuer(t, nil)

	for _, kind := range []Kind{KindQueue, KindEntry} {
		t.Run(string(kind), func(t *testing.T) {
			in := sampleClaims(kind)
			raw, issued, err := i.Issue(in)
			if err != nil {
				t.Fatalf("Issue: %v", err)
			}
			if issued.ExpiresAt.Sub(issued.IssuedAt) != i.TTL(kind) {
				t.Fatalf("ttl = %v, want %v", issued.ExpiresAt.Sub(issued.IssuedAt), i.TTL(kind))
			}

			out, err := i.Verify(raw, kind, "evt1")
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if out.TokenID != in.TokenID || out.Shard != in.Shard || out.JTI != in.JTI {
				t.Fatalf("claims round trip lost data: %+v", out)
			}
			if out.FPHash != in.FPHash || out.IPPrefix != in.IPPrefix {
				t.Fatalf("binding claims lost: %+v", out)
			}
		})
	}
}

func TestIssueRejectsIncompleteClaims(t *testing.T) {
	i := testIssuer(t, nil)

	tests := []struct {
		name    string
		mutate  func(*Claims)
		wantErr error
	}{
		{"이벤트 없음", func(c *Claims) { c.EventID = "" }, ErrIncomplete},
		{"토큰 ID 없음", func(c *Claims) { c.TokenID = "" }, ErrIncomplete},
		{"jti 없음", func(c *Claims) { c.JTI = "" }, ErrIncomplete},
		{"샤드 없음", func(c *Claims) { c.Shard = "" }, ErrIncomplete},
		{"알 수 없는 종류", func(c *Claims) { c.Kind = "admin" }, ErrKind},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := sampleClaims(KindQueue)
			tc.mutate(&c)
			if _, _, err := i.Issue(c); !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// 토큰 검증이 뚫리면 그 아래의 모든 방어가 의미를 잃는다(불변식 2).
func TestVerifyRejectsTamperedTokens(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	i := testIssuer(t, func() time.Time { return now })

	valid, _, err := i.Issue(sampleClaims(KindQueue))
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// 다른 키로 서명된 토큰
	other := testIssuer(t, func() time.Time { return now })
	other.key = []byte("a-completely-different-signing-key")
	forged, _, err := other.Issue(sampleClaims(KindQueue))
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// alg=none 으로 서명을 통째로 없앤 토큰
	noneTok, err := jwt.NewWithClaims(jwt.SigningMethodNone, registered{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "tok_abc", ID: "jti_xyz",
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
		Kind: KindQueue, EventID: "evt1", TokenID: "tok_abc", Shard: "s0042",
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("build none token: %v", err)
	}

	tests := []struct {
		name    string
		raw     string
		kind    Kind
		event   string
		wantErr error
	}{
		{"정상", valid, KindQueue, "evt1", nil},
		{"서명 위조", forged, KindQueue, "evt1", ErrSignature},
		{"alg=none", noneTok, KindQueue, "evt1", ErrSignature},
		{"페이로드 변조", tamper(valid), KindQueue, "evt1", ErrSignature},
		{"빈 문자열", "", KindQueue, "evt1", ErrMalformed},
		{"JWT 가 아님", "not.a.jwt", KindQueue, "evt1", ErrMalformed},
		{"종류 혼용 — 큐 토큰으로 구매", valid, KindEntry, "evt1", ErrKind},
		{"다른 이벤트", valid, KindQueue, "evt2", ErrEvent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := i.Verify(tc.raw, tc.kind, tc.event)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyRejectsExpired(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	i := testIssuer(t, func() time.Time { return now })

	raw, _, err := i.Issue(sampleClaims(KindEntry)) // TTL 5분
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := i.Verify(raw, KindEntry, "evt1"); err != nil {
		t.Fatalf("fresh token rejected: %v", err)
	}

	now = now.Add(6 * time.Minute)
	if _, err := i.Verify(raw, KindEntry, "evt1"); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

// 바인딩은 탈취·공유를 어렵게 만들되, 값이 없는 클라이언트를 일괄 거절하지는 않는다.
func TestVerifyBound(t *testing.T) {
	i := testIssuer(t, nil)

	bound, _, err := i.Issue(sampleClaims(KindQueue))
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	unbound := sampleClaims(KindQueue)
	unbound.FPHash, unbound.IPPrefix = "", ""
	noBinding, _, err := i.Issue(unbound)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	tests := []struct {
		name    string
		raw     string
		b       Binding
		wantErr error
	}{
		{"일치", bound, Binding{FPHash: "fp_hash_1", IPPrefix: "203.0.113.0/24"}, nil},
		{"지문 불일치", bound, Binding{FPHash: "fp_other", IPPrefix: "203.0.113.0/24"}, ErrFPMismatch},
		{"네트워크 불일치", bound, Binding{FPHash: "fp_hash_1", IPPrefix: "198.51.100.0/24"}, ErrIPMismatch},
		{"요청에 지문 없음 — 검사 생략", bound, Binding{IPPrefix: "203.0.113.0/24"}, nil},
		{"토큰에 바인딩 없음 — 검사 생략", noBinding, Binding{FPHash: "fp_x", IPPrefix: "198.51.100.0/24"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := i.VerifyBound(tc.raw, KindQueue, "evt1", tc.b)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestReason(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{ErrExpired, "expired"},
		{ErrSignature, "bad_signature"},
		{ErrKind, "wrong_kind"},
		{ErrEvent, "wrong_event"},
		{ErrFPMismatch, "fp_mismatch"},
		{ErrIPMismatch, "ip_mismatch"},
		{ErrIncomplete, "incomplete"},
		{errors.New("boom"), "malformed"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := Reason(tc.err); got != tc.want {
				t.Fatalf("= %q, want %q", got, tc.want)
			}
		})
	}
}

// 토큰 ID 는 Redis 키에 그대로 들어간다. 키 스키마를 흔드는 문자가 나오면 안 된다.
func TestNewIDIsKeySafeAndUnique(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for range 1000 {
		id, err := NewID()
		if err != nil {
			t.Fatalf("NewID: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true

		if strings.ContainsAny(id, "{}:/+= ") {
			t.Fatalf("id %q contains a character that breaks the key schema", id)
		}
		if _, err := base64.RawURLEncoding.DecodeString(id); err != nil {
			t.Fatalf("id %q is not base64url: %v", id, err)
		}
	}
}

// tamper 는 서명은 그대로 두고 페이로드만 바꾼다.
func tamper(raw string) string {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return raw
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return raw
	}
	swapped := strings.Replace(string(payload), `"s0042"`, `"s0001"`, 1)
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(swapped))
	return strings.Join(parts, ".")
}
