package challenge

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/config"
)

func testConfig() config.Challenge {
	return config.Challenge{
		TTL:            2 * time.Minute,
		BaseDifficulty: 8,
		MaxDifficulty:  20,
		NonceBytes:     16,
		HMACKey:        config.NewSecret([]byte("challenge-hmac-key-for-tests")),
	}
}

// newIssuer 는 Redis 없이 발급기를 만든다.
// Verify 는 모든 검사를 통과한 뒤에야 소각(Redis)에 들어가므로,
// 실패 경로만 보는 테스트는 클라이언트가 없어도 안전하다.
func newIssuer(t *testing.T, now func() time.Time) *Issuer {
	t.Helper()
	i, err := NewIssuer(nil, "evt1", testConfig(), nil)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if now != nil {
		i.WithClock(now)
	}
	return i
}

func TestNewIssuerValidation(t *testing.T) {
	tests := []struct {
		name  string
		event string
		cfg   config.Challenge
	}{
		{"HMAC 키 없음", "evt1", config.Challenge{TTL: time.Minute, BaseDifficulty: 8, MaxDifficulty: 20, NonceBytes: 16}},
		{"이벤트 없음", "", testConfig()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewIssuer(nil, tc.event, tc.cfg, nil); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLeadingZeroBits(t *testing.T) {
	tests := []struct {
		name  string
		first []byte
		want  int
	}{
		{"첫 비트가 1", []byte{0xff}, 0},
		{"0x7f", []byte{0x7f}, 1},
		{"0x0f", []byte{0x0f}, 4},
		{"0x01", []byte{0x01}, 7},
		{"한 바이트 전부 0", []byte{0x00, 0x80}, 8},
		{"12비트", []byte{0x00, 0x0f}, 12},
		{"두 바이트 전부 0", []byte{0x00, 0x00, 0xff}, 16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var d [sha256.Size]byte
			copy(d[:], tc.first)
			if got := LeadingZeroBits(d); got != tc.want {
				t.Fatalf("= %d, want %d", got, tc.want)
			}
		})
	}

	var allZero [sha256.Size]byte
	if got := LeadingZeroBits(allZero); got != sha256.Size*8 {
		t.Fatalf("all-zero digest = %d, want %d", got, sha256.Size*8)
	}
}

func TestSolveProducesAValidSolution(t *testing.T) {
	tests := []int{1, 4, 8, 12}
	for _, difficulty := range tests {
		t.Run(strings.Repeat("*", difficulty), func(t *testing.T) {
			sol, ok := Solve("test-nonce", difficulty, 1<<22)
			if !ok {
				t.Fatalf("could not solve difficulty %d", difficulty)
			}
			if !Check("test-nonce", sol, difficulty) {
				t.Fatalf("Solve returned an invalid solution %q", sol)
			}
			// nonce 가 다르면 같은 풀이가 통하지 않아야 한다 — 아니면 답 하나를 재활용할 수 있다.
			if Check("other-nonce", sol, difficulty) && difficulty >= 8 {
				t.Fatalf("solution %q also solves a different nonce", sol)
			}
		})
	}
}

func TestIssueAppliesDifficultyBounds(t *testing.T) {
	tests := []struct {
		name     string
		provider DifficultyProvider
		want     int
	}{
		{"기본", nil, 8},
		{"상향", Fixed(15), 15},
		{"기본보다 낮으면 기본으로", Fixed(2), 8},
		{"상한 초과는 상한으로", Fixed(99), 20},
		{"오류 시 기본값", failingProvider{}, 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			i, err := NewIssuer(nil, "evt1", testConfig(), tc.provider)
			if err != nil {
				t.Fatalf("NewIssuer: %v", err)
			}
			c, err := i.Issue(context.Background(), Subject{})
			if err != nil {
				t.Fatalf("Issue: %v", err)
			}
			if c.Difficulty != tc.want {
				t.Fatalf("difficulty = %d, want %d", c.Difficulty, tc.want)
			}
			if c.Algorithm != Algorithm {
				t.Fatalf("algorithm = %q", c.Algorithm)
			}
			if c.Nonce == "" || c.Signature == "" {
				t.Fatalf("challenge is incomplete: %+v", c)
			}
		})
	}
}

type failingProvider struct{}

func (failingProvider) Difficulty(context.Context, Subject) (int, error) {
	return 99, errors.New("suspicion lookup failed")
}

// 스코어러가 죽어도 대기실은 열려 있어야 한다(불변식 5).
func TestIssueSurvivesProviderFailure(t *testing.T) {
	i, err := NewIssuer(nil, "evt1", testConfig(), failingProvider{})
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	if _, err := i.Issue(context.Background(), Subject{}); err != nil {
		t.Fatalf("Issue failed when the difficulty provider was down: %v", err)
	}
}

func TestNonceIsUnpredictable(t *testing.T) {
	i := newIssuer(t, nil)
	seen := make(map[string]bool, 500)
	for range 500 {
		c, err := i.Issue(context.Background(), Subject{})
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if seen[c.Nonce] {
			t.Fatalf("duplicate nonce %q", c.Nonce)
		}
		seen[c.Nonce] = true
	}
}

// 챌린지 검증이 뚫리면 진입 비용을 올린다는 §4-L2 의 전제가 통째로 무너진다.
func TestVerifyRejectsTamperedChallenges(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	i := newIssuer(t, func() time.Time { return now })

	issued, err := i.Issue(context.Background(), Subject{})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	solution, ok := Solve(issued.Nonce, issued.Difficulty, 1<<20)
	if !ok {
		t.Fatal("could not solve the challenge")
	}

	tests := []struct {
		name     string
		mutate   func(*Challenge)
		solution string
		wantErr  error
	}{
		{
			// 서명이 난이도를 덮고 있지 않으면 이 한 줄로 PoW 전체가 무의미해진다.
			name:     "난이도 하향 변조",
			mutate:   func(c *Challenge) { c.Difficulty = 1 },
			solution: solution,
			wantErr:  ErrSignature,
		},
		{
			name:     "만료 연장 변조",
			mutate:   func(c *Challenge) { c.ExpiresAt += int64(time.Hour / time.Millisecond) },
			solution: solution,
			wantErr:  ErrSignature,
		},
		{
			name:     "nonce 바꿔치기",
			mutate:   func(c *Challenge) { c.Nonce = "attacker-picked-nonce" },
			solution: solution,
			wantErr:  ErrSignature,
		},
		{
			name:     "서명 위조",
			mutate:   func(c *Challenge) { c.Signature = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
			solution: solution,
			wantErr:  ErrSignature,
		},
		{
			name:     "풀이 없음",
			mutate:   func(*Challenge) {},
			solution: "",
			wantErr:  ErrMalformed,
		},
		{
			name:     "틀린 풀이",
			mutate:   func(*Challenge) {},
			solution: "definitely-not-the-answer",
			wantErr:  ErrSolution,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := issued
			tc.mutate(&c)
			err := i.Verify(context.Background(), c, tc.solution)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyRejectsExpired(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	i := newIssuer(t, func() time.Time { return now })

	c, err := i.Issue(context.Background(), Subject{})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	solution, ok := Solve(c.Nonce, c.Difficulty, 1<<20)
	if !ok {
		t.Fatal("could not solve the challenge")
	}

	now = now.Add(3 * time.Minute) // TTL 2분 초과
	if err := i.Verify(context.Background(), c, solution); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

// 다른 이벤트에서 발급된 챌린지는 통하지 않아야 한다 — 서명에 event_id 가 들어 있다.
func TestChallengeIsScopedToItsEvent(t *testing.T) {
	a := newIssuer(t, nil)

	b, err := NewIssuer(nil, "evt2", testConfig(), nil)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}

	c, err := a.Issue(context.Background(), Subject{})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	solution, ok := Solve(c.Nonce, c.Difficulty, 1<<20)
	if !ok {
		t.Fatal("could not solve the challenge")
	}

	if err := b.Verify(context.Background(), c, solution); !errors.Is(err, ErrSignature) {
		t.Fatalf("err = %v, want ErrSignature", err)
	}
}

func TestReason(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{ErrSignature, "bad_signature"},
		{ErrExpired, "expired"},
		{ErrDifficulty, "bad_difficulty"},
		{ErrSolution, "bad_solution"},
		{ErrReplay, "replay"},
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

// 난이도가 1비트 오를 때마다 기대 시도 횟수는 두 배가 된다.
// 이게 §4-L2 의 "봇팜은 1만 배 비용"이 성립하는 근거다.
func BenchmarkSolve(b *testing.B) {
	for _, difficulty := range []int{8, 12, 16} {
		b.Run(strings.Repeat("b", difficulty), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				if _, ok := Solve("bench-nonce-"+strings.Repeat("x", i%8), difficulty, 1<<24); !ok {
					b.Fatal("unsolved")
				}
			}
		})
	}
}
