package shard

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/hjr/shardgate/internal/config"
)

const testSalt = "shardgate-test-event-salt"

func newTestAssigner(t *testing.T, count, max int) *Assigner {
	t.Helper()
	a, err := NewAssigner(config.NewSecret([]byte(testSalt)), count, max)
	if err != nil {
		t.Fatalf("NewAssigner: %v", err)
	}
	return a
}

func TestNewAssignerValidation(t *testing.T) {
	tests := []struct {
		name    string
		salt    config.Secret
		count   int
		max     int
		wantErr error
	}{
		{"정상", config.NewSecret([]byte(testSalt)), 16, 4096, nil},
		{"salt 없음", config.Secret{}, 16, 4096, ErrNoSalt},
		{"샤드 수 0", config.NewSecret([]byte(testSalt)), 0, 4096, ErrBadCount},
		{"음수", config.NewSecret([]byte(testSalt)), -1, 4096, ErrBadCount},
		{"상한 초과", config.NewSecret([]byte(testSalt)), 10, 4, ErrBadCount},
		{"상한 0", config.NewSecret([]byte(testSalt)), 1, 0, ErrBadCount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewAssigner(tc.salt, tc.count, tc.max)
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

// 같은 토큰은 언제나 같은 샤드로 간다. 그렇지 않으면 사용자가 재접속할 때마다
// 자기 순번이 있는 샤드를 잃는다.
func TestAssignIsDeterministic(t *testing.T) {
	a := newTestAssigner(t, 64, 4096)
	for i := range 500 {
		tok := "tok_" + strconv.Itoa(i)
		first := a.Assign(tok)
		for range 5 {
			if got := a.Assign(tok); got != first {
				t.Fatalf("%s: %q then %q", tok, first, got)
			}
		}
	}
}

// salt 가 다르면 배정도 달라져야 한다. 배정이 salt 와 무관하다면 봇은 오픈 전에
// 샤드 배치를 미리 계산할 수 있다(§3.1).
func TestAssignDependsOnSalt(t *testing.T) {
	a := newTestAssigner(t, 64, 4096)
	b, err := NewAssigner(config.NewSecret([]byte("another-event-salt")), 64, 4096)
	if err != nil {
		t.Fatalf("NewAssigner: %v", err)
	}

	const n = 200
	same := 0
	for i := range n {
		tok := "tok_" + strconv.Itoa(i)
		if a.Assign(tok) == b.Assign(tok) {
			same++
		}
	}
	// 64 샤드면 우연히 겹칠 확률은 1/64 ≈ 3%. 10% 를 넘으면 salt 가 안 먹고 있는 것이다.
	if same > n/10 {
		t.Fatalf("%d/%d tokens landed on the same shard across salts", same, n)
	}
}

// 배정이 한쪽으로 쏠리면 "샤드 = 표본 집단"이라는 전제(§4-L5)가 흔들린다.
func TestAssignDistribution(t *testing.T) {
	const (
		shards  = 16
		tokens  = 100_000
		tolerne = 0.15
	)
	a := newTestAssigner(t, shards, 4096)

	counts := make([]int, shards)
	for i := range tokens {
		idx := a.Index("tok_" + strconv.Itoa(i))
		if idx < 0 || idx >= shards {
			t.Fatalf("index %d out of range", idx)
		}
		counts[idx]++
	}

	expected := float64(tokens) / shards
	for i, c := range counts {
		dev := (float64(c) - expected) / expected
		if dev < -tolerne || dev > tolerne {
			t.Errorf("shard %d got %d (%.1f%% off expected %.0f)", i, c, dev*100, expected)
		}
	}
}

func TestGrow(t *testing.T) {
	tests := []struct {
		name      string
		start     int
		grow      []int
		wantCount int
		wantErr   error
	}{
		{"증가", 4, []int{8}, 8, nil},
		{"단조 — 더 작은 값은 무시", 8, []int{4}, 8, nil},
		{"여러 번", 2, []int{4, 3, 16}, 16, nil},
		{"상한 초과는 거절", 4, []int{99}, 4, ErrNoCapacity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAssigner(t, tc.start, 32)
			var err error
			for _, g := range tc.grow {
				if _, e := a.Grow(g); e != nil {
					err = e
				}
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if got := a.Count(); got != tc.wantCount {
				t.Fatalf("count = %d, want %d", got, tc.wantCount)
			}
		})
	}
}

func TestEnsureCapacity(t *testing.T) {
	tests := []struct {
		name      string
		start     int
		max       int
		joined    int
		shardSize int
		wantCount int
		wantErr   error
	}{
		{"여유 있으면 그대로", 16, 4096, 1_000, 1000, 16, nil},
		{"모자라면 확장", 4, 4096, 10_000, 1000, 10, nil},
		{"나머지는 올림", 4, 4096, 10_001, 1000, 11, nil},
		{"0명이면 최소 1", 1, 4096, 0, 1000, 1, nil},
		{"상한까지만 늘린다", 4, 8, 100_000, 1000, 8, ErrNoCapacity},
		{"샤드 크기 0 은 오류", 4, 8, 100, 0, 4, ErrBadCount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAssigner(t, tc.start, tc.max)
			n, err := a.EnsureCapacity(tc.joined, tc.shardSize)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if n != tc.wantCount {
				t.Fatalf("returned count = %d, want %d", n, tc.wantCount)
			}
			if got := a.Count(); got != tc.wantCount {
				t.Fatalf("count = %d, want %d", got, tc.wantCount)
			}
		})
	}
}

// 확장해도 이미 배정된 사용자는 재배치하지 않는다. 그래서 확장 전에 발급된 토큰의
// 샤드는 토큰 쪽에 저장된 값이 진실이고, 새 N 은 이후 토큰에만 적용된다(§3.1).
func TestGrowDoesNotRebindIssuedTokens(t *testing.T) {
	a := newTestAssigner(t, 4, 4096)

	issued := make(map[string]string)
	for i := range 100 {
		tok := "tok_" + strconv.Itoa(i)
		issued[tok] = a.Assign(tok)
	}

	if _, err := a.Grow(64); err != nil {
		t.Fatalf("Grow: %v", err)
	}

	// 새 배정은 달라질 수 있다 — 그게 확장의 목적이다.
	changed := 0
	for tok, before := range issued {
		if a.Assign(tok) != before {
			changed++
		}
	}
	if changed == 0 {
		t.Fatal("growing N did not change assignment for any token; N is being ignored")
	}
	// 하지만 발급된 토큰이 들고 있는 값 자체는 그대로다 — 순번은 그 값으로 찾는다.
	for tok, before := range issued {
		if before == "" || Validate(before) != nil {
			t.Fatalf("issued shard for %s is unusable: %q", tok, before)
		}
	}
}

func TestIDFormat(t *testing.T) {
	tests := []struct {
		n         int
		want      string
		wantGrey  string
		roundTrip bool
	}{
		{0, "s0000", "g0000", true},
		{7, "s0007", "g0007", true},
		{42, "s0042", "g0042", true},
		{4095, "s4095", "g4095", true},
		{10000, "s10000", "g10000", true},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := ID(tc.n); got != tc.want {
				t.Fatalf("ID = %q, want %q", got, tc.want)
			}
			if got := GreylistID(tc.n); got != tc.wantGrey {
				t.Fatalf("GreylistID = %q, want %q", got, tc.wantGrey)
			}
			idx, grey, err := ParseID(tc.want)
			if err != nil || idx != tc.n || grey {
				t.Fatalf("ParseID(%q) = (%d,%v,%v)", tc.want, idx, grey, err)
			}
			idx, grey, err = ParseID(tc.wantGrey)
			if err != nil || idx != tc.n || !grey {
				t.Fatalf("ParseID(%q) = (%d,%v,%v)", tc.wantGrey, idx, grey, err)
			}
		})
	}
}

// 샤드 ID 는 Redis 해시태그 안에 그대로 들어간다. 형식을 벗어난 값을 통과시키면
// 다른 샤드의 키를 가리키게 만드는 입력이 가능해진다(§3.3).
func TestValidateRejectsKeyInjection(t *testing.T) {
	tests := []struct {
		name string
		id   string
		ok   bool
	}{
		{"일반 샤드", "s0042", true},
		{"greylist 샤드", "g0042", true},
		{"빈 문자열", "", false},
		{"접두사만", "s", false},
		{"알 수 없는 접두사", "x0042", false},
		{"태그 닫기 시도", "s0042}", false},
		{"태그 열기 시도", "s00{42", false},
		{"구분자 주입", "s0042:evt2", false},
		{"다른 이벤트 태그", "s0042}:queue:{evt2:s0001", false},
		{"공백", "s 42", false},
		{"음수", "s-1", false},
		{"자릿수 초과", "s" + strings.Repeat("9", 10), false},
		{"유니코드", "s００４２", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.id)
			if tc.ok && err != nil {
				t.Fatalf("Validate(%q) = %v, want nil", tc.id, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Validate(%q) accepted an invalid id", tc.id)
			}
		})
	}
}

func TestGreylistRoundTrip(t *testing.T) {
	orig := ID(42)

	grey, err := Greylist(orig)
	if err != nil {
		t.Fatalf("Greylist: %v", err)
	}
	if !IsGreylist(grey) {
		t.Fatalf("%q not recognised as greylist", grey)
	}
	if IsGreylist(orig) {
		t.Fatalf("%q wrongly recognised as greylist", orig)
	}

	back, err := Origin(grey)
	if err != nil {
		t.Fatalf("Origin: %v", err)
	}
	if back != orig {
		t.Fatalf("round trip = %q, want %q", back, orig)
	}

	if _, err := Greylist("bogus"); err == nil {
		t.Fatal("Greylist accepted an invalid id")
	}
	if _, err := Origin("bogus"); err == nil {
		t.Fatal("Origin accepted an invalid id")
	}
}

// event_salt 는 절대 로그에 남기지 않는다(CLAUDE.md, §3.1).
func TestAssignerNeverLeaksSalt(t *testing.T) {
	a := newTestAssigner(t, 16, 4096)

	tests := []struct {
		name   string
		render func() string
	}{
		{"String", a.String},
		{"fmt %v", func() string { return fmt.Sprintf("%v", a) }},
		{"fmt %s", func() string { var v any = a; return fmt.Sprintf("%s", v) }},
		{"fmt %+v", func() string { return fmt.Sprintf("%+v", a) }},
		{"fmt %#v", func() string { return fmt.Sprintf("%#v", a) }},
		{"struct field", func() string {
			return fmt.Sprintf("%+v", struct {
				A *Assigner
				N int
			}{a, 1})
		}},
		{"slog", func() string {
			var buf bytes.Buffer
			slog.New(slog.NewJSONHandler(&buf, nil)).Info("assign", slog.Any("assigner", a))
			return buf.String()
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.render()
			if strings.Contains(out, testSalt) {
				t.Fatalf("salt leaked: %s", out)
			}
			// hex 로 인코딩해 흘리는 경로도 막혀 있어야 한다.
			if strings.Contains(out, fmt.Sprintf("%x", testSalt)) {
				t.Fatalf("salt leaked as hex: %s", out)
			}
		})
	}
}

func BenchmarkAssign(b *testing.B) {
	a, err := NewAssigner(config.NewSecret([]byte(testSalt)), 1024, 4096)
	if err != nil {
		b.Fatal(err)
	}
	tokens := make([]string, 1024)
	for i := range tokens {
		tokens[i] = "tok_" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_ = a.Assign(tokens[i%len(tokens)])
	}
}
