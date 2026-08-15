package queue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"
)

func TestNewValidatesEventID(t *testing.T) {
	tests := []struct {
		name    string
		event   string
		batch   int
		wantErr error
	}{
		{"정상", "evt1", 256, nil},
		{"빈 이벤트", "", 256, ErrInvalidEvent},
		{"해시태그 주입", "evt1}:queue:{evt2", 256, ErrInvalidEvent},
		{"구분자 주입", "evt1:s0001", 256, ErrInvalidEvent},
		{"스윕 배치 0", "evt1", 0, errors.New("sweep batch")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(nil, Config{EventID: tc.event, SweepBatch: tc.batch}, nil, nil)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr == nil:
			case err == nil:
				t.Fatal("expected error, got nil")
			case !strings.Contains(err.Error(), strings.TrimPrefix(tc.wantErr.Error(), "queue: ")):
				t.Fatalf("err = %v, want something like %v", err, tc.wantErr)
			}
		})
	}
}

// 토큰 ID 와 샤드 ID 는 Redis 키에 들어간다. 키 스키마를 흔들 수 있는 입력은
// Redis 에 닿기 전에 막아야 한다(§3.3).
func TestCheckRejectsKeyInjection(t *testing.T) {
	s, err := New(nil, Config{EventID: "evt1", SweepBatch: 8}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name    string
		shard   string
		token   string
		wantErr error
	}{
		{"정상", "s0001", "tok_abc-123.x", nil},
		{"빈 토큰", "s0001", "", ErrEmptyToken},
		{"토큰에 태그 주입", "s0001", "tok}:user:{evt1:s0002", ErrInvalidToken},
		{"토큰에 콜론", "s0001", "tok:abc", ErrInvalidToken},
		{"토큰 길이 초과", "s0001", strings.Repeat("a", maxIDLen+1), ErrInvalidToken},
		{"잘못된 샤드", "shard-one", "tok", shard.ErrInvalidID},
		{"빈 샤드", "", "tok", shard.ErrInvalidID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s.check(tc.shard, tc.token)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestLotteryEnd(t *testing.T) {
	open := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		cfg    Config
		want   time.Time
		isZero bool
	}{
		{
			name: "오픈 시각 + 추첨 창",
			cfg:  Config{EventID: "e", SweepBatch: 1, OpenAt: open, LotteryWindow: 2 * time.Minute},
			want: open.Add(2 * time.Minute),
		},
		{
			// 오픈 시각을 모르면 추첨 구간을 열 수 없다 → 전원 FIFO.
			name:   "오픈 시각 미설정",
			cfg:    Config{EventID: "e", SweepBatch: 1, LotteryWindow: 2 * time.Minute},
			isZero: true,
		},
		{
			name: "추첨 창 0",
			cfg:  Config{EventID: "e", SweepBatch: 1, OpenAt: open},
			want: open,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(nil, tc.cfg, nil, nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			got := s.LotteryEnd()
			if tc.isZero {
				if !got.IsZero() {
					t.Fatalf("= %v, want zero", got)
				}
				if milliOrZero(got) != 0 {
					t.Fatalf("zero time should marshal to 0, got %d", milliOrZero(got))
				}
				return
			}
			if !got.Equal(tc.want) {
				t.Fatalf("= %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStaleAfter(t *testing.T) {
	s, err := New(nil, Config{
		EventID: "e", SweepBatch: 1,
		HeartbeatInterval: 5 * time.Second, MissedHeartbeats: 3,
	}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// §5: heartbeat 미수신 3회(약 15초) → soft-evict
	if got, want := s.StaleAfter(), 15*time.Second; got != want {
		t.Fatalf("= %v, want %v", got, want)
	}
}

func TestEstimateWait(t *testing.T) {
	tests := []struct {
		name    string
		snap    Snapshot
		perMin  float64
		want    time.Duration
		wantAhd int64
	}{
		{"맨 앞", Snapshot{Rank: 0}, 60, time.Second, 0},
		{"100번째", Snapshot{Rank: 99}, 60, 100 * time.Second, 99},
		{"느린 배분", Snapshot{Rank: 9}, 10, time.Minute, 9},
		{"큐에 없음", Snapshot{Rank: -1}, 60, 0, 0},
		{"입장률 0", Snapshot{Rank: 10}, 0, 0, 10},
		{"입장률 음수", Snapshot{Rank: 10}, -5, 0, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.snap.EstimateWait(tc.perMin); got != tc.want {
				t.Errorf("EstimateWait = %v, want %v", got, tc.want)
			}
			if got := tc.snap.Ahead(); got != tc.wantAhd {
				t.Errorf("Ahead = %d, want %d", got, tc.wantAhd)
			}
		})
	}
}

// 게이트가 켜지면 예상 대기가 달라져야 한다. 예전 값을 그대로 보여 주면 화면이
// 멈춘 것처럼 보이고, 대기 화면이 유일한 진행 표시인 곳에서 그 오해는 비싸다.
func TestEstimateWaitAt(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	joined := now.Add(-30 * time.Second)

	tests := []struct {
		name  string
		snap  Snapshot
		gates Gates
		want  time.Duration
	}{
		{
			// 게이트가 없으면 EstimateWait 와 같은 값이어야 한다.
			name: "게이트 없음",
			snap: Snapshot{Rank: 59, JoinedAt: joined},
			want: time.Minute,
		},
		{
			// 전체 게이트가 열리기 전에는 아무 자리도 나가지 않는다 → 줄이 그때 시작한다.
			name:  "추첨 게이트: 순번 대기 앞에 더한다",
			snap:  Snapshot{Rank: 59, JoinedAt: joined},
			gates: Gates{AdmitOpensAt: now.Add(2 * time.Minute)},
			want:  3 * time.Minute,
		},
		{
			name:  "추첨 게이트: 이미 열렸으면 영향 없음",
			snap:  Snapshot{Rank: 59, JoinedAt: joined},
			gates: Gates{AdmitOpensAt: now.Add(-time.Second)},
			want:  time.Minute,
		},
		{
			// 관찰 중에도 남은 자리는 계속 나가고 내 순번도 당겨진다 → 늦게 끝나는 쪽.
			name:  "관찰 게이트: 순번보다 늦으면 관찰이 이긴다",
			snap:  Snapshot{Rank: 59, JoinedAt: joined},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  4*time.Minute + 30*time.Second,
		},
		{
			name:  "관찰 게이트: 순번이 더 늦으면 순번이 이긴다",
			snap:  Snapshot{Rank: 599, JoinedAt: joined},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  10 * time.Minute,
		},
		{
			name:  "관찰 게이트: 이미 채웠으면 영향 없음",
			snap:  Snapshot{Rank: 59, JoinedAt: now.Add(-10 * time.Minute)},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  time.Minute,
		},
		{
			// 진입 시각을 모르면 남은 관찰 시간을 계산할 수 없다. 없는 값을 지어내지 않는다.
			name:  "관찰 게이트: 진입 시각을 모르면 반영하지 않는다",
			snap:  Snapshot{Rank: 59},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  time.Minute,
		},
		{
			// 재챌린지로 복귀하면 관찰 시계가 그 시점으로 되감긴다(rechallenge.lua).
			// 진입 시각으로 재면 이 사람은 이미 10분을 채웠으니 "1분 남음"이 되는데,
			// admit.lua 는 되감긴 시계로 재서 열어 주지 않는다. 화면과 서버가 갈라지고,
			// 갈라짐은 언제나 이미 한 번 걸린 사용자 쪽에서만 보인다.
			name:  "관찰 게이트: 복귀하면 되감긴 시계로 잰다",
			snap:  Snapshot{Rank: 59, JoinedAt: now.Add(-10 * time.Minute), ObserveFrom: joined},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  4*time.Minute + 30*time.Second,
		},
		{
			// 둘 다 켜면 각자의 방식으로 합쳐진다: max(관찰 잔여, 게이트 잔여 + 순번 대기).
			name:  "두 게이트 동시",
			snap:  Snapshot{Rank: 59, JoinedAt: joined},
			gates: Gates{AdmitOpensAt: now.Add(2 * time.Minute), MinDwell: 5 * time.Minute},
			want:  4*time.Minute + 30*time.Second,
		},
		{
			// EstimateWait 의 0 은 "모른다"이지 "곧 입장"이 아니다. 게이트를 더해
			// 그 자리를 채우면 큐에 없는 사용자에게 있지도 않은 예상 시간이 생긴다.
			name:  "큐에 없으면 게이트와 무관하게 0",
			snap:  Snapshot{Rank: -1, JoinedAt: joined},
			gates: Gates{MinDwell: 5 * time.Minute},
			want:  0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.snap.EstimateWaitAt(now, 60, tc.gates); got != tc.want {
				t.Errorf("EstimateWaitAt = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTicketQueued(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusCreated, true},
		{StatusExists, true},
		{StatusHeld, false},
		{StatusBlocked, false},
		{StatusAdmitted, false},
		{StatusUnknown, false},
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			if got := (Ticket{Status: tc.status}).Queued(); got != tc.want {
				t.Fatalf("= %v, want %v", got, tc.want)
			}
		})
	}
}

// 추첨 순번은 반드시 추첨 밴드 안에 떨어져야 한다. 밴드를 넘으면 FIFO 구간
// 사용자보다 뒤로 밀려 "먼저 왔는데 더 늦게 들어가는" 역전이 생긴다(§3.2).
func TestLotteryRankStaysInBand(t *testing.T) {
	seen := make(map[int64]int)
	for range 2000 {
		v, err := lotteryRank()
		if err != nil {
			t.Fatalf("lotteryRank: %v", err)
		}
		if v < 0 || v >= LotteryBand {
			t.Fatalf("rank %d out of band [0,%d)", v, LotteryBand)
		}
		seen[v]++
	}
	// 상수를 돌려주고 있지는 않은지 — 추첨이 아니게 된다.
	if len(seen) < 1900 {
		t.Fatalf("only %d distinct ranks out of 2000; randomness looks broken", len(seen))
	}
}

func TestValidID(t *testing.T) {
	tests := []struct {
		in string
		ok bool
	}{
		{"tok_abc", true},
		{"AZaz09-_.", true},
		{"", false},
		{"tok abc", false},
		{"tok{abc}", false},
		{"tok:abc", false},
		{"tok/abc", false},
		{"토큰", false},
		{strings.Repeat("a", maxIDLen), true},
		{strings.Repeat("a", maxIDLen+1), false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := validID(tc.in); got != tc.ok {
				t.Fatalf("validID(%q) = %v, want %v", tc.in, got, tc.ok)
			}
		})
	}
}

func TestReplyParsing(t *testing.T) {
	t.Run("길이 불일치", func(t *testing.T) {
		if _, err := reply([]any{int64(1)}, 3); !errors.Is(err, ErrUnexpectedReply) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("배열 아님", func(t *testing.T) {
		if _, err := reply("nope", 3); !errors.Is(err, ErrUnexpectedReply) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("정상", func(t *testing.T) {
		vals, err := reply([]any{"created", int64(0), int64(1)}, 3)
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		if asString(vals[0]) != "created" || asInt64(vals[1]) != 0 || asInt64(vals[2]) != 1 {
			t.Fatalf("bad parse: %v", vals)
		}
	})
	t.Run("타입 방어", func(t *testing.T) {
		if got := asInt64(struct{}{}); got != -1 {
			t.Errorf("asInt64(unknown) = %d, want -1", got)
		}
		if got := asString(int64(3)); got != "" {
			t.Errorf("asString(int) = %q, want empty", got)
		}
		if got := asInt64("42"); got != 42 {
			t.Errorf("asInt64(%q) = %d, want 42", "42", got)
		}
	})
}

func TestFromMilli(t *testing.T) {
	if got := fromMilli(-1); !got.IsZero() {
		t.Fatalf("fromMilli(-1) = %v, want zero time", got)
	}
	if got := fromMilli(1_754_913_600_000); got.IsZero() {
		t.Fatal("fromMilli lost a real timestamp")
	}
}

// 도메인 API 가 부르는 스크립트는 전부 실제로 임베드돼 있어야 한다.
// 파일명 오타는 init 시점 패닉으로 이어지므로 여기서 먼저 잡는다.
func TestEmbeddedScriptsExist(t *testing.T) {
	want := map[string]bool{
		"enqueue.lua":   false,
		"position.lua":  false,
		"heartbeat.lua": false,
		"evict.lua":     false,
	}
	names, err := lua.Names()
	if err != nil {
		t.Fatalf("Names: %v", err)
	}
	for _, n := range names {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for n, found := range want {
		if !found {
			t.Errorf("%s is not embedded", n)
		}
	}

	for _, s := range []*struct{ name, src string }{
		{scriptEnqueue.Name(), lua.MustRead(scriptEnqueue.Name())},
		{scriptPosition.Name(), lua.MustRead(scriptPosition.Name())},
		{scriptHeartbeat.Name(), lua.MustRead(scriptHeartbeat.Name())},
		{scriptEvict.Name(), lua.MustRead(scriptEvict.Name())},
	} {
		if s.src == "" {
			t.Errorf("%s is empty", s.name)
		}
	}
}
