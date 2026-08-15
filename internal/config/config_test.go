package config

import (
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := LoadFrom(envFrom(nil), "gate", ":8080")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"http addr", cfg.Service.HTTPAddr, ":8080"},
		{"shard size", cfg.Event.ShardSize, DefaultShardSize},
		{"lottery window", cfg.Event.LotteryWindow, DefaultLotteryWindow},
		{"admit rate", cfg.Admission.RatePerMin, DefaultAdmitRatePerMin},
		{"greylist threshold", cfg.BotScore.Greylist, DefaultScoreGreylist},
		{"hold threshold", cfg.BotScore.Hold, DefaultScoreHold},
		{"block threshold", cfg.BotScore.Block, DefaultScoreBlock},
		{"entry ttl", cfg.Token.EntryTTL, DefaultEntryTokenTTL},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// 기본 상태에서 비밀 값은 비어 있고, 필요할 때 RequireSecrets 가 걸러야 한다.
	if !cfg.Event.Salt.IsZero() {
		t.Error("event salt should be empty by default")
	}
	if err := cfg.RequireSecrets("event_salt"); err == nil {
		t.Error("RequireSecrets should fail when salt is unset")
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := LoadFrom(envFrom(map[string]string{
		"SG_SHARD_SIZE":         "500",
		"SG_LOTTERY_WINDOW":     "90s",
		"SG_ADMIT_RATE_PER_MIN": "6000",
		"SG_REDIS_ADDRS":        "a:6379, b:6379 ,",
		"SG_EVENT_SALT":         "00112233",
		"SG_SCORE_GREYLIST":     "30",
		// 통과 점수는 greylist 임계보다 낮아야 한다(아니면 통과 즉시 재격리).
		"SG_RECHALLENGE_PASS_SCORE": "25",
		"SG_EVENT_OPEN_AT":          "2026-08-10T12:00:00Z",
		// 이름이 닿는지까지 본다 — compose 든 여기든, 이름이 빠지면 팔이 조용히
		// 기본값으로 돌고 아무것도 실패하지 않는다(ROADMAP 결함 6·8).
		"SG_SCORE_ROBUST_BASELINE": "true",
	}), "queue", ":8081")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Event.ShardSize != 500 {
		t.Errorf("shard size = %d", cfg.Event.ShardSize)
	}
	if cfg.Event.LotteryWindow != 90*time.Second {
		t.Errorf("lottery window = %v", cfg.Event.LotteryWindow)
	}
	if cfg.Admission.RatePerMin != 6000 {
		t.Errorf("admit rate = %d", cfg.Admission.RatePerMin)
	}
	if got, want := strings.Join(cfg.Redis.Addrs, "|"), "a:6379|b:6379"; got != want {
		t.Errorf("redis addrs = %q, want %q", got, want)
	}
	if cfg.Event.Salt.Len() != 4 {
		t.Errorf("salt len = %d, want 4", cfg.Event.Salt.Len())
	}
	if cfg.BotScore.Greylist != 30 {
		t.Errorf("greylist = %d", cfg.BotScore.Greylist)
	}
	if !cfg.Event.OpenAt.Equal(time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("open at = %v", cfg.Event.OpenAt)
	}
	if !cfg.BotScore.RobustBaseline {
		t.Error("robust baseline = false — 이름이 로더에 닿지 않았다")
	}
}

// EffectiveEnv 는 측정 하네스가 "팔이 실제로 적용됐는가"를 대조하는 근거다.
// 세 성질이 동시에 성립해야 쓸모가 있다: 준 값이 그대로 있을 것, 안 준 값은 없을 것,
// 비밀은 가려질 것. 두 번째가 빠지면 기본값이 섞여 대조가 무의미해지고, 세 번째가
// 빠지면 로그로 salt 가 샌다.
func TestEffectiveEnvRecordsWhatWasApplied(t *testing.T) {
	cfg, err := LoadFrom(envFrom(map[string]string{
		"SG_SHARD_SIZE":     "500",
		"SG_EVENT_SALT":     "00112233",
		"SG_REDIS_PASSWORD": "hunter2",
	}), "queue", ":8081")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	eff := cfg.EffectiveEnv()

	if eff["SG_SHARD_SIZE"] != "500" {
		t.Errorf("SG_SHARD_SIZE = %q, want 500", eff["SG_SHARD_SIZE"])
	}
	// 기본값으로 결정된 항목은 들어오면 안 된다 — 하네스가 확인하는 것은
	// "내가 준 값이 닿았는가"이지 "값이 무엇인가"가 아니다.
	if v, ok := eff["SG_SHARD_COUNT"]; ok {
		t.Errorf("주지 않은 SG_SHARD_COUNT 가 %q 로 들어 있다", v)
	}
	for _, k := range []string{"SG_EVENT_SALT", "SG_REDIS_PASSWORD"} {
		if eff[k] != "<redacted>" {
			t.Errorf("%s = %q — 비밀 값이 가려지지 않았다", k, eff[k])
		}
	}
	for k, v := range eff {
		if v == "hunter2" || v == "00112233" {
			t.Errorf("%s 에 원본 비밀 값이 남았다", k)
		}
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{
			name:    "정수가 아닌 샤드 크기",
			env:     map[string]string{"SG_SHARD_SIZE": "many"},
			wantMsg: "SG_SHARD_SIZE",
		},
		{
			name:    "duration 형식 오류",
			env:     map[string]string{"SG_ADMIT_INTERVAL": "5"},
			wantMsg: "SG_ADMIT_INTERVAL",
		},
		{
			name:    "샤드 크기 0",
			env:     map[string]string{"SG_SHARD_SIZE": "0"},
			wantMsg: "SHARD_SIZE must be > 0",
		},
		{
			// 조치 파이프라인이 단조 증가하지 않으면 "단계적 격리"가 성립하지 않는다(§4).
			name:    "임계값 역전",
			env:     map[string]string{"SG_SCORE_GREYLIST": "80", "SG_SCORE_HOLD": "50"},
			wantMsg: "score thresholds",
		},
		{
			name:    "차단 임계값이 100 초과",
			env:     map[string]string{"SG_SCORE_BLOCK": "120"},
			wantMsg: "score thresholds",
		},
		{
			name:    "가중치 합이 1이 아님",
			env:     map[string]string{"SG_WEIGHT_HEARTBEAT": "0.9"},
			wantMsg: "weights must sum to 1.0",
		},
		{
			name:    "PoW 최대 난이도가 기본 난이도보다 작음",
			env:     map[string]string{"SG_POW_BASE_DIFFICULTY": "20", "SG_POW_MAX_DIFFICULTY": "10"},
			wantMsg: "POW_MAX_DIFFICULTY",
		},
		{
			// 통과 점수가 임계 이상이면 통과해도 다음 창에서 즉시 재격리된다.
			// 재챌린지가 있는데 아무 효과도 없는 상태이므로 기동을 거부한다.
			name:    "재챌린지 통과 점수가 greylist 임계 이상",
			env:     map[string]string{"SG_RECHALLENGE_PASS_SCORE": "40"},
			wantMsg: "RECHALLENGE_PASS_SCORE",
		},
		{
			// 0 회는 greylist 를 다시 출구 없는 종점으로 만든다(REPORT §3.5).
			name:    "재챌린지 횟수 0",
			env:     map[string]string{"SG_RECHALLENGE_MAX_ATTEMPTS": "0"},
			wantMsg: "RECHALLENGE_MAX_ATTEMPTS",
		},
		{
			name:    "salt 가 hex 가 아님",
			env:     map[string]string{"SG_EVENT_SALT": "nothex!"},
			wantMsg: "SG_EVENT_SALT",
		},
		{
			// 오픈 시각이 없으면 추첨 구간이 없고, 게이트는 조용히 무효가 된다.
			// 켠 사람은 켜졌다고 믿으므로 기동을 거부하는 편이 안전하다.
			name:    "게이트만 켜고 오픈 시각을 안 줌",
			env:     map[string]string{"SG_ADMIT_AFTER_LOTTERY": "true"},
			wantMsg: "ADMIT_AFTER_LOTTERY requires EVENT_OPEN_AT",
		},
		{
			// 관찰이 토큰 수명보다 길면 게이트가 열리기 전에 토큰이 죽는다.
			// 아무도 입장하지 못하는데 오류는 한 건도 나지 않는 종류의 고장이다.
			name:    "관찰 시간이 큐 토큰 수명보다 김",
			env:     map[string]string{"SG_ADMIT_MIN_DWELL": "1h", "SG_QUEUE_TOKEN_TTL": "30m"},
			wantMsg: "ADMIT_MIN_DWELL",
		},
		{
			name:    "관찰 신호 수가 음수",
			env:     map[string]string{"SG_ADMIT_MIN_BEATS": "-1"},
			wantMsg: "ADMIT_MIN_BEATS",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFrom(envFrom(tc.env), "svc", ":0")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestWeightsSum(t *testing.T) {
	cfg, err := LoadFrom(envFrom(nil), "scorer", ":0")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if sum := cfg.BotScore.Weights.Sum(); sum < 1-weightEpsilon || sum > 1+weightEpsilon {
		t.Fatalf("default weights sum = %v, want 1.0", sum)
	}
}
