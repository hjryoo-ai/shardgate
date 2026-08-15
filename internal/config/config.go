// Package config 는 모든 서비스의 설정을 환경변수에서 읽어들인다.
//
// 규칙(CLAUDE.md):
//   - 매직넘버 금지. 샤드 크기, admit rate, 봇 점수 임계값 등 모든 수치는 여기에만 존재한다.
//   - 비밀 값(event_salt, 서명키)은 Secret 타입으로 감싸 로그 유출을 타입 수준에서 막는다.
//
// 환경변수는 전부 `SG_` 접두사를 쓴다.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// 기본값 — DESIGN.md 의 수치를 그대로 옮긴 곳. 여기가 유일한 출처다.
const (
	// 서비스 공통
	DefaultLogLevel        = "info"
	DefaultShutdownTimeout = 15 * time.Second
	DefaultMetricsPath     = "/internal/metrics"

	// 인프라
	DefaultRedisAddr   = "127.0.0.1:6379"
	DefaultRedisPool   = 64
	DefaultKafkaBroker = "127.0.0.1:9092"
	DefaultKafkaTopic  = "shardgate.telemetry"
	DefaultKafkaGroup  = "shardgate-scorer"

	// 이벤트 / 샤딩 (§3.1, §3.2)
	DefaultShardSize     = 1000
	DefaultShardCount    = 16
	DefaultMaxShardCount = 4096
	DefaultLotteryWindow = 2 * time.Minute

	// 대기열 운영 (§5)
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultMissedHeartbeats  = 3
	DefaultEvictGrace        = 30 * time.Second
	DefaultStatusPollHint    = 5 * time.Second
	DefaultSSEKeepalive      = 15 * time.Second
	DefaultSweepBatch        = 256
	DefaultEvictedRetain     = 10 * time.Minute

	// 입장 제어 (§3.4, §7)
	DefaultAdmitRatePerMin = 3000
	DefaultAdmitInterval   = 5 * time.Second
	// 기본값이 false 인 이유는 Admission.AfterLottery 주석에 적었다.
	DefaultAdmitAfterLottery = false
	// 0 은 게이트 없음이다. 기본값으로 켜지 않는 이유는 Admission.MinDwell 주석에 적었다.
	DefaultAdmitMinDwell = time.Duration(0)
	DefaultAdmitMinBeats = 0

	DefaultMaxBudgetPerShard = 500
	DefaultBackpressureMin   = 0.1
	DefaultBreakerFailures   = 5
	DefaultBreakerCooldown   = 30 * time.Second
	DefaultShopHealthTimeout = 2 * time.Second

	// 챌린지 (§4-L2)
	DefaultChallengeTTL    = 2 * time.Minute
	DefaultBaseDifficulty  = 16
	DefaultMaxDifficulty   = 26
	DefaultChallengeNonces = 16 // nonce 바이트 길이

	// 토큰 (§4-L3)
	DefaultQueueTokenTTL  = 2 * time.Hour
	DefaultEntryTokenTTL  = 5 * time.Minute
	DefaultTokenIssuer    = "shardgate"
	DefaultIPv4PrefixBits = 24
	DefaultIPv6PrefixBits = 48

	// 봇 점수 임계값 (§4 조치 파이프라인) — 하드코딩 금지, 전부 여기서만.
	DefaultScoreGreylist   = 40
	DefaultScoreHold       = 70
	DefaultScoreBlock      = 90
	DefaultScoreWindow     = 60 * time.Second
	DefaultScoreFlush      = 5 * time.Second
	DefaultScoreMinSamples = 5
	DefaultScoreDecay      = 0.9

	// 신호 산출 파라미터 (§4-L5). 전부 여기서만 — 테스트를 통과시키려고 이 값을
	// 만지는 것은 금지다. 시나리오 쪽을 고칠 것.
	DefaultHeartbeatCVFloor    = 0.02                   // 이보다 규칙적이면 샤드 분포와 무관하게 비인간적이다
	DefaultSignalZSpan         = 3.0                    // z-score 가 이 값이면 신호 1.0
	DefaultScoreRobustBaseline = true                   // 상대 신호 기준선을 중앙값/MAD 로 (REPORT §3.13·§3.14 에서 측정 후 켬)
	DefaultSyncBin             = 250 * time.Millisecond // 동기화 판정용 시간 버킷
	DefaultFPGroupCap          = 5                      // 같은 지문 이 수 이상이면 신호 1.0
	DefaultIPGroupCap          = 8                      // 같은 /24 이 수 이상이면 신호 1.0
	DefaultMinSignalsToBlock   = 2                      // 차단에는 최소 이만큼의 신호가 함께 가리켜야 한다
	DefaultSignalMinContrib    = 0.2                    // 이 이상이어야 "가리켰다"로 센다
	DefaultSuspicionTTL        = 30 * time.Minute       // 의심도 보존 시간(적응형 난이도 입력)
	DefaultGreylistDifficulty  = 4                      // greylist 사용자의 PoW 난이도 상향폭(비트)

	// 재챌린지(§4 greylist 복귀). 값의 근거는 BotScore.RechallengePassScore 주석에 적었다.
	DefaultRechallengePassScore   = 35
	DefaultRechallengeMaxAttempts = 2

	// 신호별 가중치 (합이 1.0 이 되도록 검증한다)
	DefaultWeightHeartbeat   = 0.25
	DefaultWeightCorrelation = 0.20
	DefaultWeightFingerprint = 0.25
	DefaultWeightIPPrefix    = 0.15
	DefaultWeightPoW         = 0.15
)

// Config 는 한 프로세스가 필요로 하는 전체 설정이다.
type Config struct {
	Service   Service
	Redis     Redis
	Postgres  Postgres
	Kafka     Kafka
	Event     Event
	Queue     Queue
	Admission Admission
	Challenge Challenge
	Token     Token
	BotScore  BotScore

	// fromEnv 는 환경에서 실제로 읽힌 키·값이다. EffectiveEnv() 로만 노출한다.
	fromEnv map[string]string
}

// Service 는 프로세스 공통 설정이다.
type Service struct {
	Name            string
	HTTPAddr        string
	LogLevel        string
	MetricsPath     string
	ShutdownTimeout time.Duration
}

// Redis 는 hot path 상태 저장소 접속 설정이다.
type Redis struct {
	Addrs    []string
	Password Secret
	DB       int
	Cluster  bool
	PoolSize int
}

// Postgres 는 감사·차단 이력용 영속 DB 설정이다.
type Postgres struct {
	DSN      Secret
	Enabled  bool
	MaxConns int
}

// Kafka 는 텔레메트리 스트림 설정이다. 파티션 수는 샤드 수 상한과 맞춘다.
type Kafka struct {
	Brokers []string
	Topic   string
	GroupID string
	Enabled bool
}

// Event 는 이벤트(티켓 오픈) 단위 파라미터다.
type Event struct {
	ID            string
	Salt          Secret // 절대 로그에 남기지 않는다 (§3.1)
	OpenAt        time.Time
	ShardSize     int
	ShardCount    int
	MaxShardCount int
	LotteryWindow time.Duration
}

// Queue 는 대기열 운영 파라미터다.
type Queue struct {
	HeartbeatInterval time.Duration
	MissedHeartbeats  int
	EvictGrace        time.Duration
	StatusPollHint    time.Duration
	SSEKeepalive      time.Duration

	// SweepBatch 는 soft-evict 스윕 1회가 훑는 대기열 구간 크기다.
	// 샤드 전체를 한 번에 훑으면 Lua 한 번의 비용이 샤드 인원에 비례해 커지므로,
	// 커서를 들고 구간을 나눠 돈다.
	SweepBatch int
	// EvictedRetain 은 제거된 사용자 상태를 감사·재진입 판단용으로 남겨 두는 시간이다.
	EvictedRetain time.Duration
}

// Admission 은 입장 제어 파라미터다.
type Admission struct {
	RatePerMin int
	Interval   time.Duration

	// AfterLottery 는 추첨 구간이 닫힐 때까지 입장을 열지 않는다(§3.4).
	//
	// 탐지는 입장과 경주하고, 기본 설정에서는 진다(§12-7). 점수가 greylist 선을
	// 넘기까지 70~80초가 걸리는데 그 사이에 자리가 나가 버리기 때문이다. 이 값을
	// 켜면 추첨 구간만큼 탐지에 시간을 벌어 주는 대신 전체 대기 시간이 그만큼 늘어난다.
	// 공정성·처리량과 탐지율의 맞교환이라 기본값은 꺼 둔다. 측정은 REPORT.md §3.4.
	//
	// EVENT_OPEN_AT 이 없으면 추첨 구간 자체가 없으므로(queue.LotteryEnd) 아무 효과도 없다.
	AfterLottery bool

	// MinDwell 은 진입 후 이만큼 지나기 전에는 순번이 와도 입장시키지 않는다(§3.4).
	// MinBeats 는 같은 것을 생존 신호 수로 요구한다.
	//
	// AfterLottery 와 목적은 같고(탐지에 시간을 준다) 재는 기준이 다르다.
	// AfterLottery 는 이벤트 시각 하나로 전원을 막으므로 늦게 온 사람은 거의
	// 관찰되지 않은 채 통과하고, 막힌 주기의 자리는 그대로 사라진다. 이쪽은 각자의
	// 진입 시각부터 재기 때문에 늦게 온 사람도 똑같이 관찰되고, 예산을 건드리기 전에
	// 판단하므로 자리는 미뤄질 뿐 없어지지 않는다.
	//
	// 값의 근거는 취향이 아니라 측정이다 — 격리 P90 time-to-detection(REPORT §3.5).
	// 그보다 짧으면 아직 못 본 봇을 내보내고, 길면 사람만 기다리게 한다.
	// MinBeats 는 위조할 수 있지만(더 자주 보내면 된다) 그러면 규칙성 신호가
	// 선명해져 스스로 손해다. 강제력은 서버가 기록하는 MinDwell 쪽에 있다.
	MinDwell time.Duration
	MinBeats int64

	MaxBudgetPerShard int
	BackpressureMin   float64
	BreakerFailures   int
	BreakerCooldown   time.Duration
	ShopHealthURL     string
	ShopHealthTimeout time.Duration
}

// Challenge 는 적응형 PoW 파라미터다. 실제 난이도 값은 botscore 가 결정한다.
type Challenge struct {
	TTL            time.Duration
	BaseDifficulty int
	MaxDifficulty  int
	NonceBytes     int
	HMACKey        Secret
	CaptchaEnabled bool
}

// Token 은 큐 토큰 / 입장 토큰 설정이다.
type Token struct {
	SigningKey     Secret
	Issuer         string
	QueueTTL       time.Duration
	EntryTTL       time.Duration
	IPv4PrefixBits int
	IPv6PrefixBits int

	// SecureCookie 는 큐 토큰 쿠키에 Secure 를 붙일지다.
	// 운영에서는 반드시 켠 채로 둔다 — 끄면 평문 연결로 토큰이 새 나간다.
	// 로컬 http 개발에서만 끈다.
	SecureCookie bool
}

// BotScore 는 점수 임계값과 신호 가중치다.
type BotScore struct {
	Greylist   int
	Hold       int
	Block      int
	Window     time.Duration
	FlushEvery time.Duration
	MinSamples int
	Decay      float64
	Weights    Weights

	// 신호 산출 파라미터.
	HeartbeatCVFloor float64
	SignalZSpan      float64
	SyncBin          time.Duration
	FPGroupCap       int
	IPGroupCap       int

	// RobustBaseline 은 상대 신호(heartbeat 상대항·PoW)의 기준선을
	// 평균/표준편차 대신 **중앙값/MAD** 로 잡는다.
	//
	// 왜 있는가: 상대 신호는 "이 샤드의 다른 사람들과 비교해"를 묻고, 그 비교
	// 기준을 평균으로 잡으면 **공격자가 표본을 밀어 넣어 기준을 옮길 수 있다.**
	// 자기 봇과 같은 규칙성 대역의 계정을 다수 넣으면 그 대역이 더 이상 특이하지
	// 않게 된다 — 표적 봇의 규칙성 신호가 0.780 → 0.320 으로 밀리는 것을 합성으로
	// 확인했다(REPORT §3.12).
	//
	// **기본값을 켠 근거는 부하 측정 두 팔이다.** 오탐 집단이 인구의 14% 인
	// 팔에서 탐지율 85.3 → 98.1% 이고 오탐율은 그대로였으며(§3.13), 오탐 집단이
	// 없는 팔에서는 탐지율·오탐율이 그대로이고 격리가 33초 빨라졌다(§3.14).
	//
	// 받아들인 위험은 **붕괴점**이다: 오염이 50% 를 넘으면 중앙값/MAD 가 평균보다
	// 오히려 나쁘다. 다만 그 지점에 닿으려면 샤드의 절반을 쥐어야 하고, 배정이
	// 예측 불가능한 한(§3.1) 그건 전체 참가자의 절반을 넘긴다는 뜻이다 —
	// 그 체제에서는 상대 신호 자체가 이미 성립하지 않는다(§12-6).
	//
	// 끄면 §3.1~§3.12 의 표를 만든 체제로 돌아간다.
	RobustBaseline bool

	// MinSignalsToBlock 은 차단에 필요한 최소 신호 수다.
	// "어떤 단일 신호도 즉시 차단의 근거가 될 수 없다"(불변식 3)를 값으로 강제한다.
	MinSignalsToBlock int
	// SignalMinContrib 는 신호 하나가 "가리켰다"로 세어지는 최소 크기다.
	SignalMinContrib float64

	// SuspicionTTL 은 의심도(적응형 PoW 난이도 입력)를 남겨 두는 시간이다.
	SuspicionTTL time.Duration
	// GreylistDifficulty 는 greylist 사용자에게 올리는 PoW 난이도 폭(비트)이다.
	GreylistDifficulty int

	// RechallengePassScore 는 재챌린지를 통과한 사용자의 점수 상한이다(§4).
	//
	// 0 이 아니라 임계 직하인 이유: 통과는 무죄 판결이 아니라 재검증 1회 통과일
	// 뿐이다. 0 으로 되돌리면 CAPTCHA 를 사람이 대신 풀어 주는 봇에게 사실상
	// 면제권이 된다 — 지수평활(Decay) 때문에 40 까지 다시 오르는 데 창 스무 개가
	// 더 필요해지기 때문이다. 반대로 점수를 그대로 두면 다음 창에서 즉시 재격리돼
	// 통과가 의미를 잃는다. 그래서 "한 칸 아래"로만 내린다.
	// 클램프이지 대입이 아니다 — 이 값보다 낮은 점수를 올리지는 않는다.
	RechallengePassScore int
	// RechallengeMaxAttempts 는 허용하는 재챌린지 통과 횟수다.
	// 이 횟수를 넘겨서 또 오면 복귀시키지 않고 보류(Hold)로 올린다 —
	// 계속 걸리고 계속 풀어 대는 것 자체가 신호다.
	RechallengeMaxAttempts int
}

// Weights 는 §4-L5 신호별 가중치다. 합은 1.0.
type Weights struct {
	Heartbeat   float64
	Correlation float64
	Fingerprint float64
	IPPrefix    float64
	PoW         float64
}

// Sum 은 가중치 합을 반환한다.
func (w Weights) Sum() float64 {
	return w.Heartbeat + w.Correlation + w.Fingerprint + w.IPPrefix + w.PoW
}

// Load 는 프로세스 환경변수에서 설정을 읽는다. serviceName 은 로그·메트릭 라벨로 쓰인다.
func Load(serviceName, defaultAddr string) (*Config, error) {
	return LoadFrom(os.LookupEnv, serviceName, defaultAddr)
}

// LoadFrom 은 임의의 lookup 함수로 설정을 읽는다(테스트용).
func LoadFrom(lookup func(string) (string, bool), serviceName, defaultAddr string) (*Config, error) {
	l := &loader{lookup: lookup}

	cfg := &Config{
		Service: Service{
			Name:            serviceName,
			HTTPAddr:        l.str("HTTP_ADDR", defaultAddr),
			LogLevel:        l.str("LOG_LEVEL", DefaultLogLevel),
			MetricsPath:     l.str("METRICS_PATH", DefaultMetricsPath),
			ShutdownTimeout: l.dur("SHUTDOWN_TIMEOUT", DefaultShutdownTimeout),
		},
		Redis: Redis{
			Addrs:    l.list("REDIS_ADDRS", []string{DefaultRedisAddr}),
			Password: l.secretRaw("REDIS_PASSWORD"),
			DB:       l.intVal("REDIS_DB", 0),
			Cluster:  l.boolVal("REDIS_CLUSTER", false),
			PoolSize: l.intVal("REDIS_POOL_SIZE", DefaultRedisPool),
		},
		Postgres: Postgres{
			DSN:      l.secretRaw("POSTGRES_DSN"),
			Enabled:  l.boolVal("POSTGRES_ENABLED", true),
			MaxConns: l.intVal("POSTGRES_MAX_CONNS", 8),
		},
		Kafka: Kafka{
			Brokers: l.list("KAFKA_BROKERS", []string{DefaultKafkaBroker}),
			Topic:   l.str("KAFKA_TOPIC", DefaultKafkaTopic),
			GroupID: l.str("KAFKA_GROUP_ID", DefaultKafkaGroup),
			Enabled: l.boolVal("KAFKA_ENABLED", true),
		},
		Event: Event{
			ID:            l.str("EVENT_ID", "demo"),
			Salt:          l.secretHex("EVENT_SALT"),
			OpenAt:        l.timeVal("EVENT_OPEN_AT", time.Time{}),
			ShardSize:     l.intVal("SHARD_SIZE", DefaultShardSize),
			ShardCount:    l.intVal("SHARD_COUNT", DefaultShardCount),
			MaxShardCount: l.intVal("MAX_SHARD_COUNT", DefaultMaxShardCount),
			LotteryWindow: l.dur("LOTTERY_WINDOW", DefaultLotteryWindow),
		},
		Queue: Queue{
			HeartbeatInterval: l.dur("HEARTBEAT_INTERVAL", DefaultHeartbeatInterval),
			MissedHeartbeats:  l.intVal("MISSED_HEARTBEATS", DefaultMissedHeartbeats),
			EvictGrace:        l.dur("EVICT_GRACE", DefaultEvictGrace),
			StatusPollHint:    l.dur("STATUS_POLL_HINT", DefaultStatusPollHint),
			SSEKeepalive:      l.dur("SSE_KEEPALIVE", DefaultSSEKeepalive),
			SweepBatch:        l.intVal("SWEEP_BATCH", DefaultSweepBatch),
			EvictedRetain:     l.dur("EVICTED_RETAIN", DefaultEvictedRetain),
		},
		Admission: Admission{
			RatePerMin:        l.intVal("ADMIT_RATE_PER_MIN", DefaultAdmitRatePerMin),
			Interval:          l.dur("ADMIT_INTERVAL", DefaultAdmitInterval),
			AfterLottery:      l.boolVal("ADMIT_AFTER_LOTTERY", DefaultAdmitAfterLottery),
			MinDwell:          l.dur("ADMIT_MIN_DWELL", DefaultAdmitMinDwell),
			MinBeats:          int64(l.intVal("ADMIT_MIN_BEATS", DefaultAdmitMinBeats)),
			MaxBudgetPerShard: l.intVal("MAX_BUDGET_PER_SHARD", DefaultMaxBudgetPerShard),
			BackpressureMin:   l.floatVal("BACKPRESSURE_MIN", DefaultBackpressureMin),
			BreakerFailures:   l.intVal("BREAKER_FAILURES", DefaultBreakerFailures),
			BreakerCooldown:   l.dur("BREAKER_COOLDOWN", DefaultBreakerCooldown),
			ShopHealthURL:     l.str("SHOP_HEALTH_URL", ""),
			ShopHealthTimeout: l.dur("SHOP_HEALTH_TIMEOUT", DefaultShopHealthTimeout),
		},
		Challenge: Challenge{
			TTL:            l.dur("CHALLENGE_TTL", DefaultChallengeTTL),
			BaseDifficulty: l.intVal("POW_BASE_DIFFICULTY", DefaultBaseDifficulty),
			MaxDifficulty:  l.intVal("POW_MAX_DIFFICULTY", DefaultMaxDifficulty),
			NonceBytes:     l.intVal("POW_NONCE_BYTES", DefaultChallengeNonces),
			HMACKey:        l.secretHex("CHALLENGE_HMAC_KEY"),
			CaptchaEnabled: l.boolVal("CAPTCHA_ENABLED", false),
		},
		Token: Token{
			SigningKey:     l.secretHex("TOKEN_SIGNING_KEY"),
			Issuer:         l.str("TOKEN_ISSUER", DefaultTokenIssuer),
			QueueTTL:       l.dur("QUEUE_TOKEN_TTL", DefaultQueueTokenTTL),
			EntryTTL:       l.dur("ENTRY_TOKEN_TTL", DefaultEntryTokenTTL),
			SecureCookie:   l.boolVal("SECURE_COOKIE", true),
			IPv4PrefixBits: l.intVal("IPV4_PREFIX_BITS", DefaultIPv4PrefixBits),
			IPv6PrefixBits: l.intVal("IPV6_PREFIX_BITS", DefaultIPv6PrefixBits),
		},
		BotScore: BotScore{
			Greylist:   l.intVal("SCORE_GREYLIST", DefaultScoreGreylist),
			Hold:       l.intVal("SCORE_HOLD", DefaultScoreHold),
			Block:      l.intVal("SCORE_BLOCK", DefaultScoreBlock),
			Window:     l.dur("SCORE_WINDOW", DefaultScoreWindow),
			FlushEvery: l.dur("SCORE_FLUSH_EVERY", DefaultScoreFlush),
			MinSamples: l.intVal("SCORE_MIN_SAMPLES", DefaultScoreMinSamples),
			Decay:      l.floatVal("SCORE_DECAY", DefaultScoreDecay),

			HeartbeatCVFloor:   l.floatVal("HEARTBEAT_CV_FLOOR", DefaultHeartbeatCVFloor),
			SignalZSpan:        l.floatVal("SIGNAL_Z_SPAN", DefaultSignalZSpan),
			RobustBaseline:     l.boolVal("SCORE_ROBUST_BASELINE", DefaultScoreRobustBaseline),
			SyncBin:            l.dur("SYNC_BIN", DefaultSyncBin),
			FPGroupCap:         l.intVal("FP_GROUP_CAP", DefaultFPGroupCap),
			IPGroupCap:         l.intVal("IP_GROUP_CAP", DefaultIPGroupCap),
			MinSignalsToBlock:  l.intVal("SCORE_MIN_SIGNALS_TO_BLOCK", DefaultMinSignalsToBlock),
			SignalMinContrib:   l.floatVal("SIGNAL_MIN_CONTRIB", DefaultSignalMinContrib),
			SuspicionTTL:       l.dur("SUSPICION_TTL", DefaultSuspicionTTL),
			GreylistDifficulty: l.intVal("GREYLIST_DIFFICULTY_BUMP", DefaultGreylistDifficulty),

			RechallengePassScore:   l.intVal("RECHALLENGE_PASS_SCORE", DefaultRechallengePassScore),
			RechallengeMaxAttempts: l.intVal("RECHALLENGE_MAX_ATTEMPTS", DefaultRechallengeMaxAttempts),
			Weights: Weights{
				Heartbeat:   l.floatVal("WEIGHT_HEARTBEAT", DefaultWeightHeartbeat),
				Correlation: l.floatVal("WEIGHT_CORRELATION", DefaultWeightCorrelation),
				Fingerprint: l.floatVal("WEIGHT_FINGERPRINT", DefaultWeightFingerprint),
				IPPrefix:    l.floatVal("WEIGHT_IP_PREFIX", DefaultWeightIPPrefix),
				PoW:         l.floatVal("WEIGHT_POW", DefaultWeightPoW),
			},
		},
	}

	if err := errors.Join(l.errs...); err != nil {
		return nil, fmt.Errorf("config: parse env: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}
	cfg.fromEnv = l.fromEnv
	return cfg, nil
}

// secretKeyParts 는 값을 가려야 하는 키의 조각이다. 키 **이름**으로 판단한다 —
// 값을 보고 판단하려 들면 언젠가 새는 쪽으로 틀린다.
var secretKeyParts = []string{"SALT", "KEY", "PASSWORD", "SECRET", "DSN", "TOKEN"}

// EffectiveEnv 는 이 프로세스가 환경에서 실제로 읽어 적용한 설정이다.
// 비밀 값은 이름으로 판단해 가린다(config.Secret 과 같은 원칙).
//
// 기동 로그에 이걸 찍어 두면 측정 하네스가 팔 정의와 대조할 수 있다.
// **"설정을 줬다"와 "설정이 적용됐다"는 다른 사실이고, 표를 오염시키는 것은 후자다.**
func (c *Config) EffectiveEnv() map[string]string {
	out := make(map[string]string, len(c.fromEnv))
	for k, v := range c.fromEnv {
		out[k] = v
		for _, part := range secretKeyParts {
			if strings.Contains(k, part) {
				out[k] = "<redacted>"
				break
			}
		}
	}
	return out
}

// Validate 는 설정값의 정합성을 검사한다.
func (c *Config) Validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	check(c.Event.ID != "", "EVENT_ID must not be empty")
	check(c.Event.ShardSize > 0, "SHARD_SIZE must be > 0")
	check(c.Event.ShardCount > 0, "SHARD_COUNT must be > 0")
	check(c.Event.ShardCount <= c.Event.MaxShardCount,
		"SHARD_COUNT (%d) must be <= MAX_SHARD_COUNT (%d)", c.Event.ShardCount, c.Event.MaxShardCount)
	check(c.Event.LotteryWindow >= 0, "LOTTERY_WINDOW must be >= 0")

	check(len(c.Redis.Addrs) > 0, "REDIS_ADDRS must not be empty")
	check(c.Redis.PoolSize > 0, "REDIS_POOL_SIZE must be > 0")

	check(c.Queue.HeartbeatInterval > 0, "HEARTBEAT_INTERVAL must be > 0")
	check(c.Queue.MissedHeartbeats > 0, "MISSED_HEARTBEATS must be > 0")
	check(c.Queue.EvictGrace >= 0, "EVICT_GRACE must be >= 0")
	check(c.Queue.SweepBatch > 0, "SWEEP_BATCH must be > 0")
	check(c.Queue.EvictedRetain > 0, "EVICTED_RETAIN must be > 0")

	check(c.Admission.RatePerMin > 0, "ADMIT_RATE_PER_MIN must be > 0")
	check(c.Admission.Interval > 0, "ADMIT_INTERVAL must be > 0")
	// 게이트를 켰는데 오픈 시각이 없으면 추첨 구간 자체가 없어 게이트가 조용히
	// 아무 일도 하지 않는다. 켠 사람은 켜졌다고 믿을 테니 기동을 거부한다.
	check(!c.Admission.AfterLottery || !c.Event.OpenAt.IsZero(),
		"ADMIT_AFTER_LOTTERY requires EVENT_OPEN_AT (없으면 추첨 구간이 없어 게이트가 무효다)")
	check(c.Admission.MinDwell >= 0, "ADMIT_MIN_DWELL must be >= 0")
	check(c.Admission.MinBeats >= 0, "ADMIT_MIN_BEATS must be >= 0")
	// 관찰 시간이 큐 토큰 수명보다 길면 게이트가 열리기 전에 토큰이 죽는다.
	// 아무도 입장하지 못하는데 오류는 한 건도 나지 않는 고장이라 기동을 거부한다.
	check(c.Admission.MinDwell < c.Token.QueueTTL,
		"ADMIT_MIN_DWELL (%v) must be < QUEUE_TOKEN_TTL (%v) — 아니면 게이트가 열리기 전에 토큰이 만료된다",
		c.Admission.MinDwell, c.Token.QueueTTL)
	check(c.Admission.MaxBudgetPerShard > 0, "MAX_BUDGET_PER_SHARD must be > 0")
	check(c.Admission.BackpressureMin > 0 && c.Admission.BackpressureMin <= 1,
		"BACKPRESSURE_MIN must be within (0,1]")

	check(c.Challenge.BaseDifficulty > 0, "POW_BASE_DIFFICULTY must be > 0")
	check(c.Challenge.MaxDifficulty >= c.Challenge.BaseDifficulty,
		"POW_MAX_DIFFICULTY (%d) must be >= POW_BASE_DIFFICULTY (%d)",
		c.Challenge.MaxDifficulty, c.Challenge.BaseDifficulty)
	check(c.Challenge.MaxDifficulty <= maxSupportedDifficulty,
		"POW_MAX_DIFFICULTY must be <= %d", maxSupportedDifficulty)
	check(c.Challenge.NonceBytes >= minNonceBytes, "POW_NONCE_BYTES must be >= %d", minNonceBytes)
	check(c.Challenge.TTL > 0, "CHALLENGE_TTL must be > 0")

	check(c.Token.QueueTTL > 0, "QUEUE_TOKEN_TTL must be > 0")
	check(c.Token.EntryTTL > 0, "ENTRY_TOKEN_TTL must be > 0")
	check(c.Token.IPv4PrefixBits > 0 && c.Token.IPv4PrefixBits <= 32, "IPV4_PREFIX_BITS must be within (0,32]")
	check(c.Token.IPv6PrefixBits > 0 && c.Token.IPv6PrefixBits <= 128, "IPV6_PREFIX_BITS must be within (0,128]")

	// 조치 파이프라인은 반드시 단조 증가해야 한다: 관찰 < greylist < 보류 < 차단.
	check(0 < c.BotScore.Greylist && c.BotScore.Greylist < c.BotScore.Hold &&
		c.BotScore.Hold < c.BotScore.Block && c.BotScore.Block <= scoreMax,
		"score thresholds must satisfy 0 < greylist(%d) < hold(%d) < block(%d) <= %d",
		c.BotScore.Greylist, c.BotScore.Hold, c.BotScore.Block, scoreMax)
	check(c.BotScore.Window > 0, "SCORE_WINDOW must be > 0")
	check(c.BotScore.MinSamples > 0, "SCORE_MIN_SAMPLES must be > 0")
	check(c.BotScore.SignalZSpan > 0, "SIGNAL_Z_SPAN must be > 0")
	check(c.BotScore.SyncBin > 0, "SYNC_BIN must be > 0")
	check(c.BotScore.FPGroupCap > 1, "FP_GROUP_CAP must be > 1")
	check(c.BotScore.IPGroupCap > 1, "IP_GROUP_CAP must be > 1")
	check(c.BotScore.SuspicionTTL > 0, "SUSPICION_TTL must be > 0")
	check(c.BotScore.GreylistDifficulty >= 0, "GREYLIST_DIFFICULTY_BUMP must be >= 0")
	// 통과 점수가 greylist 임계 이상이면 통과한 사용자가 다음 창에서 즉시 재격리된다.
	// 재챌린지가 있는데 아무 효과가 없는 상태가 되므로 기동을 거부한다.
	check(c.BotScore.RechallengePassScore >= 0 && c.BotScore.RechallengePassScore < c.BotScore.Greylist,
		"RECHALLENGE_PASS_SCORE (%d) must be within [0, SCORE_GREYLIST(%d)) — 아니면 통과해도 즉시 재격리된다",
		c.BotScore.RechallengePassScore, c.BotScore.Greylist)
	// 0 회는 greylist 를 다시 출구 없는 종점으로 만든다(REPORT §3.5).
	// 사다리의 40~69 칸이 70~89 와 같아지므로 값으로 막는다.
	check(c.BotScore.RechallengeMaxAttempts >= 1,
		"RECHALLENGE_MAX_ATTEMPTS must be >= 1 (0 이면 greylist 가 출구 없는 종점이 된다)")
	// 차단에 최소 2개의 신호를 요구하는 것이 불변식 3 을 값으로 강제하는 지점이다.
	check(c.BotScore.MinSignalsToBlock >= 2,
		"SCORE_MIN_SIGNALS_TO_BLOCK must be >= 2 (불변식 3: 단일 신호로 차단 금지)")
	check(c.BotScore.SignalMinContrib > 0 && c.BotScore.SignalMinContrib <= 1,
		"SIGNAL_MIN_CONTRIB must be within (0,1]")
	check(c.BotScore.Decay > 0 && c.BotScore.Decay <= 1, "SCORE_DECAY must be within (0,1]")
	if sum := c.BotScore.Weights.Sum(); sum < 1-weightEpsilon || sum > 1+weightEpsilon {
		errs = append(errs, fmt.Errorf("botscore weights must sum to 1.0, got %.3f", sum))
	}

	return errors.Join(errs...)
}

const (
	scoreMax               = 100
	weightEpsilon          = 1e-6
	maxSupportedDifficulty = 32 // SHA-256 앞 32비트까지만 난이도로 사용
	minNonceBytes          = 8
)

// RequireSecrets 는 서비스가 실제로 필요로 하는 비밀 값이 채워졌는지 확인한다.
// 서비스마다 필요한 비밀이 다르므로 Load 와 분리했다.
func (c *Config) RequireSecrets(names ...string) error {
	var errs []error
	for _, n := range names {
		switch n {
		case "event_salt":
			if c.Event.Salt.IsZero() {
				errs = append(errs, errors.New("SG_EVENT_SALT is required"))
			}
		case "token_signing_key":
			if c.Token.SigningKey.IsZero() {
				errs = append(errs, errors.New("SG_TOKEN_SIGNING_KEY is required"))
			}
		case "challenge_hmac_key":
			if c.Challenge.HMACKey.IsZero() {
				errs = append(errs, errors.New("SG_CHALLENGE_HMAC_KEY is required"))
			}
		case "postgres_dsn":
			if c.Postgres.Enabled && c.Postgres.DSN.IsZero() {
				errs = append(errs, errors.New("SG_POSTGRES_DSN is required"))
			}
		default:
			errs = append(errs, fmt.Errorf("unknown secret requirement %q", n))
		}
	}
	return errors.Join(errs...)
}

// loader 는 환경변수를 읽으며 에러를 모아 둔다. 첫 에러에서 멈추지 않고
// 잘못된 설정을 한 번에 모두 보고한다.
type loader struct {
	lookup func(string) (string, bool)
	errs   []error

	// fromEnv 는 **환경에서 실제로 읽힌** 키와 값이다. 기본값으로 결정된 항목은
	// 들어오지 않는다 — 하네스가 확인해야 하는 것은 "내가 준 값이 닿았는가"이고,
	// 닿지 않은 이름은 여기 없는 것으로 드러난다.
	fromEnv map[string]string
}

const envPrefix = "SG_"

// raw 는 모든 설정 접근의 단일 통로다. 그래서 **환경에서 실제로 읽힌 것**을 여기서
// 기록한다 — 어느 접근자를 거쳤든 이 함수를 지나므로 한 곳이면 충분하다.
//
// 왜 기록하는가: 이름이 컨테이너에 닿지 않아도 아무것도 실패하지 않는다. compose 의
// 환경 블록은 화이트리스트라 없는 이름은 조용히 무시되고, 서비스는 코드 기본값으로
// 정상 기동하며, 측정은 팔만 조용히 바뀐 채 그럴듯한 표를 낸다(ROADMAP 결함 8).
// 손잡이마다 로그를 하나씩 심는 것으로는 손잡이가 늘 때마다 같은 구멍이 다시 열린다.
// **적용된 설정 전체를 기동 시 덤프하고 측정 하네스가 팔 정의와 대조하는 것**이
// 그 구멍의 뿌리를 막는 방법이다.
func (l *loader) raw(key string) (string, bool) {
	v, ok := l.lookup(envPrefix + key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	if l.fromEnv == nil {
		l.fromEnv = map[string]string{}
	}
	l.fromEnv[envPrefix+key] = v
	return v, true
}

func (l *loader) fail(key string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s%s: %w", envPrefix, key, err))
}

func (l *loader) str(key, def string) string {
	if v, ok := l.raw(key); ok {
		return v
	}
	return def
}

func (l *loader) list(key string, def []string) []string {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func (l *loader) intVal(key string, def int) int {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(key, errors.New("not an integer"))
		return def
	}
	return n
}

func (l *loader) floatVal(key string, def float64) float64 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.fail(key, errors.New("not a number"))
		return def
	}
	return f
}

func (l *loader) boolVal(key string, def bool) bool {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, errors.New("not a boolean"))
		return def
	}
	return b
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(key, errors.New("not a duration (e.g. 5s, 2m)"))
		return def
	}
	return d
}

func (l *loader) timeVal(key string, def time.Time) time.Time {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		l.fail(key, errors.New("not an RFC3339 timestamp"))
		return def
	}
	return t
}

func (l *loader) secretHex(key string) Secret {
	v, ok := l.raw(key)
	if !ok {
		return Secret{}
	}
	s, err := ParseSecretHex(v)
	if err != nil {
		l.fail(key, err) // ParseSecretHex 의 에러는 원문을 포함하지 않는다.
		return Secret{}
	}
	return s
}

func (l *loader) secretRaw(key string) Secret {
	v, ok := l.raw(key)
	if !ok {
		return Secret{}
	}
	return NewSecret([]byte(v))
}
