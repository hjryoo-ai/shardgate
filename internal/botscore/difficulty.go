package botscore

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
)

// Difficulty 는 의심도를 PoW 난이도로 옮기는 challenge.DifficultyProvider 구현이다.
//
// 난이도를 정하는 코드가 challenge 가 아니라 여기 있는 것이 규칙이다
// (CLAUDE.md: "난이도는 botscore 가 주는 값만 사용"). challenge 는 값을 받기만 하고,
// 그 값이 어떤 근거에서 나왔는지 알지 못한다.
//
// # 왜 지문·대역 단위인가
//
// 토큰은 버리고 새로 받으면 그만이다. 봇에게 비용이 되려면 쉽게 못 바꾸는 것에
// 의심도를 붙여야 한다 — 기기 지문과 IP 대역이 그것이다. 정상 사용자는 자기
// 지문·대역의 의심도가 오를 일이 없으므로 기본 난이도 그대로 통과한다.
type Difficulty struct {
	rdb     redis.UniversalClient
	eventID string
	base    int
	bump    int
	log     *slog.Logger
}

// NewDifficulty 는 의심도 기반 난이도 제공자를 만든다.
func NewDifficulty(rdb redis.UniversalClient, eventID string, chal config.Challenge, score config.BotScore, log *slog.Logger) *Difficulty {
	if log == nil {
		log = slog.Default()
	}
	return &Difficulty{
		rdb: rdb, eventID: eventID,
		base: chal.BaseDifficulty, bump: score.GreylistDifficulty, log: log,
	}
}

// Difficulty 는 이 요청자의 난이도를 돌려준다.
//
// 의심도 n 회당 bump 비트씩 올린다. PoW 는 1비트마다 비용이 두 배가 되므로,
// 한 번 걸린 봇팜은 다음 진입에서 2^bump 배를 문다. 상한은 challenge 쪽에서 다시
// 자른다 — 여기서 실수해도 사람이 못 푸는 난이도가 나가지 않도록.
//
// 재챌린지 회차(Attempt)는 **더하지 않고 최댓값을 취한다.**
//
// 둘은 같은 사건을 다른 범위에서 센다. 의심도는 지문·대역에 붙어 있어 그 주체가
// 격리될 때마다 오르고, 회차는 그 토큰 자신이 재검증을 통과할 때마다 오른다.
// 한 번의 격리→재검증이 양쪽을 동시에 올리므로 더하면 사건을 두 번 세는 셈이고,
// 실제로 그렇게 했더니 두 회차 만에 상한(26비트)에 닿았다 — 58건 중 55건이 상한으로
// 나가 아무도 풀지 못했고, **재검증 경로가 있는데 아무도 통과하지 못하는 상태**가 됐다.
// 출구를 만들어 놓고 문을 잠근 셈이라 고치려던 결함으로 되돌아간다.
//
// 회차를 버리지 않는 이유는 범위가 다르기 때문이다. 의심도는 지문·대역을 갈면
// 흐려지지만 회차는 그 토큰의 이력이라 갈아 낼 수 없다 — 회차는 **하한**이고,
// 의심도는 주체 단위 추정이다. 둘 중 나쁜 쪽을 쓴다.
func (d *Difficulty) Difficulty(ctx context.Context, s challenge.Subject) (int, error) {
	worst := 0
	for _, subject := range []string{s.FPHash, s.IPPrefix} {
		if subject == "" {
			continue
		}
		n, err := d.rdb.Get(ctx, keys.Suspicion(d.eventID, subject)).Int()
		if err != nil {
			// 키가 없거나(정상) 조회에 실패한 경우. 어느 쪽이든 기본 난이도로 간다.
			// 의심도를 못 읽었다고 진입을 막으면, 스코어러 장애가 곧 대기실 장애가 된다.
			continue
		}
		if n > worst {
			worst = n
		}
	}

	rounds := worst
	if s.Attempt > rounds {
		rounds = s.Attempt
	}
	return d.base + rounds*d.bump, nil
}
