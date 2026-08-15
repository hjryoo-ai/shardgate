// Command scorer 는 샤드 단위 이상탐지와 조치 파이프라인을 돌린다(DESIGN.md §4-L5).
//
// Kafka 에서 신호를 읽어 창에 쌓고, 주기적으로 샤드 분포와 비교해 점수를 낸 뒤
// 단계적 조치(관찰 → greylist → 보류 → 차단)를 적용한다.
//
// **이 프로세스가 죽어도 대기열은 진행된다**(불변식 5). admit 경로는 Redis 의
// 예산과 순번만 보고, 여기서 만드는 것은 점수와 상태 전이뿐이다. 스코어러가
// 멈추면 탐지가 멈출 뿐 사용자는 계속 입장한다.
package main

import (
	"context"

	"github.com/hjr/shardgate/internal/app"
	"github.com/hjr/shardgate/internal/botscore"
	"github.com/hjr/shardgate/internal/store/pg"
	"github.com/hjr/shardgate/internal/telemetry"
)

func main() { app.Main("scorer", run) }

func run() error {
	a, err := app.New("scorer", ":8083", "event_salt")
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 감사 적재는 선택이다. PG 가 없어도 방어는 동작해야 한다 —
	// 근거를 남기지 못하는 것과 방어를 못 하는 것은 다른 문제다.
	var sink botscore.Sink
	if a.Cfg.Postgres.Enabled && !a.Cfg.Postgres.DSN.IsZero() {
		pool, perr := pg.Open(ctx, a.Cfg.Postgres)
		if perr != nil {
			return perr
		}
		defer pool.Close()

		audit := pg.NewAudit(pool, a.Log)
		defer func() { _ = audit.Close() }()
		sink = audit
	} else {
		a.Log.Warn("postgres disabled; actions will not be recorded for audit")
	}

	scorer := botscore.NewScorer(a.Cfg.Event.ID, a.Cfg.BotScore, a.Log, a.Metrics)
	actuator := botscore.NewActuator(a.Redis, a.Cfg.Event.ID, a.Cfg.BotScore,
		a.Cfg.Token.QueueTTL, sink, a.Log, a.Metrics)

	go scorer.Run(ctx, actuator.Apply)

	if a.Cfg.Kafka.Enabled {
		consumer := telemetry.NewConsumer(a.Cfg.Kafka, a.Log, a.Metrics)
		defer func() { _ = consumer.Close() }()
		go func() { _ = consumer.Run(ctx, scorer.Observe) }()
	} else {
		a.Log.Warn("kafka disabled; scorer will not receive any signals")
	}

	return a.Run(ctx)
}
