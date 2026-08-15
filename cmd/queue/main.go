// Command queue 는 대기열 서비스다. 순번 조회(SSE/폴링)와 생존 신호를 처리하고,
// 백그라운드에서 heartbeat 이 끊긴 사용자를 soft-evict 로 정리한다(DESIGN.md §5).
package main

import (
	"context"

	"github.com/hjr/shardgate/internal/api"
	"github.com/hjr/shardgate/internal/app"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

func main() { app.Main("queue", run) }

func run() error {
	a, err := app.New("queue", ":8081", "event_salt", "token_signing_key")
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()

	store, err := queue.New(a.Redis, queue.FromConfig(a.Cfg), a.Log, a.Metrics)
	if err != nil {
		return err
	}
	issuer, err := token.NewIssuer(a.Cfg.Token)
	if err != nil {
		return err
	}

	// 텔레메트리 발행은 절대 블로킹하지 않는다. Kafka 가 죽어도 heartbeat 은
	// 정상 응답한다 — 탐지가 늦어질 뿐 대기열은 진행된다(불변식 5).
	tel := telemetry.NewProducer(a.Cfg.Kafka, a.Log, a.Metrics)
	defer func() { _ = tel.Close() }()

	api.NewQueueAPI(store, issuer, tel, a.Cfg, a.Log, a.Metrics).Register(a.Server.Mux())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 스윕은 대기열 진행과 독립적으로 돈다. 여기서 실패해도 순번 조회는 계속돼야 한다.
	go queue.NewSweeper(store, a.Cfg.Queue, a.Log, a.Metrics).Run(ctx)

	return a.Run(ctx)
}
