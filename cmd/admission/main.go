// Command admission 은 입장 제어 서비스다.
// 글로벌 admit rate 를 샤드 예산으로 주기 배분하고(§3.4), 순번이 도달한 사용자에게
// 1회용 입장 토큰을 발행한다.
package main

import (
	"context"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/api"
	"github.com/hjr/shardgate/internal/app"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

func main() { app.Main("admission", run) }

func run() error {
	a, err := app.New("admission", ":8082", "token_signing_key")
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()

	store, err := admission.NewStore(a.Redis, admission.FromConfig(a.Cfg), a.Log, a.Metrics)
	if err != nil {
		return err
	}
	qstore, err := queue.New(a.Redis, queue.FromConfig(a.Cfg), a.Log, a.Metrics)
	if err != nil {
		return err
	}
	issuer, err := token.NewIssuer(a.Cfg.Token)
	if err != nil {
		return err
	}

	tel := telemetry.NewProducer(a.Cfg.Kafka, a.Log, a.Metrics)
	defer func() { _ = tel.Close() }()

	api.NewAdmissionAPI(store, issuer, tel, a.Cfg, a.Log, a.Metrics).Register(a.Server.Mux())

	health := admission.NewHTTPHealth(a.Cfg.Admission.ShopHealthURL, a.Cfg.Admission.ShopHealthTimeout)
	controller := admission.NewController(store, qstore, health, a.Log, a.Metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 배분 루프는 HTTP 와 독립적으로 돈다.
	go func() { _ = controller.Run(ctx) }()

	return a.Run(ctx)
}
