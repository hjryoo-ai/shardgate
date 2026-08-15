// Command shop 은 mock 구매 API 다. 입장 토큰을 검증·소각하고 멱등키로 1인 1구매를
// 강제한다(DESIGN.md §4 "입장 이후 방어", 불변식 2·4).
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/api"
	"github.com/hjr/shardgate/internal/app"
	"github.com/hjr/shardgate/internal/store/pg"
	"github.com/hjr/shardgate/internal/token"
)

func main() { app.Main("shop", run) }

func run() error {
	a, err := app.New("shop", ":8084", "token_signing_key", "postgres_dsn")
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 주문은 사라지면 안 되는 값이다. PG 없이 뜨는 것을 허용하지 않는다.
	if !a.Cfg.Postgres.Enabled {
		return errors.New("shop: postgres is required for orders")
	}
	pool, err := pg.Open(ctx, a.Cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	store, err := admission.NewStore(a.Redis, admission.FromConfig(a.Cfg), a.Log, a.Metrics)
	if err != nil {
		return err
	}
	issuer, err := token.NewIssuer(a.Cfg.Token)
	if err != nil {
		return err
	}

	api.NewShopAPI(store, pg.NewOrders(pool), issuer, a.Cfg, a.Log, a.Metrics).Register(a.Server.Mux())

	// 입장 제어가 백프레셔를 걸 수 있으려면 이 서비스의 /readyz 가 정직해야 한다.
	a.Server.SetReady(func() error {
		rctx, rcancel := context.WithTimeout(ctx, a.Cfg.Admission.ShopHealthTimeout)
		defer rcancel()
		if err := a.Redis.Ping(rctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		return pool.Ping(rctx)
	})

	return a.Run(ctx)
}
