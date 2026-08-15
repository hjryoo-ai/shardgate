// Package app 은 서비스 엔트리포인트가 공유하는 조립 코드다.
// 설정 로드 → 로거 → 지표 → Redis → HTTP 서버까지의 반복을 한 곳에 모은다.
// 비즈니스 로직은 여기 들어오지 않는다.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/redisx"
)

// readyTimeout 은 /readyz 의 의존성 점검 상한이다.
const readyTimeout = 2 * time.Second

// App 은 서비스 공통 런타임 의존성이다.
type App struct {
	Cfg     *config.Config
	Log     *slog.Logger
	Metrics *obs.Metrics
	Redis   redis.UniversalClient
	Server  *httpx.Server
}

// New 는 환경변수로 서비스를 조립한다. requiredSecrets 는 config.RequireSecrets 인자다.
func New(service, defaultAddr string, requiredSecrets ...string) (*App, error) {
	cfg, err := config.Load(service, defaultAddr)
	if err != nil {
		return nil, err
	}
	if err := cfg.RequireSecrets(requiredSecrets...); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	log := obs.NewLogger(cfg.Service.LogLevel, service)

	// **적용된 설정을 기동 때 한 번 통째로 찍는다.**
	//
	// 손잡이마다 로그를 심는 방식은 손잡이가 늘 때마다 같은 구멍을 다시 연다 —
	// 이름이 컨테이너에 닿지 않아도 아무것도 실패하지 않고 팔만 조용히 바뀐다
	// (ROADMAP 결함 8). 다섯 서비스가 전부 이 함수를 지나므로, 여기 한 줄이
	// 아직 만들지 않은 손잡이까지 덮는다. `sweep.sh` 가 이 줄을 팔 정의와 대조한다.
	log.Info("effective config", slog.Any("env", cfg.EffectiveEnv()))

	metrics := obs.NewMetrics(service)
	rdb := redisx.New(cfg.Redis)
	srv := httpx.NewServer(cfg.Service, log, metrics)

	a := &App{Cfg: cfg, Log: log, Metrics: metrics, Redis: rdb, Server: srv}
	srv.SetReady(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		return nil
	})
	return a, nil
}

// Run 은 HTTP 서버를 돌린다.
func (a *App) Run(ctx context.Context) error { return a.Server.Run(ctx) }

// Close 는 공통 자원을 정리한다.
func (a *App) Close() error { return a.Redis.Close() }

// Main 은 main() 의 상투구를 줄여 준다. 오류면 로그 남기고 종료코드 1.
func Main(service string, run func() error) {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).
			Error("fatal", slog.String("service", service), slog.String("error", err.Error()))
		os.Exit(1)
	}
}
