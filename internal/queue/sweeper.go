package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
)

// Sweeper 는 샤드를 돌며 heartbeat 이 끊긴 사용자를 정리한다(§5 soft-evict).
//
// 왜 별도 루프인가: 정리를 요청 경로에 끼워 넣으면 운 나쁜 사용자 한 명이 남의
// 정리 비용까지 물게 되고, 대기열이 한산할 때는 아무도 정리되지 않는다.
//
// 커서를 샤드별로 들고 다니며 한 번에 SweepBatch 만큼만 훑는다. 100만 명이 몰린
// 상황에서도 스윕 한 번의 비용이 예측 가능해야 대기열 자체가 느려지지 않는다.
type Sweeper struct {
	store  *Store
	log    *slog.Logger
	met    *obs.Metrics
	every  time.Duration
	cursor map[string]int64
}

// NewSweeper 는 스윕 루프를 만든다.
func NewSweeper(store *Store, cfg config.Queue, log *slog.Logger, met *obs.Metrics) *Sweeper {
	if log == nil {
		log = slog.Default()
	}
	return &Sweeper{
		store:  store,
		log:    log,
		met:    met,
		every:  cfg.HeartbeatInterval,
		cursor: make(map[string]int64),
	}
}

// Run 은 ctx 가 끝날 때까지 주기적으로 스윕한다.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.every)
	defer ticker.Stop()

	s.log.Info("queue sweeper started",
		slog.Duration("every", s.every),
		slog.Duration("stale_after", s.store.StaleAfter()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.once(ctx)
		}
	}
}

// once 는 모든 샤드를 한 번씩 훑는다(샤드마다 커서 한 구간).
func (s *Sweeper) once(ctx context.Context) {
	shards, err := s.store.Shards(ctx)
	if err != nil {
		s.log.Warn("sweeper could not list shards", slog.Any("error", err))
		return
	}
	for _, sh := range shards {
		res, err := s.store.Sweep(ctx, sh, s.cursor[sh])
		if err != nil {
			// 샤드 하나의 실패가 나머지 샤드의 정리를 막지 않는다.
			s.log.Warn("sweep failed", slog.String("shard", sh), slog.Any("error", err))
			continue
		}
		s.cursor[sh] = res.NextOffset
		if res.Marked > 0 || res.Removed > 0 || res.Ghosts > 0 {
			s.log.Debug("swept",
				slog.String("shard", sh),
				slog.Int64("marked", res.Marked),
				slog.Int64("removed", res.Removed),
				slog.Int64("ghosts", res.Ghosts),
				slog.Int64("size", res.Size))
		}
	}
}
