package shard

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// WaitingCounter 는 현재 대기 인원을 알려준다(queue.Store 가 구현한다).
type WaitingCounter interface {
	TotalWaiting(ctx context.Context) (int64, error)
}

// Grower 는 진입자가 예상을 넘어서면 샤드 수를 늘린다(§3.1).
//
// 왜 요청마다 하지 않는가: 확장 판단에는 이벤트 전체 대기 인원이 필요하고, 그건
// 샤드 수만큼의 읽기다. 진입 경로에 넣으면 폭주할 때 가장 비싸지는 곳에 비용을
// 더하게 된다. 주기적으로 한 번씩만 보고 배정기의 N 을 올린다.
//
// 이미 발급된 토큰은 재배치되지 않는다. 늘어난 N 은 이후 발급되는 토큰에만
// 적용되고, 기존 사용자는 자기 토큰에 적힌 샤드에서 순번을 그대로 유지한다.
type Grower struct {
	assigner  *Assigner
	counter   WaitingCounter
	shardSize int
	every     time.Duration
	log       *slog.Logger
}

// NewGrower 는 샤드 확장 루프를 만든다.
func NewGrower(a *Assigner, c WaitingCounter, shardSize int, every time.Duration, log *slog.Logger) *Grower {
	if log == nil {
		log = slog.Default()
	}
	return &Grower{assigner: a, counter: c, shardSize: shardSize, every: every, log: log}
}

// Run 은 ctx 가 끝날 때까지 주기적으로 샤드 수를 점검한다.
func (g *Grower) Run(ctx context.Context) {
	ticker := time.NewTicker(g.every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.once(ctx)
		}
	}
}

func (g *Grower) once(ctx context.Context) {
	waiting, err := g.counter.TotalWaiting(ctx)
	if err != nil {
		// 확장 판단에 실패해도 진입은 계속된다. 샤드가 목표보다 커질 뿐이다.
		g.log.Warn("could not read waiting count for shard growth", slog.Any("error", err))
		return
	}

	before := g.assigner.Count()
	after, err := g.assigner.EnsureCapacity(int(waiting), g.shardSize)
	if err != nil && !errors.Is(err, ErrNoCapacity) {
		g.log.Warn("shard growth failed", slog.Any("error", err))
		return
	}
	if errors.Is(err, ErrNoCapacity) && after != before {
		// 상한에 걸렸다. 진입은 막지 않지만 샤드가 목표 크기보다 커진다는 뜻이므로
		// 운영자가 알아야 한다 — 상한은 Kafka 파티션 수와 맞춰져 있다.
		g.log.Warn("shard count hit its ceiling",
			slog.Int("shard_count", after), slog.Int64("waiting", waiting))
	}
	if after != before {
		g.log.Info("shard count grown",
			slog.Int("from", before), slog.Int("to", after), slog.Int64("waiting", waiting))
	}
}
