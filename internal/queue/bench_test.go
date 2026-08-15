//go:build integration

package queue

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/shard"
)

func BenchmarkEnqueue(b *testing.B) {
	h := newHarness(b, nil)
	h.clk.Advance(3 * time.Minute) // FIFO 구간: INCR 까지 포함한 최악의 경로

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		if _, err := h.store.Enqueue(ctx, EnqueueRequest{
			Shard: h.shard, TokenID: "bench" + strconv.Itoa(i),
			FPHash: "fp", IPPrefix: "203.0.113.0/24",
		}); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}
}

func BenchmarkPosition(b *testing.B) {
	h := newHarness(b, nil)
	h.clk.Advance(3 * time.Minute)

	ctx := context.Background()
	const seed = 1000
	for i := range seed {
		h.enqueue("tok" + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := h.store.Position(ctx, h.shard, "tok"+strconv.Itoa(i%seed)); err != nil {
			b.Fatalf("position: %v", err)
		}
	}
}

func BenchmarkHeartbeat(b *testing.B) {
	h := newHarness(b, nil)
	h.clk.Advance(3 * time.Minute)

	ctx := context.Background()
	const seed = 1000
	for i := range seed {
		h.enqueue("tok" + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := h.store.Heartbeat(ctx, h.shard, "tok"+strconv.Itoa(i%seed)); err != nil {
			b.Fatalf("heartbeat: %v", err)
		}
	}
}

// BenchmarkEnqueue100k 은 Phase 1 완료 기준(§10)인 10만 enqueue 를 측정한다.
//
// 티켓 오픈 직후를 흉내 내야 의미가 있으므로, 한 샤드에 순차로 밀어 넣지 않고
// 실제 배정기로 샤드에 흩뿌린 뒤 동시에 밀어 넣는다. 샤드가 슬롯에 분산돼
// 단일 핫키가 생기지 않는다는 §3.3 의 주장도 이 형태라야 확인된다.
//
//	make bench-queue 로 실행 (-benchtime=1x)
func BenchmarkEnqueue100k(b *testing.B) {
	const (
		total   = 100_000
		workers = 64
		shards  = 64
	)

	h := newHarness(b, nil)
	h.clk.Advance(30 * time.Second) // 추첨 구간 — 오픈 직후 폭주 상황

	assigner, err := shard.NewAssigner(config.NewSecret([]byte("bench-event-salt")), shards, 4096)
	if err != nil {
		b.Fatalf("assigner: %v", err)
	}

	ctx := context.Background()
	var round atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// 반복마다 토큰 네임스페이스를 바꾼다. 같은 토큰이면 멱등 경로를 타서
		// 측정 대상(신규 진입)이 아닌 것을 재는 셈이 된다.
		prefix := "r" + strconv.FormatInt(round.Add(1), 10) + "t"

		start := time.Now()
		var (
			wg     sync.WaitGroup
			next   atomic.Int64
			failed atomic.Int64
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := next.Add(1) - 1
					if i >= total {
						return
					}
					tok := prefix + strconv.FormatInt(i, 10)
					if _, err := h.store.Enqueue(ctx, EnqueueRequest{
						Shard: assigner.Assign(tok), TokenID: tok,
						FPHash: "fp", IPPrefix: "203.0.113.0/24",
					}); err != nil {
						failed.Add(1)
						return
					}
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)

		if n := failed.Load(); n > 0 {
			b.Fatalf("%d workers failed to enqueue", n)
		}
		b.ReportMetric(float64(total)/elapsed.Seconds(), "enqueue/s")
		b.ReportMetric(elapsed.Seconds(), "s/100k")
	}
}
