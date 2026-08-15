package pg

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hjr/shardgate/internal/botscore"
)

// 적재 파라미터. 감사 로그는 hot path 가 아니므로 넉넉히 묶어서 쓴다.
const (
	auditBuffer  = 4096
	auditBatch   = 200
	auditFlushIn = time.Second
	auditTimeout = 5 * time.Second
	auditDrain   = 3 * time.Second
)

// Audit 은 조치·차단 이력을 PG 에 비동기로 적재한다(§6).
//
// 비동기인 이유는 §7 의 분리 원칙 그대로다. PG 가 느려지면 조치가 느려지고,
// 조치가 느려지면 탐지 파이프라인이 밀리고, 결국 대기열이 그 속도에 묶인다.
// 그래서 적재는 버퍼를 거치고, 버퍼가 차면 **기록을 버린다** — 감사 로그 한 줄보다
// 대기열이 도는 것이 중요하다. 버린 건은 반드시 센다.
type Audit struct {
	pool *Pool
	ch   chan any
	done chan struct{}
	log  *slog.Logger
	once sync.Once

	dropped atomic64
}

// NewAudit 은 감사 적재기를 만들고 백그라운드 루프를 띄운다.
func NewAudit(p *Pool, log *slog.Logger) *Audit {
	if log == nil {
		log = slog.Default()
	}
	a := &Audit{pool: p, ch: make(chan any, auditBuffer), done: make(chan struct{}), log: log}
	go a.loop()
	return a
}

// Audit 은 조치 기록을 큐에 넣는다. 절대 블로킹하지 않는다.
func (a *Audit) Audit(_ context.Context, rec botscore.AuditRecord) { a.enqueue(rec) }

// Block 은 차단 근거를 큐에 넣는다. 절대 블로킹하지 않는다.
func (a *Audit) Block(_ context.Context, rec botscore.BlockRecord) { a.enqueue(rec) }

// Dropped 는 버퍼가 차서 버린 기록 수다.
func (a *Audit) Dropped() int64 { return a.dropped.load() }

func (a *Audit) enqueue(rec any) {
	select {
	case a.ch <- rec:
	default:
		a.dropped.add(1)
	}
}

// Close 는 남은 기록을 비우고 닫는다.
func (a *Audit) Close() error {
	a.once.Do(func() {
		close(a.ch)
		select {
		case <-a.done:
		case <-time.After(auditDrain):
			a.log.Warn("audit flush timed out; dropping buffered records")
		}
		if n := a.dropped.load(); n > 0 {
			a.log.Warn("audit records dropped", slog.Int64("count", n))
		}
	})
	return nil
}

func (a *Audit) loop() {
	defer close(a.done)

	batch := make([]any, 0, auditBatch)
	ticker := time.NewTicker(auditFlushIn)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), auditTimeout)
		if err := a.write(ctx, batch); err != nil {
			a.log.Warn("audit batch dropped", slog.Any("error", err), slog.Int("count", len(batch)))
			a.dropped.add(int64(len(batch)))
		}
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-a.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= auditBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

const (
	insertAudit = `INSERT INTO queue_audit (event_id, token_id, shard_id, action, score, reason_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`

	insertBlock = `INSERT INTO blocks (event_id, token_id, fp_hash, score, evidence_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6)`
)

func (a *Audit) write(ctx context.Context, batch []any) error {
	b := &pgx.Batch{}
	for _, rec := range batch {
		switch r := rec.(type) {
		case botscore.AuditRecord:
			b.Queue(insertAudit, r.EventID, r.TokenID, r.Shard, r.Action,
				int(r.Score), mustJSON(r.Reason), r.At)
		case botscore.BlockRecord:
			b.Queue(insertBlock, r.EventID, r.TokenID, nullable(r.FPHash),
				int(r.Score), mustJSON(r.Evidence), r.At)
		}
	}
	if b.Len() == 0 {
		return nil
	}

	results := a.pool.pool.SendBatch(ctx, b)
	defer func() { _ = results.Close() }()
	for range b.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

// mustJSON 은 근거를 JSON 으로 만든다. 직렬화가 실패해도 기록 자체는 남긴다 —
// 근거가 비었더라도 "언제 무엇을 했는지"는 남아야 감사가 성립한다.
func mustJSON(v map[string]any) []byte {
	if v == nil {
		return []byte(`{}`)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"error":"reason could not be encoded"}`)
	}
	return b
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
