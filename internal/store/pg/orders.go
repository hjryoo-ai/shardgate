// Package pg 는 PostgreSQL 영속 저장소다.
//
// hot path 의 단일 진실은 Redis 이고(§7), PG 는 감사·분석·차단 근거처럼 남아야 하는
// 것들을 맡는다. 주문만 예외다 — "1인 1구매"는 사라져도 되는 값이 아니므로
// UNIQUE 제약으로 DB 가 직접 강제한다.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hjr/shardgate/internal/config"
)

// pgUniqueViolation 은 UNIQUE 제약 위반의 SQLSTATE 다.
const pgUniqueViolation = "23505"

// 주문 저장 결과로 구분해야 하는 상황들.
var (
	// ErrDuplicateOrder 는 이 이벤트에서 이미 구매한 계정/토큰이라는 뜻이다(1인 1구매).
	ErrDuplicateOrder = errors.New("pg: already purchased in this event")
	// ErrEntryReused 는 같은 입장 토큰으로 두 번째 주문이 들어왔다는 뜻이다.
	ErrEntryReused = errors.New("pg: entry token already used")
)

// Pool 은 연결 풀을 감싼다.
type Pool struct{ pool *pgxpool.Pool }

// Open 은 설정으로 연결 풀을 연다.
func Open(ctx context.Context, cfg config.Postgres) (*Pool, error) {
	pcfg, err := pgxpool.ParseConfig(string(cfg.DSN.Bytes()))
	if err != nil {
		// DSN 에는 비밀번호가 들어 있다. 파싱 오류에 원문을 실어 나르지 않는다.
		return nil, errors.New("pg: invalid dsn")
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = int32(cfg.MaxConns) //nolint:gosec // 설정에서 상한이 검증된다
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	return &Pool{pool: pool}, nil
}

// Close 는 연결 풀을 닫는다.
func (p *Pool) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}

// Ping 은 readiness 검사에 쓴다.
func (p *Pool) Ping(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg: ping: %w", err)
	}
	return nil
}

// Order 는 mock shop 의 주문이다.
type Order struct {
	ID        int64
	EventID   string
	TokenID   string
	AccountID string
	EntryJTI  string
	IdemKey   string
	SKU       string
	Qty       int
	CreatedAt time.Time
}

// Orders 는 주문 저장소다.
type Orders struct{ p *Pool }

// NewOrders 는 주문 저장소를 만든다.
func NewOrders(p *Pool) *Orders { return &Orders{p: p} }

const selectOrderByIdem = `
SELECT id, event_id, token_id, account_id, entry_jti, idempotency_key, sku, qty, created_at
FROM orders WHERE event_id = $1 AND idempotency_key = $2`

// FindByIdempotencyKey 는 같은 멱등키로 이미 만들어진 주문을 찾는다.
// 없으면 (nil, nil) 을 돌려준다.
func (o *Orders) FindByIdempotencyKey(ctx context.Context, eventID, key string) (*Order, error) {
	var ord Order
	err := o.p.pool.QueryRow(ctx, selectOrderByIdem, eventID, key).Scan(
		&ord.ID, &ord.EventID, &ord.TokenID, &ord.AccountID, &ord.EntryJTI,
		&ord.IdemKey, &ord.SKU, &ord.Qty, &ord.CreatedAt)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("pg: find order: %w", err)
	}
	return &ord, nil
}

const insertOrder = `
INSERT INTO orders (event_id, token_id, account_id, entry_jti, idempotency_key, sku, qty)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, created_at`

// Create 는 주문을 만든다.
//
// 1인 1구매와 입장 토큰 1회성은 애플리케이션 조건문이 아니라 DB UNIQUE 제약이
// 강제한다. 동시에 들어온 두 요청 사이에서 조건문은 언제든 뚫리지만 제약은 뚫리지 않는다.
func (o *Orders) Create(ctx context.Context, ord Order) (*Order, error) {
	err := o.p.pool.QueryRow(ctx, insertOrder,
		ord.EventID, ord.TokenID, ord.AccountID, ord.EntryJTI, ord.IdemKey, ord.SKU, ord.Qty,
	).Scan(&ord.ID, &ord.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			switch pgErr.ConstraintName {
			case "orders_event_id_entry_jti_key":
				return nil, ErrEntryReused
			case "orders_event_id_idempotency_key_key":
				// 같은 멱등키로 동시에 들어온 재시도다. 호출자가 다시 조회하면 된다.
				return nil, nil
			default:
				return nil, ErrDuplicateOrder
			}
		}
		return nil, fmt.Errorf("pg: create order: %w", err)
	}
	return &ord, nil
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
