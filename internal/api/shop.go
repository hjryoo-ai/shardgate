package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/hjr/shardgate/internal/admission"
	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/store/pg"
	"github.com/hjr/shardgate/internal/token"
)

// maxQty 는 1회 주문 수량 상한이다(mock: 1인 1개).
const maxQty = 1

// ShopAPI 는 mock 구매 API 다.
//
//	POST /api/v1/orders
//
// 세 겹으로 1인 1구매를 강제한다:
//  1. 서명된 입장 토큰이 있어야 한다        — 대기열을 거치지 않은 요청 차단
//  2. Redis 에서 그 토큰을 소각한다          — 같은 입장권의 두 번째 사용 차단(불변식 2)
//  3. PG UNIQUE 제약                        — 위 둘을 모두 통과한 경합의 최종 판정
//
// 3번이 필요한 이유: 1·2 는 애플리케이션 흐름이라 서비스 인스턴스가 여러 대면
// 어딘가에 틈이 생긴다. 제약 조건은 그 틈이 없다.
type ShopAPI struct {
	auth   authenticator
	admit  *admission.Store
	orders *pg.Orders
	log    *slog.Logger
	met    *obs.Metrics
	sku    string
}

// NewShopAPI 는 구매 API 를 만든다.
func NewShopAPI(admit *admission.Store, orders *pg.Orders, issuer *token.Issuer, cfg *config.Config, log *slog.Logger, met *obs.Metrics) *ShopAPI {
	return &ShopAPI{
		auth: authenticator{
			issuer: issuer, eventID: cfg.Event.ID,
			v4bits: cfg.Token.IPv4PrefixBits, v6bits: cfg.Token.IPv6PrefixBits, met: met,
		},
		admit:  admit,
		orders: orders,
		log:    log,
		met:    met,
		sku:    cfg.Event.ID,
	}
}

// Register 는 라우트를 mux 에 붙인다.
func (s *ShopAPI) Register(mux *http.ServeMux) {
	mux.Handle("POST /api/v1/orders", httpx.Handler(s.create))
}

// OrderRequest 는 주문 본문이다.
type OrderRequest struct {
	AccountID string `json:"account_id"`
	SKU       string `json:"sku,omitempty"`
	Qty       int    `json:"qty,omitempty"`
}

// OrderResponse 는 주문 결과다.
type OrderResponse struct {
	OrderID   int64     `json:"order_id"`
	EventID   string    `json:"event_id"`
	AccountID string    `json:"account_id"`
	SKU       string    `json:"sku"`
	Qty       int       `json:"qty"`
	CreatedAt time.Time `json:"created_at"`
	// Replayed 는 같은 멱등키의 재시도에 저장된 결과를 그대로 돌려줬다는 뜻이다.
	Replayed bool `json:"replayed"`
}

func (s *ShopAPI) create(w http.ResponseWriter, r *http.Request) error {
	claims, err := s.auth.verify(r, entryToken(r), token.KindEntry)
	if err != nil {
		return err
	}

	idem := r.Header.Get(IdempotencyHeader)
	if idem == "" {
		return httpx.NewAPIError(http.StatusBadRequest, "idempotency_key_required",
			"Idempotency-Key header is required")
	}

	var body OrderRequest
	if err := httpx.DecodeJSON(w, r, &body); err != nil {
		return err
	}
	if body.AccountID == "" {
		return httpx.NewAPIError(http.StatusBadRequest, "account_required", "account_id is required")
	}
	if body.Qty == 0 {
		body.Qty = 1
	}
	if body.Qty < 1 || body.Qty > maxQty {
		return httpx.NewAPIError(http.StatusBadRequest, "qty_invalid", "qty must be 1")
	}
	if body.SKU == "" {
		body.SKU = s.sku
	}

	ctx := r.Context()

	// 재시도 먼저. 이미 만들어진 주문이 있으면 입장 토큰을 소각하지 않고 그대로 돌려준다.
	// 소각부터 하면 네트워크 재시도 한 번에 사용자가 입장권을 잃는다.
	if prev, err := s.orders.FindByIdempotencyKey(ctx, claims.EventID, idem); err != nil {
		s.log.Error("idempotency lookup failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "order service is temporarily unavailable")
	} else if prev != nil {
		s.count("replayed")
		httpx.WriteJSON(w, http.StatusOK, toResponse(prev, true))
		return nil
	}

	// 입장 토큰 소각(불변식 2). 여기를 통과한 요청만 주문을 만들 수 있다.
	burn, err := s.admit.Burn(ctx, claims.Shard, claims.JTI, claims.TokenID)
	if err != nil {
		s.log.Error("entry burn failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "order service is temporarily unavailable")
	}
	switch burn {
	case admission.BurnOK:
	case admission.BurnMissing:
		s.count("entry_spent")
		return httpx.NewAPIError(http.StatusConflict, "entry_spent", "entry token was already used or has expired")
	default:
		// 서명은 유효한데 발행 기록의 주인이 다르다 — 탈취 시도의 신호다.
		s.count("entry_mismatch")
		s.log.Warn("entry token owner mismatch",
			slog.String("shard", claims.Shard), slog.String("jti", claims.JTI))
		return httpx.NewAPIError(http.StatusForbidden, "entry_mismatch", "entry token does not belong to this queue token")
	}

	ord, err := s.orders.Create(ctx, pg.Order{
		EventID: claims.EventID, TokenID: claims.TokenID, AccountID: body.AccountID,
		EntryJTI: claims.JTI, IdemKey: idem, SKU: body.SKU, Qty: body.Qty,
	})
	switch {
	case errors.Is(err, pg.ErrDuplicateOrder):
		s.count("duplicate")
		return httpx.NewAPIError(http.StatusConflict, "already_purchased", "this account already purchased in this event")
	case errors.Is(err, pg.ErrEntryReused):
		s.count("entry_reused")
		return httpx.NewAPIError(http.StatusConflict, "entry_reused", "entry token was already used")
	case err != nil:
		s.log.Error("order create failed", slog.Any("error", err))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "order service is temporarily unavailable")
	case ord == nil:
		// 같은 멱등키가 방금 동시에 들어왔다. 저장된 쪽을 그대로 돌려준다.
		prev, ferr := s.orders.FindByIdempotencyKey(ctx, claims.EventID, idem)
		if ferr != nil || prev == nil {
			return httpx.NewAPIError(http.StatusConflict, "idempotency_conflict", "concurrent request with the same Idempotency-Key")
		}
		s.count("replayed")
		httpx.WriteJSON(w, http.StatusOK, toResponse(prev, true))
		return nil
	}

	s.count("created")
	httpx.WriteJSON(w, http.StatusCreated, toResponse(ord, false))
	return nil
}

func (s *ShopAPI) count(result string) {
	if s.met != nil {
		s.met.Orders.WithLabelValues(result).Inc()
	}
}

func toResponse(o *pg.Order, replayed bool) OrderResponse {
	return OrderResponse{
		OrderID: o.ID, EventID: o.EventID, AccountID: o.AccountID,
		SKU: o.SKU, Qty: o.Qty, CreatedAt: o.CreatedAt, Replayed: replayed,
	}
}

// entryToken 은 요청에서 입장 토큰을 꺼낸다.
// 큐 토큰과 다른 헤더를 쓰는 이유: 두 토큰이 같은 자리에 오면 큐 토큰으로 구매를
// 시도하는 실수가 조용히 통과할 여지가 생긴다. 종류 검증이 있지만 자리부터 분리한다.
func entryToken(r *http.Request) string {
	if t := r.Header.Get(EntryHeader); t != "" {
		return t
	}
	return bearer(r)
}
