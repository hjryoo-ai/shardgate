package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/httpx"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

// QueueAPI 는 대기 중 사용자가 쓰는 두 엔드포인트를 제공한다.
//
//	GET  /api/v1/queue/status     순번 조회 (SSE 또는 폴링)
//	POST /api/v1/queue/heartbeat  생존 신호 + 텔레메트리
type QueueAPI struct {
	auth      authenticator
	store     *queue.Store
	tel       telemetry.Publisher
	log       *slog.Logger
	met       *obs.Metrics
	pollHint  time.Duration
	keepalive time.Duration
	// shardRate 는 예상 대기 시간 환산에 쓰는 샤드당 실효 입장률(명/분)이다.
	// 배분은 admission 이 하고, 여기서는 사용자에게 보여 줄 근사치만 만든다.
	shardRate float64
	// gates 는 §3.4 의 두 게이트다. 예상 시간이 게이트를 모르면 화면이 멈춘 것처럼
	// 보이고, 사용자는 그것을 고장으로 읽는다.
	gates queue.Gates
}

// NewQueueAPI 는 대기열 API 를 만든다.
func NewQueueAPI(store *queue.Store, issuer *token.Issuer, tel telemetry.Publisher,
	cfg *config.Config, log *slog.Logger, met *obs.Metrics,
) *QueueAPI {
	shardRate := float64(cfg.Admission.RatePerMin)
	if cfg.Event.ShardCount > 0 {
		shardRate /= float64(cfg.Event.ShardCount)
	}
	if tel == nil {
		tel = telemetry.Discard{}
	}
	// 게이트가 꺼져 있으면 제로값이라 예상 시간이 예전과 똑같이 나온다.
	gates := queue.Gates{MinDwell: cfg.Admission.MinDwell}
	if cfg.Admission.AfterLottery && !cfg.Event.OpenAt.IsZero() {
		gates.AdmitOpensAt = cfg.Event.OpenAt.Add(cfg.Event.LotteryWindow)
	}
	return &QueueAPI{
		auth: authenticator{
			issuer: issuer, eventID: cfg.Event.ID,
			v4bits: cfg.Token.IPv4PrefixBits, v6bits: cfg.Token.IPv6PrefixBits, met: met,
		},
		store:     store,
		tel:       tel,
		log:       log,
		met:       met,
		pollHint:  cfg.Queue.StatusPollHint,
		keepalive: cfg.Queue.SSEKeepalive,
		shardRate: shardRate,
		gates:     gates,
	}
}

// Register 는 라우트를 mux 에 붙인다.
func (q *QueueAPI) Register(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/queue/status", httpx.Handler(q.status))
	mux.Handle("POST /api/v1/queue/heartbeat", httpx.Handler(q.heartbeat))
}

// StatusResponse 는 순번 응답이다.
//
// 여기 담긴 숫자는 전부 서버가 Redis 에서 원자적으로 읽은 값이다. 클라이언트가
// 보낸 순번은 어떤 경우에도 반영되지 않는다 — 순번은 서버가 유일한 진실이다.
type StatusResponse struct {
	Event      string `json:"event"`
	Shard      string `json:"shard"`
	State      string `json:"state"`
	Segment    string `json:"segment,omitempty"`
	Rank       int64  `json:"rank"`
	Ahead      int64  `json:"ahead"`
	ShardSize  int64  `json:"shard_size"`
	EstimateMS int64  `json:"estimated_wait_ms"`
	PollAfter  int64  `json:"poll_after_ms"`
}

func (q *QueueAPI) snapshot(ctx context.Context, c token.Claims) (StatusResponse, error) {
	snap, err := q.store.Position(ctx, c.Shard, c.TokenID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return StatusResponse{}, err
		}
		q.log.Error("position lookup failed", slog.Any("error", err), slog.String("shard", c.Shard))
		return StatusResponse{}, httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "queue is temporarily unavailable")
	}
	return StatusResponse{
		Event:      c.EventID,
		Shard:      snap.Shard,
		State:      string(snap.Status),
		Segment:    string(snap.Segment),
		Rank:       snap.Rank,
		Ahead:      snap.Ahead(),
		ShardSize:  snap.Size,
		EstimateMS: snap.EstimateWaitAt(time.Now(), q.shardRate, q.gates).Milliseconds(),
		PollAfter:  q.pollHint.Milliseconds(),
	}, nil
}

// status 는 순번을 돌려준다. Accept: text/event-stream 이면 SSE 로 계속 흘려보낸다.
func (q *QueueAPI) status(w http.ResponseWriter, r *http.Request) error {
	claims, err := q.auth.verify(r, queueToken(r), token.KindQueue)
	if err != nil {
		return err
	}

	if !wantsSSE(r) {
		snap, err := q.snapshot(r.Context(), claims)
		if err != nil {
			return err
		}
		httpx.WriteJSON(w, http.StatusOK, snap)
		return nil
	}
	return q.stream(w, r, claims)
}

func wantsSSE(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept") {
		if v == "text/event-stream" || len(v) >= 17 && v[:17] == "text/event-stream" {
			return true
		}
	}
	return false
}

// stream 은 SSE 로 순번을 주기적으로 밀어 준다.
//
// 대기실은 수십만 개의 연결이 동시에 열려 있는 곳이다. 그래서 갱신 주기는 서버가
// 정하고(pollHint), 클라이언트가 더 자주 요구할 수 있는 수단을 주지 않는다.
func (q *QueueAPI) stream(w http.ResponseWriter, r *http.Request, claims token.Claims) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return httpx.NewAPIError(http.StatusInternalServerError, "sse_unsupported", "streaming is not supported")
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ctx := r.Context()
	ticker := time.NewTicker(q.pollHint)
	defer ticker.Stop()
	keepalive := time.NewTicker(q.keepalive)
	defer keepalive.Stop()

	send := func() bool {
		snap, err := q.snapshot(ctx, claims)
		if err != nil {
			return false
		}
		if err := writeSSE(w, "status", snap); err != nil {
			return false
		}
		flusher.Flush()
		// 입장 대상이 아니게 된 상태(입장 완료·차단 등)는 더 보낼 것이 없다.
		return snap.State == string(queue.StatusWaiting) || snap.State == string(queue.StatusEvicting)
	}

	if !send() {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-keepalive.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return nil
			}
			flusher.Flush()
		case <-ticker.C:
			if !send() {
				return nil
			}
		}
	}
}

// HeartbeatRequest 는 생존 신호 본문이다.
//
// 클라이언트가 보내는 값은 전부 **신호**일 뿐 판단 근거가 아니다(§4-L4).
// 위조할 수 있는 데이터이므로 여기서 상태를 바꾸지 않고, Phase 4 에서 Kafka 로
// 흘려보내 점수 파이프라인이 누적 판단하게 한다(불변식 3).
type HeartbeatRequest struct {
	// PointerEntropy 는 포인터/터치 이벤트의 다양성 지표다.
	PointerEntropy float64 `json:"pointer_entropy,omitempty"`
	// Visible 은 대기 탭이 화면에 떠 있는지다.
	Visible bool `json:"visible,omitempty"`
	// Events 는 페이지 이벤트 이름의 발생 순서다.
	Events []string `json:"events,omitempty"`
}

// HeartbeatResponse 는 생존 신호 응답이다.
type HeartbeatResponse struct {
	State     string `json:"state"`
	Rank      int64  `json:"rank"`
	Beats     int64  `json:"beats"`
	Revived   bool   `json:"revived"`
	NextAfter int64  `json:"next_after_ms"`
}

// heartbeat 는 생존 신호를 기록한다.
func (q *QueueAPI) heartbeat(w http.ResponseWriter, r *http.Request) error {
	claims, err := q.auth.verify(r, queueToken(r), token.KindQueue)
	if err != nil {
		return err
	}

	// 본문은 선택이다. 텔레메트리를 못 보내는 클라이언트를 대기열에서 밀어내지 않는다.
	var body HeartbeatRequest
	if r.ContentLength > 0 {
		if err := httpx.DecodeJSON(w, r, &body); err != nil {
			return err
		}
	}

	beat, err := q.store.Heartbeat(r.Context(), claims.Shard, claims.TokenID)
	if err != nil {
		q.log.Error("heartbeat failed", slog.Any("error", err), slog.String("shard", claims.Shard))
		return httpx.NewAPIError(http.StatusServiceUnavailable, "unavailable", "queue is temporarily unavailable")
	}
	// 간격은 서버가 관측한 값을 쓴다(beat.Interval). 클라이언트가 보고한 간격을
	// 쓰면 그 값만 사람처럼 꾸며 보내는 것으로 §4-L4 신호를 통째로 무력화할 수 있다.
	q.tel.Publish(telemetry.Event{
		Kind: telemetry.KindHeartbeat, EventID: claims.EventID,
		Shard: claims.Shard, TokenID: claims.TokenID, At: time.Now(),
		FPHash: claims.FPHash, IPPrefix: claims.IPPrefix,
		IntervalMS:     beat.Interval.Milliseconds(),
		PointerEntropy: body.PointerEntropy,
		Visible:        body.Visible,
	})

	httpx.WriteJSON(w, http.StatusOK, HeartbeatResponse{
		State:     string(beat.Status),
		Rank:      beat.Rank,
		Beats:     beat.Count,
		Revived:   beat.Revived,
		NextAfter: q.pollHint.Milliseconds(),
	})
	return nil
}
