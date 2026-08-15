package botscore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/keys"
	"github.com/hjr/shardgate/internal/obs"
	"github.com/hjr/shardgate/internal/shard"
	lua "github.com/hjr/shardgate/scripts/lua"

	"github.com/hjr/shardgate/internal/redisx"
)

var (
	scriptApply   = redisx.NewScript("apply_action.lua", lua.MustRead("apply_action.lua"))
	scriptMove    = redisx.NewScript("move_shard.lua", lua.MustRead("move_shard.lua"))
	scriptRestore = redisx.NewScript("restore_shard.lua", lua.MustRead("restore_shard.lua"))
)

// ErrUnexpectedReply 는 Lua 응답 형태가 계약과 다를 때다.
var ErrUnexpectedReply = errors.New("botscore: unexpected lua reply")

// Sink 는 조치 기록을 받는 곳이다(PG 감사 로그). nil 이어도 조치는 진행된다 —
// 감사 적재가 늦거나 실패한다고 방어가 멈추면 안 된다.
type Sink interface {
	Audit(ctx context.Context, rec AuditRecord)
	Block(ctx context.Context, rec BlockRecord)
}

// AuditRecord 는 조치 한 건의 감사 기록이다(§6 queue_audit).
type AuditRecord struct {
	EventID string
	TokenID string
	Shard   string
	Action  string
	Score   float64
	Reason  map[string]any
	At      time.Time
}

// BlockRecord 는 차단 근거다(§6 blocks). 원본 지문은 담지 않는다 — 해시만(불변식 6).
type BlockRecord struct {
	EventID  string
	TokenID  string
	FPHash   string
	Score    float64
	Evidence map[string]any
	At       time.Time
}

// Actuator 는 판정을 실제 상태 전이로 옮긴다.
type Actuator struct {
	rdb     redis.UniversalClient
	cfg     config.BotScore
	userTTL time.Duration
	eventID string
	log     *slog.Logger
	met     *obs.Metrics
	sink    Sink
	now     func() time.Time
}

// NewActuator 는 조치 실행기를 만든다.
func NewActuator(rdb redis.UniversalClient, eventID string, cfg config.BotScore,
	userTTL time.Duration, sink Sink, log *slog.Logger, met *obs.Metrics,
) *Actuator {
	if log == nil {
		log = slog.Default()
	}
	return &Actuator{
		rdb: rdb, cfg: cfg, userTTL: userTTL, eventID: eventID,
		log: log, met: met, sink: sink, now: time.Now,
	}
}

// Apply 는 판정 묶음을 적용한다. 하나가 실패해도 나머지는 계속한다.
func (a *Actuator) Apply(ctx context.Context, decisions []Decision) {
	for _, d := range decisions {
		if err := a.applyOne(ctx, d); err != nil {
			a.log.Warn("action failed",
				slog.String("shard", d.Shard), slog.String("action", string(d.Action)),
				slog.Any("error", err))
		}
	}
}

func (a *Actuator) applyOne(ctx context.Context, d Decision) error {
	if err := shard.Validate(d.Shard); err != nil {
		return err
	}

	var (
		applied string
		err     error
	)
	switch d.Action {
	case ActionObserve:
		applied, err = a.runApply(ctx, d, "observe")
	case ActionHold, ActionBlock:
		applied, err = a.runApply(ctx, d, string(d.Action))
	case ActionGreylist:
		applied, err = a.move(ctx, d)
	case ActionRestore:
		applied, err = a.restore(ctx, d)
	default:
		return fmt.Errorf("botscore: unknown action %q", d.Action)
	}
	if err != nil {
		return err
	}

	if a.met != nil {
		a.met.Actions.WithLabelValues(a.eventID, applied).Inc()
	}
	// 관측만 한 건은 기록하지 않는다 — 모든 사용자가 매 창마다 감사 로그를
	// 한 줄씩 남기면 감사 테이블이 대기열보다 커진다.
	if applied != "observe" && applied != "noop" {
		a.record(ctx, d, applied)
	}
	return nil
}

// runApply 는 apply_action.lua 를 실행한다(점수 기록 + observe/hold/block).
//
// greylist 대기열 키를 함께 넘긴다. 조치는 greylist 안에 있는 사용자에게도 내려오고
// (점수가 계속 오르기 때문이다 — 불변식 7), 그때 멤버는 원 대기열이 아니라 greylist
// 쪽에 있다. 두 키는 같은 슬롯이라 한 Lua 안에서 함께 만질 수 있다(§3.3).
func (a *Actuator) runApply(ctx context.Context, d Decision, action string) (string, error) {
	ev := a.eventID
	grey, err := shard.Greylist(d.Shard)
	if err != nil {
		return "", err
	}
	res, err := scriptApply.Run(ctx, a.rdb,
		[]string{
			keys.Queue(ev, d.Shard),
			keys.Hold(ev, d.Shard),
			keys.Score(ev, d.Shard),
			keys.User(ev, d.Shard, d.TokenID),
			keys.Queue(ev, grey),
		},
		d.TokenID, int64(d.Score), action, a.now().UnixMilli(), a.userTTL.Milliseconds(),
	)
	if err != nil {
		return "", err
	}
	return firstString(res)
}

// move 는 greylist 샤드로 옮긴다.
func (a *Actuator) move(ctx context.Context, d Decision) (string, error) {
	grey, err := shard.Greylist(d.Shard)
	if err != nil {
		return "", err
	}
	if shard.IsGreylist(d.Shard) {
		// 이미 greylist 안이다. 점수만 갱신한다.
		return a.runApply(ctx, d, "observe")
	}

	ev := a.eventID
	res, err := scriptMove.Run(ctx, a.rdb,
		[]string{
			keys.Queue(ev, d.Shard),
			keys.Queue(ev, grey),
			keys.Score(ev, d.Shard),
			keys.User(ev, d.Shard, d.TokenID),
		},
		d.TokenID, grey, int64(d.Score), a.now().UnixMilli(), a.userTTL.Milliseconds(),
	)
	if err != nil {
		return "", err
	}
	applied, err := firstString(res)
	if err != nil {
		return "", err
	}

	// 의심도를 올려 둔다. 다음 챌린지의 난이도가 여기서 나온다(§4-L2 적응형 난이도).
	if applied == "greylist" {
		a.raiseSuspicion(ctx, d)
	}
	return applied, nil
}

// restore 는 greylist/hold 를 풀고 원 샤드의 원 순번으로 되돌린다.
func (a *Actuator) restore(ctx context.Context, d Decision) (string, error) {
	origin, err := shard.Origin(d.Shard)
	if err != nil {
		return "", err
	}
	grey, err := shard.Greylist(d.Shard)
	if err != nil {
		return "", err
	}

	ev := a.eventID
	res, err := scriptRestore.Run(ctx, a.rdb,
		[]string{
			keys.Queue(ev, origin),
			keys.Queue(ev, grey),
			keys.Hold(ev, d.Shard),
			keys.User(ev, d.Shard, d.TokenID),
		},
		d.TokenID, origin, a.now().UnixMilli(), a.userTTL.Milliseconds(),
	)
	if err != nil {
		return "", err
	}
	return firstString(res)
}

// raiseSuspicion 은 적응형 PoW 난이도의 입력을 올린다.
//
// 주체는 지문 해시 또는 IP 프리픽스다 — 토큰은 버리고 새로 받으면 그만이지만,
// 기기와 대역은 그만큼 싸게 바꿀 수 없다. 실패해도 조치 자체는 이미 적용됐으므로
// 로그만 남기고 넘어간다.
func (a *Actuator) raiseSuspicion(ctx context.Context, d Decision) {
	for _, subject := range a.subjects(ctx, d) {
		if subject == "" {
			continue
		}
		key := keys.Suspicion(a.eventID, subject)
		if err := a.rdb.Incr(ctx, key).Err(); err != nil {
			a.log.Debug("suspicion bump failed", slog.Any("error", err))
			continue
		}
		if err := a.rdb.Expire(ctx, key, a.cfg.SuspicionTTL).Err(); err != nil {
			a.log.Debug("suspicion ttl failed", slog.Any("error", err))
		}
	}
}

func (a *Actuator) subjects(ctx context.Context, d Decision) []string {
	vals, err := a.rdb.HMGet(ctx, keys.User(a.eventID, d.Shard, d.TokenID), "fp_hash", "ip_prefix").Result()
	if err != nil || len(vals) != 2 {
		return nil
	}
	out := make([]string, 0, 2)
	for _, v := range vals {
		if s, ok := v.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// record 는 조치를 감사 로그에 남긴다.
func (a *Actuator) record(ctx context.Context, d Decision, applied string) {
	if a.sink == nil {
		return
	}

	reason := map[string]any{
		"signals":      d.Signals,
		"contributing": d.Contributing,
	}
	if d.CappedFrom != "" {
		// 왜 차단하지 않았는지를 남기는 것이 왜 차단했는지만큼 중요하다.
		reason["capped_from"] = string(d.CappedFrom)
		reason["min_signals_to_block"] = a.cfg.MinSignalsToBlock
	}

	now := a.now()
	a.sink.Audit(ctx, AuditRecord{
		EventID: a.eventID, TokenID: d.TokenID, Shard: d.Shard,
		Action: applied, Score: d.Score, Reason: reason, At: now,
	})

	if applied == "block" {
		fp := ""
		if subs := a.subjects(ctx, d); len(subs) > 0 {
			fp = subs[0]
		}
		a.sink.Block(ctx, BlockRecord{
			EventID: a.eventID, TokenID: d.TokenID, FPHash: fp,
			Score: d.Score, Evidence: reason, At: now,
		})
	}
}

func firstString(v any) (string, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", fmt.Errorf("%w: got %T", ErrUnexpectedReply, v)
	}
	s, ok := arr[0].(string)
	if !ok {
		return "", fmt.Errorf("%w: first element is %T", ErrUnexpectedReply, arr[0])
	}
	return s, nil
}
