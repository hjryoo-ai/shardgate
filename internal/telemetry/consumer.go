package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
)

// 컨슈머 파라미터.
const (
	fetchMinBytes = 1
	fetchMaxBytes = 10 << 20 // 10MiB
	commitEvery   = time.Second
	readBackoff   = time.Second

	// 토픽이 생기기를 기다리는 주기. 자동 생성 토픽은 **첫 produce 때** 만들어지므로,
	// 스코어러가 생산자보다 먼저 뜨면 없는 토픽에 조인하게 된다(waitForTopic 참고).
	topicPoll = 2 * time.Second
)

// Consumer 는 Kafka 에서 신호를 읽어 핸들러에 넘긴다.
//
// 파티션 키가 shard_id 이므로 한 컨슈머는 담당 파티션에 속한 샤드들의 이벤트만
// 받는다(§7). 덕분에 스코어러는 이벤트 전체 모수가 아니라 자기 샤드들의 창만
// 메모리에 들고 있으면 된다.
type Consumer struct {
	r   *kafka.Reader
	cfg config.Kafka
	log *slog.Logger
	met *obs.Metrics
}

// NewConsumer 는 컨슈머를 만든다.
func NewConsumer(cfg config.Kafka, log *slog.Logger, met *obs.Metrics) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			Topic:    cfg.Topic,
			GroupID:  cfg.GroupID,
			MinBytes: fetchMinBytes,
			MaxBytes: fetchMaxBytes,
			// 커밋을 묶는다. 중복 처리는 창에 신호가 한 번 더 들어가는 정도의
			// 영향이고, 그건 조치를 뒤집을 만한 차이가 아니다.
			CommitInterval: commitEvery,
		}),
		cfg: cfg,
		log: log,
		met: met,
	}
}

// waitForTopic 은 토픽에 파티션이 생길 때까지 기다린다.
//
// **없는 토픽에 컨슈머 그룹으로 조인하면 파티션 0개를 배정받고 그대로 안정된다.**
// 그 뒤에 토픽이 생겨도 이 컨슈머는 아무것도 읽지 않는다. 리밸런스를 촉발할 일이
// 없기 때문이다 — 멤버도 그대로고 구독도 그대로다.
//
// 이 조합이 실제로 일어난다. 토픽은 자동 생성이라 **첫 produce 때** 만들어지는데,
// compose 는 스코어러를 생산자(gate/queue)와 같이 띄우고 Kafka 는 볼륨이 없어
// `down` 마다 토픽이 사라진다. 그래서 "스택을 내렸다 올리면 탐지가 죽는다".
//
// 고약한 것은 아무것도 실패하지 않는다는 점이다. 브로커 헬스체크는 통과하고,
// 그룹 상태는 Stable 이고, ReadMessage 는 오류 없이 그냥 영원히 블록한다.
// 로그도 지표도 정상으로 보이는 채 **탐지만 통째로 꺼진다.** 실제로 이 상태로
// 측정을 한 번 돌려서 탐지율 0% 를 얻었다(docs/ROADMAP.md 결함 7).
func (c *Consumer) waitForTopic(ctx context.Context) error {
	if len(c.cfg.Brokers) == 0 {
		return errors.New("telemetry: no kafka brokers configured")
	}

	logged := false
	for {
		n, err := c.partitionCount(ctx)
		if err == nil && n > 0 {
			if logged {
				c.log.Info("telemetry topic ready",
					slog.String("topic", c.cfg.Topic), slog.Int("partitions", n))
			}
			return nil
		}
		if !logged {
			// 한 번만 알린다. 기다리는 것 자체는 정상이지만, 오래 걸리면
			// "왜 탐지가 안 도나"의 답이 여기 있어야 한다.
			c.log.Warn("waiting for telemetry topic — 그때까지 탐지는 돌지 않는다",
				slog.String("topic", c.cfg.Topic), slog.Any("error", err))
			logged = true
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(topicPoll):
		}
	}
}

// partitionCount 는 토픽의 파티션 수를 센다. 토픽이 없으면 0 이다.
func (c *Consumer) partitionCount(ctx context.Context) (int, error) {
	var d kafka.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.cfg.Brokers[0])
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	parts, err := conn.ReadPartitions(c.cfg.Topic)
	if err != nil {
		return 0, err
	}
	return len(parts), nil
}

// Close 는 컨슈머를 닫는다.
func (c *Consumer) Close() error {
	if err := c.r.Close(); err != nil {
		return err
	}
	return nil
}

// Run 은 ctx 가 끝날 때까지 이벤트를 읽어 handle 에 넘긴다.
//
// 읽기 실패로 루프를 끝내지 않는다. Kafka 가 잠깐 사라져도 스코어러는 살아 있다가
// 돌아오면 이어서 처리하면 된다 — 그 사이 대기열은 아무 영향 없이 진행된다(불변식 5).
func (c *Consumer) Run(ctx context.Context, handle func(Event)) error {
	// 토픽이 생긴 뒤에 그룹에 조인한다. 이유는 waitForTopic 에 적었다.
	if err := c.waitForTopic(ctx); err != nil {
		return err
	}

	c.log.Info("telemetry consumer started",
		slog.String("topic", c.r.Config().Topic),
		slog.String("group", c.r.Config().GroupID))

	for {
		msg, err := c.r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				// 종료 신호다 — ctx 취소나 리더 닫힘은 오류가 아니라 정상 종료다.
				// 여기서 오류를 돌려주면 정상 종료마다 스코어러가 실패로 기록된다.
				return nil //nolint:nilerr // 종료는 오류가 아니다
			}
			c.log.Warn("telemetry read failed", slog.Any("error", err))
			if c.met != nil {
				c.met.TelemetryErrors.WithLabelValues("read").Inc()
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(readBackoff):
			}
			continue
		}

		e, err := Decode(msg.Value)
		if err != nil {
			// 못 읽는 메시지 하나가 파티션을 막게 두지 않는다.
			if c.met != nil {
				c.met.TelemetryErrors.WithLabelValues("decode").Inc()
			}
			continue
		}

		if c.met != nil && !e.At.IsZero() {
			// 이벤트 발생 시각 대비 처리 지연 — 탐지가 얼마나 늦고 있는지의 지표다.
			c.met.ScorerLagSeconds.Set(time.Since(e.At).Seconds())
		}
		handle(e)
	}
}
