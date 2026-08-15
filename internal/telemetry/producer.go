package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/hjr/shardgate/internal/config"
	"github.com/hjr/shardgate/internal/obs"
)

// 버퍼와 배치 파라미터.
const (
	// bufferSize 는 발행 대기 큐의 크기다. 이 이상 밀리면 이벤트를 버린다 —
	// 요청 경로를 세우느니 탐지 정확도를 조금 잃는 쪽이 낫다(불변식 5).
	bufferSize = 8192
	// batchSize / batchWait 는 Kafka 쓰기를 묶는 기준이다.
	batchSize = 256
	batchWait = 200 * time.Millisecond
	// writeTimeout 은 한 배치의 쓰기 상한이다.
	writeTimeout = 5 * time.Second
	// flushTimeout 은 종료 시 남은 이벤트를 비우는 데 주는 시간이다.
	flushTimeout = 3 * time.Second
)

// Publisher 는 신호를 발행한다.
type Publisher interface {
	// Publish 는 절대 블로킹하지 않고 절대 오류를 돌려주지 않는다.
	// 호출자(요청 핸들러)가 텔레메트리 때문에 실패할 길을 아예 만들지 않는다.
	Publish(e Event)
	Close() error
}

// Discard 는 Kafka 가 꺼져 있을 때 쓰는 무동작 구현이다.
type Discard struct{}

// Publish 는 아무것도 하지 않는다.
func (Discard) Publish(Event) {}

// Close 는 아무것도 하지 않는다.
func (Discard) Close() error { return nil }

// Producer 는 Kafka 로 신호를 보내는 비동기 발행기다.
type Producer struct {
	w    *kafka.Writer
	ch   chan Event
	done chan struct{}
	log  *slog.Logger
	met  *obs.Metrics
	once sync.Once
}

// NewProducer 는 Kafka 발행기를 만든다. Kafka 가 꺼져 있으면 Discard 를 돌려준다.
//
// 파티션 키는 shard_id 다(§7). 같은 샤드의 이벤트가 같은 파티션 → 같은 컨슈머로
// 가야 스코어러가 "샤드 하나의 상태만 들고" 스트림 처리를 할 수 있다.
// 파티션 수는 샤드 수 상한과 맞춰 둔다.
func NewProducer(cfg config.Kafka, log *slog.Logger, met *obs.Metrics) Publisher {
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return Discard{}
	}
	if log == nil {
		log = slog.Default()
	}

	p := &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.Hash{}, // 키(shard_id) 해시 → 같은 샤드는 같은 파티션
			BatchSize:    batchSize,
			BatchTimeout: batchWait,
			WriteTimeout: writeTimeout,
			RequiredAcks: kafka.RequireOne,
			Async:        false, // 배치 단위 오류를 우리가 직접 보고 싶다
			Compression:  kafka.Snappy,
		},
		ch:   make(chan Event, bufferSize),
		done: make(chan struct{}),
		log:  log,
		met:  met,
	}
	go p.loop()
	return p
}

// Publish 는 이벤트를 버퍼에 넣는다. 버퍼가 차 있으면 조용히 버린다.
//
// 여기서 블로킹하면 Kafka 의 지연이 그대로 heartbeat 응답 시간이 되고, 결국
// 대기열 전체가 탐지 파이프라인의 속도에 묶인다. 그건 정확히 §7 이 분리하려던 상황이다.
func (p *Producer) Publish(e Event) {
	if !e.Valid() {
		return
	}
	select {
	case p.ch <- e:
		if p.met != nil {
			p.met.TelemetryEvents.WithLabelValues(string(e.Kind), "out").Inc()
		}
	default:
		// 버린 것은 반드시 센다. 조용히 사라지면 탐지율이 왜 낮은지 알 수 없다.
		if p.met != nil {
			p.met.TelemetryErrors.WithLabelValues("buffer_full").Inc()
		}
	}
}

// Close 는 남은 이벤트를 비우고 발행기를 닫는다.
func (p *Producer) Close() error {
	var err error
	p.once.Do(func() {
		close(p.ch)
		select {
		case <-p.done:
		case <-time.After(flushTimeout):
			p.log.Warn("telemetry flush timed out; dropping buffered events")
		}
		err = p.w.Close()
	})
	if err != nil {
		return fmt.Errorf("telemetry: close writer: %w", err)
	}
	return nil
}

func (p *Producer) loop() {
	defer close(p.done)

	batch := make([]kafka.Message, 0, batchSize)
	ticker := time.NewTicker(batchWait)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		if err := p.w.WriteMessages(ctx, batch...); err != nil {
			// 실패해도 재시도하지 않는다. 밀린 배치를 붙들고 있으면 버퍼가 차고,
			// 그 다음에 버려지는 건 더 최신 신호다.
			p.log.Warn("telemetry batch dropped", slog.Any("error", err), slog.Int("count", len(batch)))
			if p.met != nil {
				p.met.TelemetryErrors.WithLabelValues("write").Add(float64(len(batch)))
			}
		}
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			msg, err := encode(e)
			if err != nil {
				if p.met != nil {
					p.met.TelemetryErrors.WithLabelValues("encode").Inc()
				}
				continue
			}
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func encode(e Event) (kafka.Message, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("telemetry: encode: %w", err)
	}
	return kafka.Message{
		Key:   []byte(e.Shard), // 파티션 키 = shard_id (§7)
		Value: payload,
		Time:  e.At,
	}, nil
}

// Decode 는 Kafka 에서 읽은 값을 이벤트로 되돌린다(스코어러용).
func Decode(payload []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return Event{}, fmt.Errorf("telemetry: decode: %w", err)
	}
	if !e.Valid() {
		return Event{}, errors.New("telemetry: incomplete event")
	}
	return e, nil
}
