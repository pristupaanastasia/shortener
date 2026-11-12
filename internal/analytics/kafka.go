package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic is the Kafka topic for click events. Records are keyed by link key,
// so all clicks of a link go to the same partition.
const Topic = "clicks"

// KafkaPublisher publishes clicks to Kafka asynchronously.
type KafkaPublisher struct {
	client  *kgo.Client
	logger  *slog.Logger
	dropped atomic.Int64
}

// NewKafkaPublisher returns a publisher for brokers. It does not wait for
// Kafka to be reachable: the redirect service must start without it.
func NewKafkaPublisher(brokers []string, logger *slog.Logger) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(Topic),
		kgo.AllowAutoTopicCreation(),
		// Records wait in memory and are sent in batches.
		kgo.ProducerLinger(50*time.Millisecond),
		// A bounded buffer: when Kafka is down and it fills up, new clicks
		// are dropped instead of growing memory without limit. Both limits
		// matter: the record count alone allows huge records.
		kgo.MaxBufferedRecords(100_000),
		kgo.MaxBufferedBytes(64<<20),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaPublisher{client: client, logger: logger}, nil
}

// Publish implements Publisher. TryProduce returns immediately; delivery
// errors arrive in the callback and only count as dropped events.
func (p *KafkaPublisher) Publish(c Click) {
	data, err := json.Marshal(c)
	if err != nil {
		p.drop(err)
		return
	}
	p.client.TryProduce(context.Background(), &kgo.Record{Key: []byte(c.Key), Value: data}, func(_ *kgo.Record, err error) {
		if err != nil {
			p.drop(err)
		}
	})
}

// drop counts a lost event. It logs the first loss and then every 1000th,
// so a Kafka outage does not flood the logs.
func (p *KafkaPublisher) drop(err error) {
	if n := p.dropped.Add(1); n == 1 || n%1000 == 0 {
		p.logger.Warn("click events dropped", "total", n, "err", err)
	}
}

// Dropped returns the number of events lost so far.
func (p *KafkaPublisher) Dropped() int64 { return p.dropped.Load() }

// Close sends buffered events (waiting at most a few seconds) and closes the
// client.
func (p *KafkaPublisher) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.client.Flush(ctx)
	p.client.Close()
}
