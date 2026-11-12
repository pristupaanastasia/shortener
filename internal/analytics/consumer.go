package analytics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Sink stores enriched clicks.
type Sink interface {
	Insert(ctx context.Context, rows []Row) error
}

// ConsumerConfig tunes batching.
type ConsumerConfig struct {
	Brokers       []string
	Group         string
	BatchSize     int           // insert when this many rows are buffered...
	FlushInterval time.Duration // ...or when this much time has passed
}

// Consumer reads clicks from Kafka and writes them to a Sink in batches.
//
// Delivery is at-least-once: offsets are committed only after a successful
// insert, so a crash between the two re-inserts that batch. Duplicated clicks
// are acceptable for statistics; lost ones would be too.
type Consumer struct {
	client *kgo.Client
	sink   Sink
	cfg    ConsumerConfig
	logger *slog.Logger
}

// NewConsumer returns a Consumer in group cfg.Group.
func NewConsumer(cfg ConsumerConfig, sink Sink, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(Topic),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Consumer{client: client, sink: sink, cfg: cfg, logger: logger}, nil
}

// Run consumes until ctx is cancelled, then flushes the buffered rows.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()
	var batch []Row
	deadline := time.Now().Add(c.cfg.FlushInterval)

	for {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := c.client.PollRecords(pollCtx, c.cfg.BatchSize-len(batch))
		cancel()

		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				c.logger.Warn("kafka fetch", "topic", topic, "partition", partition, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			row, err := Decode(r.Value)
			if err != nil {
				// A malformed event can never succeed; skip it instead of
				// blocking the partition forever.
				c.logger.Warn("skipping malformed click", "offset", r.Offset, "err", err)
				return
			}
			batch = append(batch, row)
		})

		stopping := ctx.Err() != nil
		if len(batch) >= c.cfg.BatchSize || !time.Now().Before(deadline) || stopping {
			if err := c.flush(ctx, batch, stopping); err != nil {
				return err
			}
			batch = batch[:0]
			deadline = time.Now().Add(c.cfg.FlushInterval)
		}
		if stopping {
			return nil
		}
	}
}

// flush inserts the batch, retrying until it succeeds, then commits offsets.
// When stopping (or when ctx is cancelled during a retry), it makes one last
// attempt with a fresh timeout; if that fails, it returns an error and does
// not commit, so the batch is read again after a restart.
func (c *Consumer) flush(ctx context.Context, batch []Row, stopping bool) error {
	for attempt := 1; len(batch) > 0; attempt++ {
		insertCtx, cancel := ctx, context.CancelFunc(func() {})
		if stopping {
			insertCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		}
		err := c.sink.Insert(insertCtx, batch)
		cancel()
		if err == nil {
			break
		}
		if stopping {
			return fmt.Errorf("insert %d clicks on shutdown, offsets not committed: %w", len(batch), err)
		}
		c.logger.Warn("insert clicks, retrying", "rows", len(batch), "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			stopping = true // one last attempt, then give up without committing
		case <-time.After(min(time.Duration(attempt)*time.Second, 30*time.Second)):
		}
	}

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := c.client.CommitUncommittedOffsets(commitCtx); err != nil {
		// The rows are stored; on restart they may be inserted again.
		c.logger.Warn("commit offsets", "err", err)
	}
	return nil
}
