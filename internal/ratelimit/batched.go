package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Batched is an approximate fixed-window limiter. Allow decides in memory,
// without a Redis round trip, and a background loop flushes the counted
// increments to Redis every interval, reading back the global count of each
// window (which includes other instances' requests).
//
// The trade-off is accuracy: an instance learns about other instances'
// requests only when it flushes, so all instances together can admit up to
// one sync round's worth of extra requests per window (the interval plus the
// flush's own round trip). In exchange, the per-request Redis transaction
// (12.8% of service CPU at 10K creates/s) is gone.
//
// A failed flush is never resent: Redis may already have applied it (e.g. a
// read timeout after execution), and resending would count those requests
// twice. They stay counted locally until their window ends, so during a
// Redis outage each instance keeps enforcing limits on its own traffic.
//
// The client must not retry commands (MaxRetries: -1), for the same reason.
// Counters in Redis use the same keys as Limiter.
type Batched struct {
	redis    *redis.Client
	interval time.Duration
	now      func() time.Time
	logger   *slog.Logger

	mu       sync.Mutex
	counters map[counterKey]*counter

	stop chan struct{}
	done chan struct{}
}

// maxBatch bounds the counters sent in one pipeline, so a large backlog
// (e.g. many subjects) does not turn into one request that times out.
const maxBatch = 500

type counterKey struct {
	redisKey string
	window   time.Duration
}

// counter is one subject's window. Allow counts all four parts.
type counter struct {
	start       time.Time
	global      int64 // last count read from Redis (includes our synced requests)
	pending     int64 // counted here, not sent yet
	inflight    int64 // sent in the flush that is running now
	unconfirmed int64 // sent in a failed flush: maybe applied, never resent
}

func (c *counter) count() int64 { return c.global + c.pending + c.inflight + c.unconfirmed }

// NewBatched returns a Batched limiter that flushes every interval. rdb must
// not retry commands (MaxRetries: -1). Call Close to stop it and flush the
// remaining increments.
func NewBatched(rdb *redis.Client, interval time.Duration, logger *slog.Logger, opts ...Option) *Batched {
	l := &Limiter{now: time.Now}
	for _, opt := range opts {
		opt(l)
	}
	b := &Batched{
		redis:    rdb,
		interval: interval,
		now:      l.now,
		logger:   logger,
		counters: make(map[counterKey]*counter),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go b.loop()
	return b
}

// Allow records a request by subject and reports whether it is within limit
// requests per window, by the instance's current view of the global count.
// It never returns an error.
func (b *Batched) Allow(_ context.Context, subject string, limit int, window time.Duration) (Result, error) {
	start := b.now().Truncate(window)
	k := counterKey{redisKey: fmt.Sprintf("rl:%s:%d", subject, start.Unix()), window: window}

	b.mu.Lock()
	c, ok := b.counters[k]
	if !ok {
		c = &counter{start: start}
		b.counters[k] = c
	}
	c.pending++
	count := c.count()
	b.mu.Unlock()

	return Result{
		Allowed:   count <= int64(limit),
		Limit:     limit,
		Remaining: max(0, limit-int(count)),
		Reset:     start.Add(window),
	}, nil
}

func (b *Batched) loop() {
	defer close(b.done)
	t := time.NewTicker(b.interval)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			b.flush(context.Background())
			return
		case <-t.C:
			b.flush(context.Background())
		}
	}
}

// Close stops the flush loop after a final flush.
func (b *Batched) Close() {
	close(b.stop)
	<-b.done
}

// flush sends pending increments in pipelines of at most maxBatch counters
// and updates the global counts. Counters of windows that have ended are
// dropped, whatever they hold: their Redis keys expire anyway, and keeping
// them would grow memory without bound during a Redis outage.
func (b *Batched) flush(ctx context.Context) {
	type item struct {
		k    counterKey
		c    *counter
		sent int64
		incr *redis.IntCmd
	}
	now := b.now()

	b.mu.Lock()
	var items []item
	for k, c := range b.counters {
		if !now.Before(c.start.Add(k.window)) {
			delete(b.counters, k)
			continue
		}
		if c.pending > 0 {
			items = append(items, item{k: k, c: c, sent: c.pending})
			c.inflight, c.pending = c.pending, 0 // still counted by Allow
		}
	}
	b.mu.Unlock()

	for len(items) > 0 {
		batch := items[:min(len(items), maxBatch)]
		items = items[len(batch):]

		pipe := b.redis.Pipeline()
		for i := range batch {
			batch[i].incr = pipe.IncrBy(ctx, batch[i].k.redisKey, batch[i].sent)
			// The extra second covers clock skew between instances.
			pipe.Expire(ctx, batch[i].k.redisKey, batch[i].k.window+time.Second)
		}
		_, err := pipe.Exec(ctx)

		b.mu.Lock()
		for _, it := range batch {
			it.c.inflight = 0
			if it.incr.Err() != nil {
				it.c.unconfirmed += it.sent
				continue
			}
			it.c.global = it.incr.Val()
		}
		b.mu.Unlock()
		if err != nil {
			b.logger.Warn("rate limit sync failed; counting locally until Redis recovers", "counters", len(batch), "err", err)
		}
	}
}
