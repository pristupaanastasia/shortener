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
// one sync interval's worth of extra requests per window. In exchange, the
// per-request Redis transaction (12.8% of service CPU at 10K creates/s) is
// gone. Counters in Redis use the same keys as Limiter, so both can run side
// by side during a rollout.
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

type counterKey struct {
	redisKey string
	window   time.Duration
}

type counter struct {
	global  int64 // last count read from Redis, including our flushed requests
	pending int64 // requests counted here and not flushed yet
	start   time.Time
}

// NewBatched returns a Batched limiter that flushes every interval. Call
// Close to stop it and flush the remaining increments.
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
	count := c.global + c.pending
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

// flush sends pending increments in one pipeline and updates the global
// counts. On failure the increments are kept for the next flush. Counters of
// windows that have ended are dropped.
func (b *Batched) flush(ctx context.Context) {
	type item struct {
		k       counterKey
		c       *counter
		pending int64
		incr    *redis.IntCmd
	}
	now := b.now()

	b.mu.Lock()
	var items []item
	for k, c := range b.counters {
		if c.pending > 0 {
			items = append(items, item{k: k, c: c, pending: c.pending})
			c.pending = 0
		} else if !now.Before(c.start.Add(k.window)) {
			delete(b.counters, k)
		}
	}
	b.mu.Unlock()
	if len(items) == 0 {
		return
	}

	pipe := b.redis.Pipeline()
	for i := range items {
		items[i].incr = pipe.IncrBy(ctx, items[i].k.redisKey, items[i].pending)
		// The extra second covers clock skew between instances.
		pipe.Expire(ctx, items[i].k.redisKey, items[i].k.window+time.Second)
	}
	_, err := pipe.Exec(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		for _, it := range items {
			it.c.pending += it.pending // retry with the next flush
		}
		b.logger.Warn("rate limit sync failed, deciding locally until Redis recovers", "err", err)
		return
	}
	for _, it := range items {
		it.c.global = it.incr.Val()
	}
}
