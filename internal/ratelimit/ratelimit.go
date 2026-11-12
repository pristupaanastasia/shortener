// Package ratelimit implements a fixed-window rate limiter in Redis.
//
// Fixed windows are the simplest algorithm: one counter per subject per
// window. The trade-off is a burst of up to 2× the limit around a window
// boundary (the end of one window plus the start of the next).
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result describes the subject's quota after a request.
type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	Reset     time.Time // when the current window ends
}

// Limiter counts requests per subject in Redis, shared by all instances.
type Limiter struct {
	redis *redis.Client
	now   func() time.Time
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithClock replaces time.Now, so tests do not depend on window boundaries.
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// New returns a Limiter backed by rdb.
func New(rdb *redis.Client, opts ...Option) *Limiter {
	l := &Limiter{redis: rdb, now: time.Now}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow records a request by subject (e.g. "user:<id>" or "ip:<addr>") and
// reports whether it is within limit requests per window.
func (l *Limiter) Allow(ctx context.Context, subject string, limit int, window time.Duration) (Result, error) {
	start := l.now().Truncate(window)
	key := fmt.Sprintf("rl:%s:%d", subject, start.Unix())

	pipe := l.redis.TxPipeline()
	incr := pipe.Incr(ctx, key)
	// The extra second covers clock skew between instances.
	pipe.Expire(ctx, key, window+time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return Result{}, fmt.Errorf("rate limit %s: %w", subject, err)
	}

	count := int(incr.Val())
	return Result{
		Allowed:   count <= limit,
		Limit:     limit,
		Remaining: max(0, limit-count),
		Reset:     start.Add(window),
	}, nil
}
