package ratelimit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newBatched returns a limiter with a fixed clock and a flush interval long
// enough that only explicit flush calls sync.
func newBatched(t *testing.T, mr *miniredis.Miniredis, now *time.Time) *Batched {
	t.Helper()
	rdb := redis.NewClient(testOptions(mr))
	b := NewBatched(rdb, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)), WithClock(func() time.Time { return *now }))
	t.Cleanup(func() {
		b.Close()
		_ = rdb.Close()
	})
	return b
}

func TestBatched(t *testing.T) {
	mr := miniredis.RunT(t)
	now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
	a, b := newBatched(t, mr, &now), newBatched(t, mr, &now)
	ctx := context.Background()
	key := "rl:ip:1.2.3.4:" + strconv.FormatInt(now.Truncate(time.Minute).Unix(), 10)
	allow := func(l *Batched) Result { r, _ := l.Allow(ctx, "ip:1.2.3.4", 4, time.Minute); return r }

	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "decisions do not touch redis", run: func(t *testing.T) {
			before := mr.CommandCount()
			for i := 1; i <= 3; i++ {
				if r := allow(a); !r.Allowed || r.Remaining != 4-i {
					t.Fatalf("request %d: %+v", i, r)
				}
			}
			if got := mr.CommandCount() - before; got != 0 {
				t.Errorf("%d Redis commands before a flush, want 0", got)
			}
		}},
		{name: "a flush writes the count and its ttl", run: func(t *testing.T) {
			a.flush(ctx)
			if got, _ := mr.Get(key); got != "3" {
				t.Errorf("Redis count = %q, want 3", got)
			}
			if ttl := mr.TTL(key); ttl <= 0 || ttl > time.Minute+time.Second {
				t.Errorf("TTL = %v", ttl)
			}
		}},
		{name: "another instance learns the global count on its flush", run: func(t *testing.T) {
			if r := allow(b); !r.Allowed {
				t.Fatalf("b's first request: %+v (it has not synced yet, so it sees only itself)", r)
			}
			b.flush(ctx) // global is now 4
			if r := allow(b); r.Allowed || r.Remaining != 0 {
				t.Errorf("b after sync: %+v, want denied (4 of 4 used)", r)
			}
		}},
		{name: "the first instance sees the limit after its next flush", run: func(t *testing.T) {
			a.flush(ctx) // nothing pending: no change
			allow(a)
			a.flush(ctx)
			if r := allow(a); r.Allowed {
				t.Errorf("a after sync: %+v, want denied", r)
			}
		}},
		{name: "the next window starts fresh", run: func(t *testing.T) {
			now = now.Add(time.Minute)
			if r := allow(a); !r.Allowed || r.Remaining != 3 {
				t.Errorf("next window: %+v", r)
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			break
		}
	}
}

// pipelineHook replaces how pipelines run, to simulate a slow or failing
// Redis: run gets the real execution as next.
type pipelineHook struct {
	run func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error
}

func (pipelineHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (pipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h pipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { return h.run(ctx, cmds, next) }
}

// newHooked returns a limiter whose pipelines go through run.
func newHooked(t *testing.T, mr *miniredis.Miniredis, now *time.Time, run func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error) *Batched {
	t.Helper()
	rdb := redis.NewClient(testOptions(mr))
	rdb.AddHook(pipelineHook{run: run})
	b := NewBatched(rdb, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)), WithClock(func() time.Time { return *now }))
	t.Cleanup(func() { b.Close(); _ = rdb.Close() })
	return b
}

func redisCount(mr *miniredis.Miniredis, subject string, now time.Time) string {
	v, _ := mr.Get("rl:" + subject + ":" + strconv.FormatInt(now.Truncate(time.Minute).Unix(), 10))
	return v
}

// Regression: pending was zeroed before the round trip, so requests in
// flight were not counted and a slow Redis handed out a fresh quota on every
// flush (150 requests in 3s against 10/min).
func TestBatchedCountsInflightRequests(t *testing.T) {
	tests := []struct {
		name  string
		limit int
	}{
		{name: "a request during a blocked flush is denied once the limit is used", limit: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			l := newHooked(t, mr, &now, func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				once.Do(func() { // block only the first flush, not the final one in Close
					close(entered)
					<-release
				})
				return next(ctx, cmds)
			})
			for range tt.limit {
				_, _ = l.Allow(context.Background(), "ip:1.2.3.4", tt.limit, time.Minute)
			}
			done := make(chan struct{})
			go func() { l.flush(context.Background()); close(done) }()
			<-entered // the flush is in its round trip now

			r, _ := l.Allow(context.Background(), "ip:1.2.3.4", tt.limit, time.Minute)
			close(release)
			<-done
			if r.Allowed {
				t.Errorf("request during the flush: %+v, want denied (%d of %d already used)", r, tt.limit, tt.limit)
			}
		})
	}
}

// Regression: a failed flush re-added its increments, so a pipeline that
// Redis had applied before the error (e.g. a read timeout) was counted again.
func TestBatchedFailedFlushIsNotResent(t *testing.T) {
	tests := []struct {
		name     string
		requests int
	}{
		{name: "applied but failed pipeline is not counted twice", requests: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			failNext := true
			l := newHooked(t, mr, &now, func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				err := next(ctx, cmds) // Redis applies it...
				// Fail the flush, not go-redis's connection handshake pipeline.
				if failNext && len(cmds) > 0 && cmds[0].Name() == "incrby" {
					failNext = false
					for _, c := range cmds {
						c.SetErr(errors.New("i/o timeout")) // ...but the reply is lost
					}
					return errors.New("i/o timeout")
				}
				return err
			})
			for range tt.requests {
				_, _ = l.Allow(context.Background(), "user:u1", 100, time.Minute)
			}
			l.flush(context.Background()) // fails after applying
			l.flush(context.Background()) // must not resend
			if got := redisCount(mr, "user:u1", now); got != strconv.Itoa(tt.requests) {
				t.Errorf("Redis count = %q, want %d (each request counted once)", got, tt.requests)
			}
			// The requests stay counted locally: the limit still holds.
			r, _ := l.Allow(context.Background(), "user:u1", tt.requests, time.Minute)
			if r.Allowed {
				t.Errorf("after the failed flush: %+v, want denied", r)
			}
		})
	}
}

func TestBatchedRedisDown(t *testing.T) {
	tests := []struct {
		name  string
		limit int
	}{
		{name: "limits hold locally while Redis is down", limit: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			l := newBatched(t, mr, &now)
			mr.Close()
			ctx := context.Background()
			var allowed int
			for range 3 * tt.limit {
				if r, err := l.Allow(ctx, "user:u1", tt.limit, time.Minute); err != nil {
					t.Fatalf("Allow with Redis down: %v", err)
				} else if r.Allowed {
					allowed++
				}
				l.flush(ctx) // every flush fails
			}
			if allowed != tt.limit {
				t.Errorf("%d requests allowed with Redis down, want %d", allowed, tt.limit)
			}
		})
	}
}

// Regression: counters were kept while they had pending increments, so an
// outage kept every subject of every window in memory.
func TestBatchedEvictsEndedWindows(t *testing.T) {
	tests := []struct {
		name      string
		redisDown bool
	}{
		{name: "after a successful flush"},
		{name: "while Redis is down", redisDown: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			l := newBatched(t, mr, &now)
			if tt.redisDown {
				mr.Close()
			}
			for i := range 1000 {
				_, _ = l.Allow(context.Background(), "ip:"+strconv.Itoa(i), 10, time.Minute)
			}
			l.flush(context.Background())
			now = now.Add(time.Minute) // the window has ended
			l.flush(context.Background())
			l.mu.Lock()
			n := len(l.counters)
			l.mu.Unlock()
			if n != 0 {
				t.Errorf("%d counters left after their window ended, want 0", n)
			}
		})
	}
}

func TestBatchedChunksLargeFlushes(t *testing.T) {
	tests := []struct {
		name          string
		subjects      int
		wantPipelines int
	}{
		{name: "one pipeline for a small batch", subjects: 10, wantPipelines: 1},
		{name: "1200 subjects in pipelines of 500", subjects: 1200, wantPipelines: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			pipelines := 0
			l := newHooked(t, mr, &now, func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
				// Count only flushes: go-redis also pipelines its connection handshake.
				if len(cmds) > 0 && cmds[0].Name() == "incrby" {
					pipelines++
				}
				return next(ctx, cmds)
			})
			for i := range tt.subjects {
				_, _ = l.Allow(context.Background(), "ip:"+strconv.Itoa(i), 10, time.Minute)
			}
			l.flush(context.Background())
			if pipelines != tt.wantPipelines {
				t.Errorf("%d pipelines, want %d", pipelines, tt.wantPipelines)
			}
			if got := redisCount(mr, "ip:0", now); got != "1" {
				t.Errorf("count for ip:0 = %q, want 1", got)
			}
		})
	}
}

func TestBatchedCloseFlushes(t *testing.T) {
	tests := []struct {
		name     string
		requests int
	}{
		{name: "pending increments are written on close", requests: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer rdb.Close()
			l := NewBatched(rdb, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)), WithClock(func() time.Time { return now }))
			for range tt.requests {
				_, _ = l.Allow(context.Background(), "user:u1", 100, time.Minute)
			}
			l.Close()
			key := "rl:user:u1:" + strconv.FormatInt(now.Truncate(time.Minute).Unix(), 10)
			if got, _ := mr.Get(key); got != strconv.Itoa(tt.requests) {
				t.Errorf("Redis count after Close = %q, want %d", got, tt.requests)
			}
		})
	}
}

// testOptions match the production limiter client: tight timeouts, no command
// retries and a single dial retry, so a closed Redis fails fast.
func testOptions(mr *miniredis.Miniredis) *redis.Options {
	return &redis.Options{
		Addr:               mr.Addr(),
		MaxRetries:         -1,
		DialTimeout:        200 * time.Millisecond,
		ReadTimeout:        50 * time.Millisecond,
		WriteTimeout:       50 * time.Millisecond,
		DialerRetries:      1,
		DialerRetryTimeout: 5 * time.Millisecond,
	}
}
