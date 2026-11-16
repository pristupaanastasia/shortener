package ratelimit

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newBatched returns a limiter with a fixed clock and a flush interval long
// enough that only explicit flush calls sync.
func newBatched(t *testing.T, mr *miniredis.Miniredis, now *time.Time) *Batched {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
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

func TestBatchedRedisDown(t *testing.T) {
	tests := []struct {
		name     string
		requests int
	}{
		{name: "increments survive a failed flush", requests: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			l := newBatched(t, mr, &now)
			ctx := context.Background()
			addr := mr.Addr()
			mr.Close()

			for range tt.requests {
				if _, err := l.Allow(ctx, "user:u1", 100, time.Minute); err != nil {
					t.Fatalf("Allow with Redis down: %v", err)
				}
			}
			l.flush(ctx) // fails; the increments are kept

			mr2 := miniredis.NewMiniRedis()
			if err := mr2.StartAddr(addr); err != nil {
				t.Skipf("cannot restart Redis on %s: %v", addr, err)
			}
			defer mr2.Close()
			l.flush(ctx)
			key := "rl:user:u1:" + strconv.FormatInt(now.Truncate(time.Minute).Unix(), 10)
			if got, _ := mr2.Get(key); got != strconv.Itoa(tt.requests) {
				t.Errorf("Redis count after recovery = %q, want %d", got, tt.requests)
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
