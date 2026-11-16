package ratelimit

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/pristupaanastasia/shortener/internal/breaker"
)

// With the Redis circuit breaker open, a flush fails at once: the counts stay
// local until a later flush, and nothing is logged per flush.
func TestBatchedBreakerOpen(t *testing.T) {
	tests := []struct {
		name      string
		open      bool
		wantLog   bool
		wantRedis string // count in Redis after the flush
	}{
		{name: "closed breaker syncs", open: false, wantLog: false, wantRedis: "3"},
		{name: "open breaker keeps counts local without logging", open: true, wantLog: false, wantRedis: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
			cb := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Hour})
			if tt.open {
				cb.Allow()
				cb.Done(breaker.Failure)
			}
			rdb := redis.NewClient(testOptions(mr))
			rdb.AddHook(breaker.Hook(cb))
			var buf bytes.Buffer
			b := NewBatched(rdb, time.Hour, slog.New(slog.NewTextHandler(&buf, nil)), WithClock(func() time.Time { return now }))
			t.Cleanup(func() { b.Close(); _ = rdb.Close() })

			var last Result
			for range 3 {
				last, _ = b.Allow(context.Background(), "ip:1.2.3.4", 10, time.Minute)
			}
			b.flush(context.Background())
			if got := redisCount(mr, "ip:1.2.3.4", now); got != tt.wantRedis {
				t.Errorf("count in Redis = %q, want %q", got, tt.wantRedis)
			}
			if got := buf.Len() > 0; got != tt.wantLog {
				t.Errorf("logged = %v, want %v; log: %s", got, tt.wantLog, buf.String())
			}
			// Still counted locally either way.
			if r, _ := b.Allow(context.Background(), "ip:1.2.3.4", 10, time.Minute); r.Remaining != last.Remaining-1 {
				t.Errorf("Remaining = %d, want %d", r.Remaining, last.Remaining-1)
			}
		})
	}
}
