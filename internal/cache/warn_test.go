package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pristupaanastasia/shortener/internal/breaker"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

func TestWarnRedis(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantLog bool
	}{
		{name: "redis failure is logged", err: errors.New("i/o timeout"), wantLog: true},
		{name: "open breaker is not logged", err: breaker.ErrOpen, wantLog: false},
		{name: "wrapped open breaker is not logged", err: fmt.Errorf("get: %w", breaker.ErrOpen), wantLog: false},
		{name: "failed probe is not logged", err: fmt.Errorf("%w: %w", breaker.ErrProbeFailed, errors.New("i/o timeout")), wantLog: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			s := &Store{logger: slog.New(slog.NewTextHandler(&buf, nil))}
			s.warnRedis("cache get failed", "k", tt.err)
			if got := buf.Len() > 0; got != tt.wantLog {
				t.Errorf("logged = %v, want %v; log: %s", got, tt.wantLog, buf.String())
			}
		})
	}
}

// Regression: with the breaker open, both deletes of an invalidation were
// rejected, and redirects served the old URL for the entry's whole TTL once
// the breaker closed.
func TestInvalidateBypassesBreaker(t *testing.T) {
	tests := []struct {
		name    string
		open    bool
		wantURL string
	}{
		{name: "breaker closed", open: false, wantURL: "https://example.com/new"},
		{name: "breaker open during the update", open: true, wantURL: "https://example.com/new"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			cb := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Hour})
			rdb := NewRedisClient(mr.Addr())
			rdb.AddHook(breaker.Hook(cb))
			t.Cleanup(func() { _ = rdb.Close() })
			cfg := DefaultConfig()
			cfg.L1TTL = time.Millisecond // every Get after a pause goes to Redis
			c := New(memory.New(), rdb, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
			ctx := context.Background()
			l := link.Link{Key: "upd", LongURL: "https://example.com/old", CreatedAt: time.Now()}
			mustCreate(t, c, l)
			if _, err := c.Get(ctx, "upd"); err != nil { // cached in Redis
				t.Fatal(err)
			}
			if !mr.Exists("link:upd") {
				t.Fatalf("link not cached in Redis; keys: %v", mr.Keys())
			}
			if tt.open {
				cb.Allow()
				cb.Done(breaker.Failure)
			}
			l.LongURL = "https://example.com/new"
			if err := c.Update(ctx, l, link.Fields{LongURL: true}); err != nil {
				t.Fatal(err)
			}
			cb.Allow()
			cb.Done(breaker.Success)         // Redis is back
			time.Sleep(5 * time.Millisecond) // past L1
			got, err := c.Get(ctx, "upd")
			if err != nil || got.LongURL != tt.wantURL {
				t.Errorf("Get after the update = %q, %v; want %q", got.LongURL, err, tt.wantURL)
			}
		})
	}
}
