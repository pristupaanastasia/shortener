//go:build integration

package cache

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

func TestRealRedis(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := NewRedisClient(addr)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	backing := &countingStore{Store: memory.New()}
	c := New(backing, rdb, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("%v (is Redis running? try `make up`)", err)
	}
	key := "it" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), redisKey(key)) })
	mustCreate(t, c, link.Link{Key: key, LongURL: "https://example.com/real"})

	// L1 is purged before every lookup, so hits must come from Redis.
	tests := []struct {
		name      string
		wantReads int32
	}{
		{name: "miss reads the store", wantReads: 1},
		{name: "redis hit", wantReads: 1},
		{name: "redis hit again", wantReads: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.l1.Purge()
			if l, err := c.Get(ctx, key); err != nil || l.LongURL != "https://example.com/real" {
				t.Fatalf("Get = %+v, %v", l, err)
			}
			if got := backing.gets.Load(); got != tt.wantReads {
				t.Errorf("store reads = %d, want %d", got, tt.wantReads)
			}
			if ttl := rdb.TTL(ctx, redisKey(key)).Val(); ttl <= 0 {
				t.Errorf("Redis TTL = %v, want positive", ttl)
			}
		})
	}
}
