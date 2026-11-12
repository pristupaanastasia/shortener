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
	cfg := DefaultConfig()
	cfg.L1Size = 1 // force reads to go to Redis
	c := New(backing, rdb, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("%v (is Redis running? try `make up`)", err)
	}

	key := "it" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), redisKey(key)) })
	if err := c.Create(ctx, link.Link{Key: key, LongURL: "https://example.com/real"}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		mustGet(t, c, key)
		c.l1.Purge()
	}
	if got := backing.gets.Load(); got != 1 {
		t.Errorf("store reads = %d, want 1", got)
	}
	if ttl := rdb.TTL(ctx, redisKey(key)).Val(); ttl <= 0 {
		t.Errorf("Redis TTL = %v, want positive", ttl)
	}
}
