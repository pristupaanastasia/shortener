package cache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

// countingStore counts Get calls and can block them until release is closed.
type countingStore struct {
	*memory.Store
	gets    atomic.Int32
	release chan struct{} // nil: do not block
}

func (c *countingStore) Get(ctx context.Context, key string) (link.Link, error) {
	c.gets.Add(1)
	if c.release != nil {
		<-c.release
	}
	return c.Store.Get(ctx, key)
}

func newTestCache(t *testing.T, cfg Config) (*Store, *countingStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = rdb.Close() })
	backing := &countingStore{Store: memory.New()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(backing, rdb, cfg, logger), backing, mr
}

func mustCreate(t *testing.T, s link.Store, key, url string) {
	t.Helper()
	if err := s.Create(context.Background(), link.Link{Key: key, LongURL: url}); err != nil {
		t.Fatal(err)
	}
}

func mustGet(t *testing.T, s link.Store, key string) link.Link {
	t.Helper()
	l, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return l
}

func TestGetReadsThroughLevels(t *testing.T) {
	cfg := DefaultConfig()
	cfg.L1TTL = 50 * time.Millisecond
	c, backing, mr := newTestCache(t, cfg)
	mustCreate(t, backing.Store, "abc1234", "https://example.com")

	// Miss: read from the store, populate Redis and L1.
	if got := mustGet(t, c, "abc1234"); got.LongURL != "https://example.com" {
		t.Fatalf("LongURL = %q", got.LongURL)
	}
	if !mr.Exists("link:abc1234") {
		t.Fatal("entry was not written to Redis")
	}

	// L1 hit.
	mustGet(t, c, "abc1234")

	// L1 expired: Redis hit.
	time.Sleep(100 * time.Millisecond)
	mustGet(t, c, "abc1234")

	// Redis gone too: back to the store.
	mr.FlushAll()
	time.Sleep(100 * time.Millisecond)
	mustGet(t, c, "abc1234")

	if got := backing.gets.Load(); got != 2 {
		t.Errorf("store reads = %d, want 2 (first miss and after Redis flush)", got)
	}
}

func TestRedisTTLHasJitter(t *testing.T) {
	c, backing, mr := newTestCache(t, DefaultConfig())
	mustCreate(t, backing.Store, "abc1234", "https://example.com")
	mustGet(t, c, "abc1234")

	ttl := mr.TTL("link:abc1234")
	if ttl < time.Hour || ttl >= time.Hour+10*time.Minute {
		t.Errorf("TTL = %v, want within [1h, 1h10m)", ttl)
	}
}

func TestConcurrentMissesReadStoreOnce(t *testing.T) {
	c, backing, _ := newTestCache(t, DefaultConfig())
	mustCreate(t, backing.Store, "hot1234", "https://example.com/viral")
	backing.release = make(chan struct{})

	const n = 100
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if got := mustGet(t, c, "hot1234"); got.LongURL != "https://example.com/viral" {
				t.Errorf("LongURL = %q", got.LongURL)
			}
		})
	}
	// Give all goroutines time to reach the miss path, then let the read finish.
	time.Sleep(100 * time.Millisecond)
	close(backing.release)
	wg.Wait()

	if got := backing.gets.Load(); got != 1 {
		t.Errorf("store reads = %d, want 1", got)
	}
}

func TestNegativeCaching(t *testing.T) {
	c, backing, mr := newTestCache(t, DefaultConfig())
	ctx := context.Background()

	for range 3 {
		if _, err := c.Get(ctx, "missing"); !errors.Is(err, link.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	}
	if got := backing.gets.Load(); got != 1 {
		t.Errorf("store reads = %d, want 1", got)
	}
	if ttl := mr.TTL("link:missing"); ttl != time.Minute {
		t.Errorf("negative TTL = %v, want 1m", ttl)
	}
}

func TestCreateDropsNegativeEntry(t *testing.T) {
	c, _, _ := newTestCache(t, DefaultConfig())
	ctx := context.Background()

	if _, err := c.Get(ctx, "sale2026"); !errors.Is(err, link.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	mustCreate(t, c, "sale2026", "https://example.com/sale")
	if got := mustGet(t, c, "sale2026"); got.LongURL != "https://example.com/sale" {
		t.Errorf("LongURL = %q: a stale negative entry was served", got.LongURL)
	}
}

func TestInvalidateDelayedDoubleDelete(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InvalidateDelay = 50 * time.Millisecond
	c, backing, mr := newTestCache(t, cfg)
	mustCreate(t, backing.Store, "abc1234", "https://example.com/old")
	mustGet(t, c, "abc1234")
	stale, err := mr.Get("link:abc1234")
	if err != nil {
		t.Fatal(err)
	}

	c.Invalidate(context.Background(), "abc1234")
	if mr.Exists("link:abc1234") {
		t.Fatal("first delete did not remove the Redis entry")
	}

	// A concurrent reader that loaded the old value before the store was
	// updated writes it back after the first delete.
	if err := mr.Set("link:abc1234", stale); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if mr.Exists("link:abc1234") {
		t.Error("second delete did not remove the stale entry")
	}
}

func TestRedisDownFallsBackToStore(t *testing.T) {
	c, backing, mr := newTestCache(t, DefaultConfig())
	mustCreate(t, backing.Store, "abc1234", "https://example.com")
	mr.Close()

	start := time.Now()
	if got := mustGet(t, c, "abc1234"); got.LongURL != "https://example.com" {
		t.Errorf("LongURL = %q", got.LongURL)
	}
	// The redirect latency budget is 100 ms; an unavailable cache must not eat it.
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Get with Redis down took %v, want < 200ms", elapsed)
	}
	mustCreate(t, c, "new1234", "https://example.com/new")
}

func TestCachedExpiredLinkStaysExpired(t *testing.T) {
	c, backing, _ := newTestCache(t, DefaultConfig())
	past := time.Now().Add(-time.Hour)
	if err := backing.Store.Create(context.Background(), link.Link{Key: "old1234", LongURL: "https://example.com", ExpiresAt: &past}); err != nil {
		t.Fatal(err)
	}
	svc := link.NewService(c, "sho.rt")
	for range 2 {
		if _, err := svc.Resolve(context.Background(), "old1234"); !errors.Is(err, link.ErrGone) {
			t.Fatalf("err = %v, want ErrGone", err)
		}
	}
	if got := backing.gets.Load(); got != 1 {
		t.Errorf("store reads = %d, want 1 (410 served from cache)", got)
	}
}
