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

func mustCreate(t *testing.T, s link.Store, l link.Link) {
	t.Helper()
	if err := s.Create(context.Background(), l); err != nil {
		t.Fatal(err)
	}
}

// step is a cache lookup with the expected result and store reads so far.
type step struct {
	name      string
	before    func() // e.g. let L1 expire, flush Redis
	key       string
	wantURL   string
	wantErr   error
	wantReads int32
}

func runSteps(t *testing.T, c link.Store, backing *countingStore, steps []step) {
	t.Helper()
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if step.before != nil {
				step.before()
			}
			l, err := c.Get(context.Background(), step.key)
			if !errors.Is(err, step.wantErr) {
				t.Fatalf("Get(%q) err = %v, want %v", step.key, err, step.wantErr)
			}
			if l.LongURL != step.wantURL {
				t.Errorf("Get(%q) LongURL = %q, want %q", step.key, l.LongURL, step.wantURL)
			}
			if got := backing.gets.Load(); got != step.wantReads {
				t.Errorf("store reads = %d, want %d", got, step.wantReads)
			}
		})
		if !ok {
			break
		}
	}
}

func TestGetReadsThroughLevels(t *testing.T) {
	cfg := DefaultConfig()
	cfg.L1TTL = 50 * time.Millisecond
	c, backing, mr := newTestCache(t, cfg)
	mustCreate(t, backing.Store, link.Link{Key: "abc1234", LongURL: "https://example.com"})
	expireL1 := func() { time.Sleep(100 * time.Millisecond) }

	runSteps(t, c, backing, []step{
		{name: "miss reads the store", key: "abc1234", wantURL: "https://example.com", wantReads: 1},
		{name: "l1 hit", key: "abc1234", wantURL: "https://example.com", wantReads: 1},
		{name: "l1 expired, redis hit", before: expireL1, key: "abc1234", wantURL: "https://example.com", wantReads: 1},
		{name: "redis flushed, store read again", before: func() { mr.FlushAll(); expireL1() }, key: "abc1234", wantURL: "https://example.com", wantReads: 2},
	})
}

func TestNegativeCaching(t *testing.T) {
	c, backing, _ := newTestCache(t, DefaultConfig())

	runSteps(t, c, backing, []step{
		{name: "first miss reads the store", key: "sale2026", wantErr: link.ErrNotFound, wantReads: 1},
		{name: "second miss is cached", key: "sale2026", wantErr: link.ErrNotFound, wantReads: 1},
		{name: "third miss is cached", key: "sale2026", wantErr: link.ErrNotFound, wantReads: 1},
		{
			name:   "create drops the negative entry",
			before: func() { mustCreate(t, c, link.Link{Key: "sale2026", LongURL: "https://example.com/sale"}) },
			key:    "sale2026", wantURL: "https://example.com/sale", wantReads: 2,
		},
	})
}

func TestCachedExpiredLinkStaysExpired(t *testing.T) {
	c, backing, _ := newTestCache(t, DefaultConfig())
	past := time.Now().Add(-time.Hour)
	mustCreate(t, backing.Store, link.Link{Key: "old1234", LongURL: "https://example.com", ExpiresAt: &past})
	svc := link.NewService(c, "sho.rt")

	tests := []struct {
		name      string
		wantReads int32
	}{
		{name: "first resolve reads the store", wantReads: 1},
		{name: "second resolve serves 410 from cache", wantReads: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := svc.Resolve(context.Background(), "old1234"); !errors.Is(err, link.ErrGone) {
				t.Fatalf("Resolve err = %v, want ErrGone", err)
			}
			if got := backing.gets.Load(); got != tt.wantReads {
				t.Errorf("store reads = %d, want %d", got, tt.wantReads)
			}
		})
	}
}

func TestRedisTTL(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		exists  bool
		wantMin time.Duration
		wantMax time.Duration // exclusive
	}{
		{name: "found link: 1h plus jitter", key: "abc1234", exists: true, wantMin: time.Hour, wantMax: time.Hour + 10*time.Minute},
		{name: "missing link: negative ttl", key: "missing", exists: false, wantMin: time.Minute, wantMax: time.Minute + time.Nanosecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, backing, mr := newTestCache(t, DefaultConfig())
			if tt.exists {
				mustCreate(t, backing.Store, link.Link{Key: tt.key, LongURL: "https://example.com"})
			}
			_, _ = c.Get(context.Background(), tt.key)
			if ttl := mr.TTL(redisKey(tt.key)); ttl < tt.wantMin || ttl >= tt.wantMax {
				t.Errorf("TTL = %v, want within [%v, %v)", ttl, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestConcurrentMissesReadStoreOnce(t *testing.T) {
	tests := []struct {
		name       string
		goroutines int
		wantReads  int32
	}{
		{name: "single caller", goroutines: 1, wantReads: 1},
		{name: "100 concurrent misses share one read", goroutines: 100, wantReads: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, backing, _ := newTestCache(t, DefaultConfig())
			mustCreate(t, backing.Store, link.Link{Key: "hot1234", LongURL: "https://example.com/viral"})
			backing.release = make(chan struct{})

			var wg sync.WaitGroup
			for range tt.goroutines {
				wg.Go(func() {
					l, err := c.Get(context.Background(), "hot1234")
					if err != nil || l.LongURL != "https://example.com/viral" {
						t.Errorf("Get = %+v, %v", l, err)
					}
				})
			}
			// Let every goroutine reach the miss path, then finish the read.
			time.Sleep(100 * time.Millisecond)
			close(backing.release)
			wg.Wait()

			if got := backing.gets.Load(); got != tt.wantReads {
				t.Errorf("store reads = %d, want %d", got, tt.wantReads)
			}
		})
	}
}

func TestInvalidateDelayedDoubleDelete(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InvalidateDelay = 50 * time.Millisecond
	c, backing, mr := newTestCache(t, cfg)
	mustCreate(t, backing.Store, link.Link{Key: "abc1234", LongURL: "https://example.com/old"})
	key := redisKey("abc1234")
	var stale string

	steps := []struct {
		name       string
		action     func(t *testing.T)
		wantCached bool
	}{
		{name: "read caches the link", wantCached: true, action: func(t *testing.T) {
			if _, err := c.Get(context.Background(), "abc1234"); err != nil {
				t.Fatal(err)
			}
			stale, _ = mr.Get(key)
		}},
		{name: "first delete removes it", wantCached: false, action: func(*testing.T) {
			c.Invalidate(context.Background(), "abc1234")
		}},
		{name: "a racing reader writes the stale value back", wantCached: true, action: func(t *testing.T) {
			if err := mr.Set(key, stale); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "second delete removes the stale value", wantCached: false, action: func(*testing.T) {
			time.Sleep(3 * cfg.InvalidateDelay)
		}},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			step.action(t)
			if got := mr.Exists(key); got != step.wantCached {
				t.Fatalf("cached in Redis = %v, want %v", got, step.wantCached)
			}
		})
		if !ok {
			break
		}
	}
}

func TestUpdateInvalidates(t *testing.T) {
	c, backing, _ := newTestCache(t, DefaultConfig())
	mustCreate(t, backing.Store, link.Link{Key: "abc1234", LongURL: "https://example.com/old"})

	runSteps(t, c, backing, []step{
		{name: "cached before update", key: "abc1234", wantURL: "https://example.com/old", wantReads: 1},
		{
			name: "update through the cache is visible at once",
			before: func() {
				if err := c.Update(context.Background(), link.Link{Key: "abc1234", LongURL: "https://example.com/new"}); err != nil {
					t.Fatal(err)
				}
			},
			key: "abc1234", wantURL: "https://example.com/new", wantReads: 2,
		},
	})
}

func TestRedisDown(t *testing.T) {
	tests := []struct {
		name   string
		action func(c *Store) error
	}{
		{name: "get falls back to the store", action: func(c *Store) error {
			l, err := c.Get(context.Background(), "abc1234")
			if err == nil && l.LongURL != "https://example.com" {
				return errors.New("wrong link: " + l.LongURL)
			}
			return err
		}},
		{name: "create still works", action: func(c *Store) error {
			return c.Create(context.Background(), link.Link{Key: "new1234", LongURL: "https://example.com/new"})
		}},
		{name: "update still works", action: func(c *Store) error {
			return c.Update(context.Background(), link.Link{Key: "abc1234", LongURL: "https://example.com/new"})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, backing, mr := newTestCache(t, DefaultConfig())
			mustCreate(t, backing.Store, link.Link{Key: "abc1234", LongURL: "https://example.com"})
			mr.Close()

			start := time.Now()
			if err := tt.action(c); err != nil {
				t.Fatalf("with Redis down: %v", err)
			}
			// The redirect latency budget is 100 ms; an unavailable cache must not eat it.
			if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
				t.Errorf("took %v with Redis down, want < 200ms", elapsed)
			}
		})
	}
}
