// Package cache is a read-through cache in front of a link.Store:
// in-process LRU (L1) → Redis (L2) → the wrapped store.
// See "Flow 1: Redirect" in README.md.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/pristupaanastasia/shortener/internal/link"
)

// Config tunes the cache.
type Config struct {
	L1Size int           // max entries in the in-process cache
	L1TTL  time.Duration // short, since L1 cannot be invalidated across instances

	RedisTTL       time.Duration
	RedisTTLJitter time.Duration // random extra TTL, so entries cached together do not expire together
	NegativeTTL    time.Duration // TTL for "not found" entries

	// InvalidateDelay is the gap before the second delete of a delayed double
	// delete, covering readers that loaded a stale value before the first one.
	InvalidateDelay time.Duration
	// StoreTimeout bounds a store read on a miss. The read is shared by all
	// waiting callers, so it does not use any single caller's context.
	StoreTimeout time.Duration
}

// DefaultConfig returns the values from README.md.
func DefaultConfig() Config {
	return Config{
		L1Size:          100_000,
		L1TTL:           10 * time.Second,
		RedisTTL:        time.Hour,
		RedisTTLJitter:  10 * time.Minute,
		NegativeTTL:     time.Minute,
		InvalidateDelay: time.Second,
		StoreTimeout:    5 * time.Second,
	}
}

// NewRedisClient returns a client tuned for the redirect hot path: tight
// timeouts and a single fast retry, so an unavailable Redis costs
// milliseconds per request instead of seconds (the library defaults).
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            addr,
		DialTimeout:     200 * time.Millisecond,
		ReadTimeout:     50 * time.Millisecond,
		WriteTimeout:    50 * time.Millisecond,
		MaxRetries:      1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 5 * time.Millisecond,
		// Dial retries are separate from command retries (default: 5 × 100ms),
		// and the two multiply.
		DialerRetries:      1,
		DialerRetryTimeout: 5 * time.Millisecond,
	})
}

// Store wraps a link.Store with L1 and Redis caches. It implements link.Store.
type Store struct {
	next   link.Store
	redis  *redis.Client
	l1     *expirable.LRU[string, entry]
	group  singleflight.Group
	cfg    Config
	logger *slog.Logger
}

// New returns a Store that caches reads from next.
func New(next link.Store, rdb *redis.Client, cfg Config, logger *slog.Logger) *Store {
	return &Store{
		next:   next,
		redis:  rdb,
		l1:     expirable.NewLRU[string, entry](cfg.L1Size, nil, cfg.L1TTL),
		cfg:    cfg,
		logger: logger,
	}
}

// entry is a cached lookup result. Found is false for negative entries.
type entry struct {
	Found     bool       `json:"f"`
	LongURL   string     `json:"u,omitempty"`
	UserID    string     `json:"o,omitempty"`
	CreatedAt time.Time  `json:"c,omitzero"`
	ExpiresAt *time.Time `json:"e,omitempty"`
	DeletedAt *time.Time `json:"d,omitempty"`
}

func newEntry(l link.Link) entry {
	return entry{
		Found:     true,
		LongURL:   l.LongURL,
		UserID:    l.UserID,
		CreatedAt: l.CreatedAt,
		ExpiresAt: l.ExpiresAt,
		DeletedAt: l.DeletedAt,
	}
}

func (e entry) link(key string) (link.Link, error) {
	if !e.Found {
		return link.Link{}, link.ErrNotFound
	}
	return link.Link{
		Key:       key,
		LongURL:   e.LongURL,
		UserID:    e.UserID,
		CreatedAt: e.CreatedAt,
		ExpiresAt: e.ExpiresAt,
		DeletedAt: e.DeletedAt,
	}, nil
}

func redisKey(key string) string { return "link:" + key }

// Create implements link.Store. It drops any cached "not found" entry for the
// key, so a freshly created alias resolves immediately on this instance.
func (s *Store) Create(ctx context.Context, l link.Link) error {
	if err := s.next.Create(ctx, l); err != nil {
		return err
	}
	s.drop(ctx, l.Key)
	return nil
}

// Get implements link.Store.
//
// Expired and deleted links are cached as they are: the service checks
// Active on every read, so a cached expired link still yields 410 without a
// store round-trip.
func (s *Store) Get(ctx context.Context, key string) (link.Link, error) {
	if e, ok := s.l1.Get(key); ok {
		return e.link(key)
	}
	if e, ok := s.getRedis(ctx, key); ok {
		s.l1.Add(key, e)
		return e.link(key)
	}

	// Concurrent misses for one key share a single store read.
	v, err, _ := s.group.Do(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.StoreTimeout)
		defer cancel()

		l, err := s.next.Get(ctx, key)
		var e entry
		switch {
		case errors.Is(err, link.ErrNotFound):
			e = entry{Found: false}
		case err != nil:
			return nil, err
		default:
			e = newEntry(l)
		}
		s.setRedis(ctx, key, e)
		s.l1.Add(key, e)
		return e, nil
	})
	if err != nil {
		return link.Link{}, err
	}
	return v.(entry).link(key)
}

// Invalidate removes key from both cache levels, then again after
// InvalidateDelay (delayed double delete). Call it after updating or deleting
// a link in the store.
func (s *Store) Invalidate(ctx context.Context, key string) {
	s.drop(ctx, key)
	time.AfterFunc(s.cfg.InvalidateDelay, func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.StoreTimeout)
		defer cancel()
		s.drop(ctx, key)
	})
}

func (s *Store) drop(ctx context.Context, key string) {
	s.l1.Remove(key)
	if err := s.redis.Del(ctx, redisKey(key)).Err(); err != nil {
		s.logger.Warn("cache delete failed", "key", key, "err", err)
	}
}

// getRedis returns the cached entry. Redis errors are logged and treated as a
// miss: the cache must never make a redirect fail.
func (s *Store) getRedis(ctx context.Context, key string) (entry, bool) {
	data, err := s.redis.Get(ctx, redisKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return entry{}, false
	}
	if err != nil {
		s.logger.Warn("cache get failed", "key", key, "err", err)
		return entry{}, false
	}
	var e entry
	if err := json.Unmarshal(data, &e); err != nil {
		s.logger.Warn("cache entry is corrupt", "key", key, "err", err)
		return entry{}, false
	}
	return e, true
}

func (s *Store) setRedis(ctx context.Context, key string, e entry) {
	data, err := json.Marshal(e)
	if err != nil {
		s.logger.Error("marshal cache entry", "key", key, "err", err)
		return
	}
	if err := s.redis.Set(ctx, redisKey(key), data, s.ttl(e)).Err(); err != nil {
		s.logger.Warn("cache set failed", "key", key, "err", err)
	}
}

func (s *Store) ttl(e entry) time.Duration {
	if !e.Found {
		return s.cfg.NegativeTTL
	}
	if s.cfg.RedisTTLJitter <= 0 {
		return s.cfg.RedisTTL
	}
	return s.cfg.RedisTTL + rand.N(s.cfg.RedisTTLJitter)
}

// Ping checks the Redis connection.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}
