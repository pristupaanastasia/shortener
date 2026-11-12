// Package idempotency stores responses to requests carrying an
// Idempotency-Key, so a retried request returns the original response
// instead of being executed twice.
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL is how long a key and its response are kept.
const DefaultTTL = 24 * time.Hour

// Record is the state of an idempotency key.
type Record struct {
	// Fingerprint identifies the request body, so a key reused with a
	// different request is detected.
	Fingerprint string `json:"fp"`
	Pending     bool   `json:"pending,omitempty"` // the first request is still running
	Status      int    `json:"status,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

// Store keeps records in Redis.
type Store struct {
	redis *redis.Client
	ttl   time.Duration
}

// New returns a Store backed by rdb.
func New(rdb *redis.Client, ttl time.Duration) *Store {
	return &Store{redis: rdb, ttl: ttl}
}

func redisKey(scope, key string) string { return "idem:" + scope + ":" + key }

// Begin claims key for a new request. If it was free, Begin returns
// started=true and the caller must later call Complete or Abort. Otherwise it
// returns the existing record: pending, or the stored response.
func (s *Store) Begin(ctx context.Context, scope, key, fingerprint string) (rec Record, started bool, err error) {
	pending, _ := json.Marshal(Record{Fingerprint: fingerprint, Pending: true})
	ok, err := s.redis.SetNX(ctx, redisKey(scope, key), pending, s.ttl).Result()
	if err != nil {
		return Record{}, false, fmt.Errorf("claim idempotency key: %w", err)
	}
	if ok {
		return Record{}, true, nil
	}

	data, err := s.redis.Get(ctx, redisKey(scope, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		// Expired or aborted between SETNX and GET: try once more.
		return s.Begin(ctx, scope, key, fingerprint)
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("get idempotency key: %w", err)
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return Record{}, false, fmt.Errorf("decode idempotency record: %w", err)
	}
	return rec, false, nil
}

// Complete stores the response for a key claimed with Begin.
func (s *Store) Complete(ctx context.Context, scope, key string, rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := s.redis.Set(ctx, redisKey(scope, key), data, s.ttl).Err(); err != nil {
		return fmt.Errorf("store idempotent response: %w", err)
	}
	return nil
}

// Abort releases a key claimed with Begin, so the request can be retried.
// Used when the request failed with a server error.
func (s *Store) Abort(ctx context.Context, scope, key string) error {
	if err := s.redis.Del(ctx, redisKey(scope, key)).Err(); err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	return nil
}
