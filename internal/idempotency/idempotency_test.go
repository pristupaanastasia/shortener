package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, DefaultTTL), mr
}

func TestLifecycle(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	steps := []struct {
		name        string
		action      func() (Record, bool, error)
		wantStarted bool
		wantRecord  Record
		check       func(t *testing.T)
	}{
		{name: "first request claims the key", wantStarted: true, action: func() (Record, bool, error) {
			return s.Begin(ctx, "user:1", "k1", "fp1")
		}, check: func(t *testing.T) {
			if ttl := mr.TTL(redisKey("user:1", "k1")); ttl <= 0 || ttl > PendingTTL {
				t.Errorf("pending TTL = %v, want (0, %v]", ttl, PendingTTL)
			}
		}},
		{name: "retry while running sees pending", wantRecord: Record{Fingerprint: "fp1", Pending: true}, action: func() (Record, bool, error) {
			return s.Begin(ctx, "user:1", "k1", "fp1")
		}},
		{name: "complete then retry sees the response", wantRecord: Record{Fingerprint: "fp1", Status: 201, Body: []byte(`{"key":"abc"}`)}, action: func() (Record, bool, error) {
			if err := s.Complete(ctx, "user:1", "k1", Record{Fingerprint: "fp1", Status: 201, Body: []byte(`{"key":"abc"}`)}); err != nil {
				return Record{}, false, err
			}
			return s.Begin(ctx, "user:1", "k1", "fp1")
		}, check: func(t *testing.T) {
			if ttl := mr.TTL(redisKey("user:1", "k1")); ttl <= PendingTTL {
				t.Errorf("stored response TTL = %v, want the full %v", ttl, DefaultTTL)
			}
		}},
		{name: "a stuck pending key frees itself", wantStarted: true, action: func() (Record, bool, error) {
			if _, _, err := s.Begin(ctx, "user:3", "k1", "fp1"); err != nil {
				return Record{}, false, err
			}
			mr.FastForward(PendingTTL + time.Second) // the first request never completed
			return s.Begin(ctx, "user:3", "k1", "fp1")
		}},
		{name: "same key in another scope is free", wantStarted: true, action: func() (Record, bool, error) {
			return s.Begin(ctx, "user:2", "k1", "fp1")
		}},
		{name: "abort frees the key", wantStarted: true, action: func() (Record, bool, error) {
			if err := s.Abort(ctx, "user:2", "k1"); err != nil {
				return Record{}, false, err
			}
			return s.Begin(ctx, "user:2", "k1", "fp1")
		}},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			rec, started, err := step.action()
			if err != nil {
				t.Fatal(err)
			}
			if started != step.wantStarted {
				t.Fatalf("started = %v, want %v", started, step.wantStarted)
			}
			if rec.Fingerprint != step.wantRecord.Fingerprint || rec.Pending != step.wantRecord.Pending ||
				rec.Status != step.wantRecord.Status || string(rec.Body) != string(step.wantRecord.Body) {
				t.Errorf("record = %+v, want %+v", rec, step.wantRecord)
			}
			if step.check != nil {
				step.check(t)
			}
		})
		if !ok {
			break
		}
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(mr *miniredis.Miniredis)
		call  func(s *Store) error
	}{
		{
			name:  "begin with redis down",
			setup: func(mr *miniredis.Miniredis) { mr.Close() },
			call:  func(s *Store) error { _, _, err := s.Begin(context.Background(), "u", "k", "fp"); return err },
		},
		{
			name:  "corrupt record",
			setup: func(mr *miniredis.Miniredis) { _ = mr.Set(redisKey("u", "k"), "not json") },
			call:  func(s *Store) error { _, _, err := s.Begin(context.Background(), "u", "k", "fp"); return err },
		},
		{
			name:  "complete with redis down",
			setup: func(mr *miniredis.Miniredis) { mr.Close() },
			call:  func(s *Store) error { return s.Complete(context.Background(), "u", "k", Record{}) },
		},
		{
			name:  "abort with redis down",
			setup: func(mr *miniredis.Miniredis) { mr.Close() },
			call:  func(s *Store) error { return s.Abort(context.Background(), "u", "k") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, mr := newTestStore(t)
			tt.setup(mr)
			if err := tt.call(s); err == nil {
				t.Error("err = nil, want an error")
			}
		})
	}
}
