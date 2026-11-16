//go:build integration

package cassandra

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/cleanup"
	"github.com/pristupaanastasia/shortener/internal/link"
)

// testDay returns a day no other run uses, so index partitions are private
// to the test.
func testDay() time.Time {
	return time.Date(2300+rand.IntN(5000), time.Month(1+rand.IntN(12)), 1+rand.IntN(28), 0, 0, 0, 0, time.UTC)
}

func indexed(t *testing.T, s *Store, day time.Time, key string) bool {
	t.Helper()
	entries, err := s.Expired(context.Background(), day, expiryBucket(key), day.AddDate(0, 0, 1), 1000)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(entries, func(e cleanup.Entry) bool { return e.Key == key })
}

func TestExpiryIndex(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	day, otherDay := testDay(), testDay()
	at := day.Add(10 * time.Hour)
	later := otherDay.Add(3 * time.Hour)
	l := link.Link{Key: uniqueKey(), LongURL: "https://example.com", CreatedAt: now(), ExpiresAt: &at}
	permanent := link.Link{Key: uniqueKey(), LongURL: "https://example.com", CreatedAt: now()}

	steps := []struct {
		name   string
		action func(t *testing.T)
		key    string
		day    time.Time
		want   bool
	}{
		{name: "create indexes the expiry", action: func(t *testing.T) {
			if err := s.Create(ctx, l, link.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}, key: l.Key, day: day, want: true},
		{name: "a permanent link is not indexed", action: func(t *testing.T) {
			if err := s.Create(ctx, permanent, link.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}, key: permanent.Key, day: day, want: false},
		{name: "changing the expiry indexes the new day", action: func(t *testing.T) {
			l.ExpiresAt = &later
			if err := s.Update(ctx, l, link.Fields{ExpiresAt: true}); err != nil {
				t.Fatal(err)
			}
		}, key: l.Key, day: otherDay, want: true},
		{name: "the old entry stays as a hint", action: func(*testing.T) {}, key: l.Key, day: day, want: true},
		{name: "entries before the cutoff only", action: func(t *testing.T) {
			got, err := s.Expired(ctx, otherDay, expiryBucket(l.Key), later, 1000)
			if err != nil || len(got) != 0 {
				t.Fatalf("Expired(before = expires_at) = %v, %v; want none", got, err)
			}
		}, key: l.Key, day: otherDay, want: true},
		{name: "delete an entry", action: func(t *testing.T) {
			if err := s.DeleteExpiry(ctx, day, expiryBucket(l.Key), cleanup.Entry{Key: l.Key, ExpiresAt: at}); err != nil {
				t.Fatal(err)
			}
		}, key: l.Key, day: day, want: false},
		{name: "delete a partition", action: func(t *testing.T) {
			if err := s.DeleteExpiryPartition(ctx, otherDay, expiryBucket(l.Key)); err != nil {
				t.Fatal(err)
			}
		}, key: l.Key, day: otherDay, want: false},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			step.action(t)
			if got := indexed(t, s, step.day, step.key); got != step.want {
				t.Errorf("indexed on %s = %v, want %v", step.day.Format(time.DateOnly), got, step.want)
			}
		})
		if !ok {
			break
		}
	}
}

func TestTombstone(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	past := now().Add(-time.Hour)
	user := gocql.MustRandomUUID().String()
	tests := []struct {
		name       string
		setup      func(t *testing.T, l *link.Link) // l is the link as the worker read it
		want       cleanup.TombstoneResult
		wantGone   bool // tombstone: no URL, deleted
		wantListed bool
	}{
		{name: "expired link becomes a tombstone", setup: func(*testing.T, *link.Link) {}, want: cleanup.Tombstoned, wantGone: true},
		{name: "extended since it was read", setup: func(t *testing.T, l *link.Link) {
			fresh := *l
			later := now().Add(time.Hour)
			fresh.ExpiresAt = &later
			if err := s.Update(ctx, fresh, link.Fields{ExpiresAt: true}); err != nil {
				t.Fatal(err)
			}
		}, want: cleanup.Changed, wantListed: true},
		{name: "deleted by its owner", setup: func(t *testing.T, l *link.Link) {
			deleted := *l
			at := now()
			deleted.DeletedAt = &at
			if err := s.Delete(ctx, deleted); err != nil {
				t.Fatal(err)
			}
		}, want: cleanup.AlreadyDeleted},
		{name: "repeated", setup: func(t *testing.T, l *link.Link) {
			if _, err := s.Tombstone(ctx, *l, now()); err != nil {
				t.Fatal(err)
			}
		}, want: cleanup.AlreadyDeleted, wantGone: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Created with a future expiry (the API rejects past ones), then
			// read as expired by setting it in the past directly.
			l := link.Link{Key: uniqueKey(), LongURL: "https://example.com", UserID: user, CreatedAt: now(), ExpiresAt: &past}
			if err := s.Create(ctx, l, link.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			tt.setup(t, &l)
			got, err := s.Tombstone(ctx, l, now())
			if err != nil || got != tt.want {
				t.Fatalf("Tombstone() = %v, %v; want %v", got, err, tt.want)
			}
			cur, err := s.Get(ctx, l.Key)
			if err != nil {
				t.Fatal(err)
			}
			if gone := cur.LongURL == "" && cur.DeletedAt != nil; gone != tt.wantGone {
				t.Errorf("after Tombstone: %+v; want tombstone: %v", cur, tt.wantGone)
			}
			if err := s.Create(ctx, link.Link{Key: l.Key, LongURL: "https://example.com/new", CreatedAt: now()}, link.CreateOptions{}); err != link.ErrKeyTaken {
				t.Errorf("reusing the key: %v, want ErrKeyTaken", err)
			}
			links, _, err := s.ListByUser(ctx, user, "", 1000)
			if err != nil {
				t.Fatal(err)
			}
			if listed := slices.ContainsFunc(links, func(x link.Link) bool { return x.Key == l.Key }); listed != tt.wantListed {
				t.Errorf("listed = %v, want %v", listed, tt.wantListed)
			}
		})
	}
}

func TestCheckpoint(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	name := "test-" + uniqueKey()
	day := testDay()
	steps := []struct {
		name   string
		set    *time.Time
		want   time.Time
		wantOK bool
	}{
		{name: "missing before the first run", wantOK: false},
		{name: "set", set: &day, want: day, wantOK: true},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if step.set != nil {
				if err := s.SetCheckpoint(ctx, name, *step.set); err != nil {
					t.Fatal(err)
				}
			}
			got, ok, err := s.Checkpoint(ctx, name)
			if err != nil || ok != step.wantOK || !got.Equal(step.want) {
				t.Errorf("Checkpoint() = %v, %v, %v; want %v, %v", got, ok, err, step.want, step.wantOK)
			}
		})
		if !ok {
			break
		}
	}
}

func TestLease(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	name := "test-" + uniqueKey()
	const ttl = 2 * time.Second
	steps := []struct {
		name string
		run  func() (bool, error)
		want bool
	}{
		{name: "a acquires", run: func() (bool, error) { return s.AcquireLease(ctx, name, "a", ttl) }, want: true},
		{name: "b cannot", run: func() (bool, error) { return s.AcquireLease(ctx, name, "b", ttl) }, want: false},
		{name: "a again (restarted)", run: func() (bool, error) { return s.AcquireLease(ctx, name, "a", ttl) }, want: true},
		{name: "a renews", run: func() (bool, error) { return s.RenewLease(ctx, name, "a", ttl) }, want: true},
		{name: "b cannot renew", run: func() (bool, error) { return s.RenewLease(ctx, name, "b", ttl) }, want: false},
		{name: "b cannot release a's lease", run: func() (bool, error) {
			if err := s.ReleaseLease(ctx, name, "b"); err != nil {
				return false, err
			}
			return s.RenewLease(ctx, name, "a", ttl) // a still holds it
		}, want: true},
		{name: "a releases, b acquires", run: func() (bool, error) {
			if err := s.ReleaseLease(ctx, name, "a"); err != nil {
				return false, err
			}
			return s.AcquireLease(ctx, name, "b", ttl)
		}, want: true},
		{name: "a renewal after losing it", run: func() (bool, error) { return s.RenewLease(ctx, name, "a", ttl) }, want: false},
		{name: "an abandoned lease expires", run: func() (bool, error) {
			time.Sleep(ttl + time.Second) // b stops renewing
			return s.AcquireLease(ctx, name, "c", ttl)
		}, want: true},
	}
	for i, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			got, err := step.run()
			if err != nil || got != step.want {
				t.Errorf("step %d = %v, %v; want %v", i, got, err, step.want)
			}
		})
		if !ok {
			break
		}
	}
}
