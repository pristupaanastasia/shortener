//go:build integration

package cleanup_test

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/cleanup"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/cassandra"
	"github.com/pristupaanastasia/shortener/internal/storage/s3"
)

// setup opens a fresh keyspace, so the worker sees only this test's links,
// and an archive prefix of its own.
func setup(t *testing.T) (*cassandra.Store, *s3.Archive) {
	t.Helper()
	ctx := context.Background()
	hosts := os.Getenv("CASSANDRA_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1"
	}
	endpoint := os.Getenv("ARCHIVE_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:8333"
	}
	id := strings.ToLower(rand.Text())[:10]
	ks := "cleanup_" + id
	store, err := cassandra.Open(ctx, cassandra.Config{Hosts: strings.Split(hosts, ","), Keyspace: ks, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("open Cassandra (is it running? try `make up`): %v", err)
	}
	admin, err := gocql.NewCluster(strings.Split(hosts, ",")...).CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		_ = admin.Query("DROP KEYSPACE IF EXISTS " + ks).Exec()
		admin.Close()
	})
	archive, err := s3.Open(ctx, s3.Config{Endpoint: endpoint, Bucket: "shortener-test", Prefix: id + "/", AccessKey: "shortener", SecretKey: "shortener"})
	if err != nil {
		t.Fatalf("open S3 (is it running? try `make up`): %v", err)
	}
	return store, archive
}

func archivedKeys(t *testing.T, a *s3.Archive) []string {
	t.Helper()
	ctx := context.Background()
	names, err := a.List(ctx, "expired/")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, name := range names {
		data, err := a.Get(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		for line := range strings.Lines(string(data)) {
			_, rest, _ := strings.Cut(line, `"key":"`)
			key, _, _ := strings.Cut(rest, `"`)
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// createExpiring stores links that expire in a second, then waits for them to
// expire (the API rejects expiries in the past).
func createExpiring(t *testing.T, s *cassandra.Store, user string, keys ...string) {
	t.Helper()
	at := time.Now().UTC().Add(time.Second).Truncate(time.Millisecond)
	for _, k := range keys {
		l := link.Link{Key: k, LongURL: "https://example.com/" + k, UserID: user, CreatedAt: time.Now().UTC().Truncate(time.Millisecond), ExpiresAt: &at}
		if err := s.Create(context.Background(), l, link.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Until(at) + 100*time.Millisecond)
}

func newWorker(s *cassandra.Store, a *s3.Archive) *cleanup.Worker {
	cfg := cleanup.DefaultConfig()
	cfg.Lookback = 1
	return cleanup.New(s, a, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestCleanup(t *testing.T) {
	s, a := setup(t)
	ctx := context.Background()
	user := gocql.MustRandomUUID().String()
	createExpiring(t, s, user, "exp1", "exp2")
	later := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	if err := s.Create(ctx, link.Link{Key: "live", LongURL: "https://example.com/live", UserID: user, CreatedAt: time.Now().UTC(), ExpiresAt: &later}, link.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	stats, err := newWorker(s, a).Run(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Archived != 2 {
		t.Errorf("archived %d, want 2", stats.Archived)
	}

	listed, _, err := s.ListByUser(ctx, user, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name         string
		key          string
		wantArchived bool
		wantGone     bool
	}{
		{name: "expired link is archived and tombstoned", key: "exp1", wantArchived: true, wantGone: true},
		{name: "second expired link too", key: "exp2", wantArchived: true, wantGone: true},
		{name: "live link is kept", key: "live"},
	}
	archived := archivedKeys(t, a)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slices.Contains(archived, tt.key); got != tt.wantArchived {
				t.Errorf("archived = %v, want %v (archive: %v)", got, tt.wantArchived, archived)
			}
			l, err := s.Get(ctx, tt.key)
			if err != nil {
				t.Fatal(err)
			}
			if gone := l.LongURL == "" && l.DeletedAt != nil; gone != tt.wantGone {
				t.Errorf("link = %+v; want tombstone: %v", l, tt.wantGone)
			}
			isListed := slices.ContainsFunc(listed, func(x link.Link) bool { return x.Key == tt.key })
			if isListed == tt.wantGone {
				t.Errorf("listed = %v, want %v", isListed, !tt.wantGone)
			}
			if err := s.Create(ctx, link.Link{Key: tt.key, LongURL: "https://example.com/x", CreatedAt: time.Now().UTC()}, link.CreateOptions{}); err != link.ErrKeyTaken {
				t.Errorf("reusing the key: %v, want ErrKeyTaken", err)
			}
		})
	}
}

// Without the lease, concurrent workers still archive every link and
// tombstone it exactly once: the lease only saves work.
func TestCleanupConcurrentWorkers(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		links   int
	}{
		{name: "four workers, no lease", workers: 4, links: 40},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, a := setup(t)
			var keys []string
			for i := range tt.links {
				keys = append(keys, "c"+strings.ToLower(rand.Text())[:6]+string(rune('a'+i%26)))
			}
			createExpiring(t, s, "", keys...)

			var mu sync.Mutex
			var total cleanup.Stats
			var wg sync.WaitGroup
			for range tt.workers {
				wg.Go(func() {
					stats, err := newWorker(s, a).Run(context.Background(), time.Now())
					if err != nil {
						t.Errorf("Run: %v", err)
					}
					mu.Lock()
					total.Archived += stats.Archived
					mu.Unlock()
				})
			}
			wg.Wait()
			if total.Archived != tt.links {
				t.Errorf("tombstoned %d in total, want each of the %d links exactly once", total.Archived, tt.links)
			}
			slices.Sort(keys)
			if got := archivedKeys(t, a); !slices.Equal(got, keys) {
				t.Errorf("archived %d distinct links, want %d", len(got), len(keys))
			}
		})
	}
}

func TestCleanupLease(t *testing.T) {
	s, _ := setup(t)
	tests := []struct {
		name     string
		workers  int
		wantHeld int
	}{
		{name: "one of concurrent runs holds the lease", workers: 4, wantHeld: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := make(chan struct{})
			var held sync.WaitGroup
			var mu sync.Mutex
			n := 0
			for i := range tt.workers {
				held.Go(func() {
					<-start
					ok, err := cleanup.WithLease(context.Background(), s, "links", string(rune('a'+i)), time.Minute, func(context.Context) error {
						time.Sleep(500 * time.Millisecond) // overlap with the others
						return nil
					})
					if err != nil {
						t.Errorf("WithLease: %v", err)
					}
					if ok {
						mu.Lock()
						n++
						mu.Unlock()
					}
				})
			}
			close(start)
			held.Wait()
			if n != tt.wantHeld {
				t.Errorf("%d runs held the lease, want %d", n, tt.wantHeld)
			}
		})
	}
}
