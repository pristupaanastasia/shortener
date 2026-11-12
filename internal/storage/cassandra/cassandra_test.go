//go:build integration

package cassandra

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/link"
)

const testKeyspace = "shortener_test"

func openTestStore(t *testing.T) *Store {
	t.Helper()
	hosts := os.Getenv("CASSANDRA_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := Open(ctx, Config{Hosts: strings.Split(hosts, ","), Keyspace: testKeyspace, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("open store (is Cassandra running? try `make up`): %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// uniqueKey avoids collisions with rows left by earlier runs.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return "t" + strings.ToLower(rand.Text())[:12]
}

func TestCreateGet(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	in := link.Link{
		Key:       uniqueKey(t),
		LongURL:   "https://example.com/a",
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
		ExpiresAt: &expires,
	}
	if err := s.Create(ctx, in); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, in.Key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LongURL != in.LongURL || !got.CreatedAt.Equal(in.CreatedAt) || got.UserID != "" {
		t.Errorf("Get = %+v, want %+v", got, in)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(expires) {
		t.Errorf("ExpiresAt = %v, want %v", got.ExpiresAt, expires)
	}
	if got.DeletedAt != nil {
		t.Errorf("DeletedAt = %v, want nil", got.DeletedAt)
	}
}

func TestGetMissing(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Get(context.Background(), uniqueKey(t)); !errors.Is(err, link.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCreateDuplicateKeepsOriginal(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t)

	if err := s.Create(ctx, link.Link{Key: key, LongURL: "https://first.example"}); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := s.Create(ctx, link.Link{Key: key, LongURL: "https://second.example"})
	if !errors.Is(err, link.ErrKeyTaken) {
		t.Fatalf("second Create: err = %v, want ErrKeyTaken", err)
	}

	got, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LongURL != "https://first.example" {
		t.Errorf("LongURL = %q: the duplicate insert overwrote the original", got.LongURL)
	}
}

// Without IF NOT EXISTS, Cassandra would silently let every insert win.
func TestCreateConcurrentSameKey(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t)

	const n = 20
	var wg sync.WaitGroup
	var created atomic.Int32
	for range n {
		wg.Go(func() {
			err := s.Create(ctx, link.Link{Key: key, LongURL: "https://example.com"})
			switch {
			case err == nil:
				created.Add(1)
			case !errors.Is(err, link.ErrKeyTaken):
				t.Errorf("Create: %v", err)
			}
		})
	}
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Errorf("%d concurrent creates succeeded, want exactly 1", got)
	}
}

func TestCreateWritesLinksByUser(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	userID := gocql.MustRandomUUID().String()
	in := link.Link{
		Key:       uniqueKey(t),
		LongURL:   "https://example.com/owned",
		UserID:    userID,
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := s.Create(ctx, in); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, in.Key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.UserID != userID {
		t.Errorf("UserID = %q, want %q", got.UserID, userID)
	}

	uid, err := gocql.ParseUUID(userID)
	if err != nil {
		t.Fatal(err)
	}
	var key, longURL string
	err = s.session.Query(`SELECT key, long_url FROM `+testKeyspace+`.links_by_user WHERE user_id = ?`,
		uid).ScanContext(ctx, &key, &longURL)
	if err != nil {
		t.Fatalf("select links_by_user: %v", err)
	}
	if key != in.Key || longURL != in.LongURL {
		t.Errorf("links_by_user row = (%q, %q), want (%q, %q)", key, longURL, in.Key, in.LongURL)
	}
}

func TestServiceEndToEnd(t *testing.T) {
	svc := link.NewService(openTestStore(t), "sho.rt")
	ctx := context.Background()

	created, err := svc.Create(ctx, link.CreateParams{LongURL: "https://example.com/e2e"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := svc.Resolve(ctx, created.Key)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.LongURL != created.LongURL {
		t.Errorf("LongURL = %q, want %q", got.LongURL, created.LongURL)
	}
}
