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
func uniqueKey() string {
	return "t" + strings.ToLower(rand.Text())[:12]
}

func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func TestCreateGet(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	expires := now().Add(time.Hour)

	tests := []struct {
		name string
		in   link.Link
	}{
		{name: "anonymous permanent link", in: link.Link{Key: uniqueKey(), LongURL: "https://example.com/a", CreatedAt: now()}},
		{name: "link with expiry", in: link.Link{Key: uniqueKey(), LongURL: "https://example.com/b", CreatedAt: now(), ExpiresAt: &expires}},
		{name: "owned link", in: link.Link{Key: uniqueKey(), LongURL: "https://example.com/c", CreatedAt: now(), UserID: gocql.MustRandomUUID().String()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.Create(ctx, tt.in); err != nil {
				t.Fatalf("Create: %v", err)
			}
			got, err := s.Get(ctx, tt.in.Key)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.LongURL != tt.in.LongURL || got.UserID != tt.in.UserID || !got.CreatedAt.Equal(tt.in.CreatedAt) || got.DeletedAt != nil {
				t.Errorf("Get = %+v, want %+v", got, tt.in)
			}
			if (got.ExpiresAt == nil) != (tt.in.ExpiresAt == nil) || (got.ExpiresAt != nil && !got.ExpiresAt.Equal(*tt.in.ExpiresAt)) {
				t.Errorf("ExpiresAt = %v, want %v", got.ExpiresAt, tt.in.ExpiresAt)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	key := uniqueKey()

	steps := []struct {
		name       string
		run        func() error
		wantErr    error
		wantAnyErr bool // for errors without a sentinel
		check      func(t *testing.T)
	}{
		{name: "get missing", run: func() error { _, err := s.Get(ctx, key); return err }, wantErr: link.ErrNotFound},
		{
			name: "update missing",
			run: func() error {
				return s.Update(ctx, link.Link{Key: key, LongURL: "https://example.com", CreatedAt: now()}, link.Fields{LongURL: true})
			},
			wantErr: link.ErrNotFound,
			check: func(t *testing.T) {
				// IF EXISTS must not create the row: a plain UPDATE is an upsert.
				if _, err := s.Get(ctx, key); !errors.Is(err, link.ErrNotFound) {
					t.Errorf("row created by a failed update: Get err = %v", err)
				}
			},
		},
		{name: "delete missing", run: func() error {
			deleted := now()
			return s.Delete(ctx, link.Link{Key: key, CreatedAt: now(), DeletedAt: &deleted})
		}, wantErr: link.ErrNotFound},
		{name: "create", run: func() error { return s.Create(ctx, link.Link{Key: key, LongURL: "https://first.example"}) }},
		{
			name:    "duplicate keeps the original",
			run:     func() error { return s.Create(ctx, link.Link{Key: key, LongURL: "https://second.example"}) },
			wantErr: link.ErrKeyTaken,
			check: func(t *testing.T) {
				if got, _ := s.Get(ctx, key); got.LongURL != "https://first.example" {
					t.Errorf("LongURL = %q: the duplicate overwrote the original", got.LongURL)
				}
			},
		},
		{
			name: "invalid user id",
			run: func() error {
				return s.Create(ctx, link.Link{Key: uniqueKey(), LongURL: "https://example.com", UserID: "not-a-uuid"})
			},
			wantAnyErr: true,
		},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			err := step.run()
			switch {
			case step.wantAnyErr && err == nil:
				t.Fatal("err = nil, want an error")
			case !step.wantAnyErr && !errors.Is(err, step.wantErr):
				t.Fatalf("err = %v, want %v", err, step.wantErr)
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

// Without IF NOT EXISTS, Cassandra would silently let every insert win.
func TestCreateConcurrentSameKey(t *testing.T) {
	s := openTestStore(t)
	tests := []struct {
		name        string
		goroutines  int
		wantCreated int32
	}{
		{name: "single writer", goroutines: 1, wantCreated: 1},
		{name: "20 concurrent writers, one wins", goroutines: 20, wantCreated: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := uniqueKey()
			var wg sync.WaitGroup
			var created atomic.Int32
			for range tt.goroutines {
				wg.Go(func() {
					err := s.Create(context.Background(), link.Link{Key: key, LongURL: "https://example.com"})
					switch {
					case err == nil:
						created.Add(1)
					case !errors.Is(err, link.ErrKeyTaken):
						t.Errorf("Create: %v", err)
					}
				})
			}
			wg.Wait()
			if got := created.Load(); got != tt.wantCreated {
				t.Errorf("%d creates succeeded, want %d", got, tt.wantCreated)
			}
		})
	}
}

func TestUpdateAndDelete(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	userID := gocql.MustRandomUUID().String()
	created := link.Link{Key: uniqueKey(), LongURL: "https://example.com/old", UserID: userID, CreatedAt: now()}
	stale := created // a copy read before the changes below, like a stale cache entry
	deletedAt := now()
	expires := now().Add(time.Hour)

	steps := []struct {
		name        string
		run         func() error
		wantErr     error
		wantURL     string
		wantExpires bool
		wantDeleted bool
		wantListed  bool
	}{
		{name: "create", run: func() error { return s.Create(ctx, created) },
			wantURL: "https://example.com/old", wantListed: true},
		{name: "update url in both tables", run: func() error {
			l := created
			l.LongURL = "https://example.com/new"
			return s.Update(ctx, l, link.Fields{LongURL: true})
		}, wantURL: "https://example.com/new", wantListed: true},
		{name: "expiry from a stale copy keeps the new url", run: func() error {
			l := stale
			l.ExpiresAt = &expires
			return s.Update(ctx, l, link.Fields{ExpiresAt: true})
		}, wantURL: "https://example.com/new", wantExpires: true, wantListed: true},
		{name: "delete", run: func() error {
			l := created
			l.DeletedAt = &deletedAt
			return s.Delete(ctx, l)
		}, wantURL: "https://example.com/new", wantExpires: true, wantDeleted: true},
		{name: "update from a stale copy does not undelete", run: func() error {
			l := stale
			l.LongURL = "https://example.com/resurrected"
			return s.Update(ctx, l, link.Fields{LongURL: true})
		}, wantErr: link.ErrNotFound, wantURL: "https://example.com/new", wantExpires: true, wantDeleted: true},
		{name: "repeated delete repairs a leftover listing row", run: func() error {
			// Simulate a failure between the two writes of an earlier delete.
			if err := s.session.Query(s.insertLinkByUser, mustUUID(t, userID), created.CreatedAt, created.Key, "https://example.com/new", nil).ExecContext(ctx); err != nil {
				return err
			}
			l := created
			later := deletedAt.Add(time.Hour)
			l.DeletedAt = &later
			return s.Delete(ctx, l)
		}, wantURL: "https://example.com/new", wantExpires: true, wantDeleted: true},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if err := step.run(); !errors.Is(err, step.wantErr) {
				t.Fatalf("err = %v, want %v", err, step.wantErr)
			}
			got, err := s.Get(ctx, created.Key)
			if err != nil {
				t.Fatal(err)
			}
			if got.LongURL != step.wantURL || (got.ExpiresAt != nil) != step.wantExpires || (got.DeletedAt != nil) != step.wantDeleted {
				t.Errorf("links row = %+v; want url %s, expiry %v, deleted %v", got, step.wantURL, step.wantExpires, step.wantDeleted)
			}
			if got.DeletedAt != nil && !got.DeletedAt.Equal(deletedAt) {
				t.Errorf("DeletedAt = %v, want the first deletion time %v", got.DeletedAt, deletedAt)
			}
			page, _, err := s.ListByUser(ctx, userID, "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if listed := len(page) == 1 && page[0].Key == created.Key; listed != step.wantListed || len(page) > 1 {
				t.Errorf("links_by_user = %+v, want listed %v", page, step.wantListed)
			}
			if step.wantListed && page[0].LongURL != step.wantURL {
				t.Errorf("links_by_user url = %q, want %q", page[0].LongURL, step.wantURL)
			}
		})
		if !ok {
			break
		}
	}
}

func TestListByUserPagination(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	userID := gocql.MustRandomUUID().String()
	base := now()
	const total = 7
	for i := range total {
		l := link.Link{Key: uniqueKey(), LongURL: "https://example.com", UserID: userID, CreatedAt: base.Add(time.Duration(i) * time.Second)}
		if err := s.Create(ctx, l); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		user      string
		limit     int
		wantTotal int
		wantPages int
	}{
		{name: "pages of three", user: userID, limit: 3, wantTotal: total, wantPages: 3},
		{name: "one big page", user: userID, limit: 100, wantTotal: total, wantPages: 1},
		{name: "user without links", user: gocql.MustRandomUUID().String(), limit: 3, wantTotal: 0, wantPages: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var all []link.Link
			cursor, pages := "", 0
			for pages <= total {
				page, next, err := s.ListByUser(ctx, tt.user, cursor, tt.limit)
				if err != nil {
					t.Fatalf("ListByUser: %v", err)
				}
				all = append(all, page...)
				pages++
				if next == "" {
					break
				}
				cursor = next
			}
			if len(all) != tt.wantTotal || pages != tt.wantPages {
				t.Fatalf("listed %d links in %d pages, want %d in %d", len(all), pages, tt.wantTotal, tt.wantPages)
			}
			seen := map[string]bool{}
			for i, l := range all {
				if seen[l.Key] {
					t.Errorf("key %q listed twice", l.Key)
				}
				seen[l.Key] = true
				if i > 0 && l.CreatedAt.After(all[i-1].CreatedAt) {
					t.Errorf("not newest first at %d: %v after %v", i, l.CreatedAt, all[i-1].CreatedAt)
				}
			}
		})
	}
}

func TestListByUserErrors(t *testing.T) {
	s := openTestStore(t)
	tests := []struct {
		name    string
		user    string
		cursor  string
		wantErr error
	}{
		{name: "cursor not base64", user: gocql.MustRandomUUID().String(), cursor: "!!!not-base64", wantErr: link.ErrInvalidCursor},
		{name: "user id not a uuid", user: "not-a-uuid", wantErr: link.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := s.ListByUser(context.Background(), tt.user, tt.cursor, 3); !errors.Is(err, tt.wantErr) {
				t.Errorf("ListByUser err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestServiceEndToEnd(t *testing.T) {
	svc := link.NewService(openTestStore(t), "sho.rt")
	tests := []struct {
		name   string
		params link.CreateParams
	}{
		{name: "generated key", params: link.CreateParams{LongURL: "https://example.com/e2e"}},
		{name: "alias", params: link.CreateParams{LongURL: "https://example.com/e2e", Alias: uniqueKey()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created, err := svc.Create(context.Background(), tt.params)
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			got, err := svc.Resolve(context.Background(), created.Key)
			if err != nil || got.LongURL != created.LongURL || !got.CreatedAt.Equal(created.CreatedAt) {
				t.Errorf("Resolve = %+v, %v, want %+v", got, err, created)
			}
		})
	}
}

func mustUUID(t *testing.T, s string) gocql.UUID {
	t.Helper()
	u, err := gocql.ParseUUID(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
