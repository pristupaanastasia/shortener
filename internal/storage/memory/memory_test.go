package memory

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pristupaanastasia/shortener/internal/link"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	s := New()
	deleted := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "get missing", run: func(t *testing.T) {
			if _, err := s.Get(ctx, "abc1234"); !errors.Is(err, link.ErrNotFound) {
				t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
			}
		}},
		{name: "create", run: func(t *testing.T) {
			if err := s.Create(ctx, link.Link{Key: "abc1234", LongURL: "https://example.com"}); err != nil {
				t.Fatalf("Create: %v", err)
			}
		}},
		{name: "get created", run: func(t *testing.T) {
			got, err := s.Get(ctx, "abc1234")
			if err != nil || got.LongURL != "https://example.com" {
				t.Fatalf("Get = %+v, %v, want https://example.com", got, err)
			}
		}},
		{name: "duplicate keeps original", run: func(t *testing.T) {
			if err := s.Create(ctx, link.Link{Key: "abc1234", LongURL: "https://other.com"}); !errors.Is(err, link.ErrKeyTaken) {
				t.Fatalf("Create duplicate err = %v, want ErrKeyTaken", err)
			}
			if got, _ := s.Get(ctx, "abc1234"); got.LongURL != "https://example.com" {
				t.Errorf("LongURL = %q: the duplicate overwrote the original", got.LongURL)
			}
		}},
		{name: "update writes only selected fields", run: func(t *testing.T) {
			if err := s.Update(ctx, link.Link{Key: "abc1234", LongURL: "https://example.com/new", ExpiresAt: &deleted}, link.Fields{LongURL: true}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			got, _ := s.Get(ctx, "abc1234")
			if got.LongURL != "https://example.com/new" || got.ExpiresAt != nil {
				t.Errorf("after Update = %+v, want new url and no expiry", got)
			}
		}},
		{name: "update missing", run: func(t *testing.T) {
			if err := s.Update(ctx, link.Link{Key: "missing"}, link.Fields{LongURL: true}); !errors.Is(err, link.ErrNotFound) {
				t.Errorf("Update(missing) err = %v, want ErrNotFound", err)
			}
		}},
		{name: "delete", run: func(t *testing.T) {
			if err := s.Delete(ctx, link.Link{Key: "abc1234", DeletedAt: &deleted}); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if got, _ := s.Get(ctx, "abc1234"); got.DeletedAt == nil || !got.DeletedAt.Equal(deleted) {
				t.Errorf("DeletedAt = %v, want %v", got.DeletedAt, deleted)
			}
		}},
		{name: "repeated delete keeps the first time", run: func(t *testing.T) {
			later := deleted.Add(time.Hour)
			if err := s.Delete(ctx, link.Link{Key: "abc1234", DeletedAt: &later}); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if got, _ := s.Get(ctx, "abc1234"); !got.DeletedAt.Equal(deleted) {
				t.Errorf("DeletedAt = %v, want %v", got.DeletedAt, deleted)
			}
		}},
		{name: "update deleted", run: func(t *testing.T) {
			if err := s.Update(ctx, link.Link{Key: "abc1234", LongURL: "https://x"}, link.Fields{LongURL: true}); !errors.Is(err, link.ErrNotFound) {
				t.Errorf("Update(deleted) err = %v, want ErrNotFound", err)
			}
		}},
		{name: "delete missing", run: func(t *testing.T) {
			if err := s.Delete(ctx, link.Link{Key: "missing", DeletedAt: &deleted}); !errors.Is(err, link.ErrNotFound) {
				t.Errorf("Delete(missing) err = %v, want ErrNotFound", err)
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			break
		}
	}
}

func TestCreateConcurrentSameKey(t *testing.T) {
	tests := []struct {
		name        string
		goroutines  int
		wantCreated int32
	}{
		{name: "single writer", goroutines: 1, wantCreated: 1},
		{name: "50 concurrent writers, one wins", goroutines: 50, wantCreated: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			var wg sync.WaitGroup
			var created atomic.Int32
			for range tt.goroutines {
				wg.Go(func() {
					if err := s.Create(context.Background(), link.Link{Key: "same123"}); err == nil {
						created.Add(1)
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

func TestListByUser(t *testing.T) {
	ctx := context.Background()
	s := New()
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	deleted := base
	for _, l := range []link.Link{
		{Key: "a1", UserID: "alice", CreatedAt: base.Add(1 * time.Second)},
		{Key: "a2", UserID: "alice", CreatedAt: base.Add(2 * time.Second)},
		// Same timestamp: ordered by key.
		{Key: "a3", UserID: "alice", CreatedAt: base.Add(3 * time.Second)},
		{Key: "a4", UserID: "alice", CreatedAt: base.Add(3 * time.Second)},
		{Key: "gone", UserID: "alice", CreatedAt: base.Add(4 * time.Second), DeletedAt: &deleted},
		{Key: "b1", UserID: "bob", CreatedAt: base.Add(5 * time.Second)},
	} {
		if err := s.Create(ctx, l); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		user      string
		limit     int
		wantPages [][]string
	}{
		{name: "one page", user: "alice", limit: 10, wantPages: [][]string{{"a3", "a4", "a2", "a1"}}},
		{name: "pages of two", user: "alice", limit: 2, wantPages: [][]string{{"a3", "a4"}, {"a2", "a1"}}},
		{name: "pages of three", user: "alice", limit: 3, wantPages: [][]string{{"a3", "a4", "a2"}, {"a1"}}},
		{name: "other user", user: "bob", limit: 10, wantPages: [][]string{{"b1"}}},
		{name: "no links", user: "carol", limit: 10, wantPages: [][]string{nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPages [][]string
			cursor := ""
			for range len(tt.wantPages) + 1 {
				page, next, err := s.ListByUser(ctx, tt.user, cursor, tt.limit)
				if err != nil {
					t.Fatalf("ListByUser: %v", err)
				}
				var keys []string
				for _, l := range page {
					keys = append(keys, l.Key)
				}
				gotPages = append(gotPages, keys)
				if next == "" {
					break
				}
				cursor = next
			}
			if len(gotPages) != len(tt.wantPages) {
				t.Fatalf("pages = %v, want %v", gotPages, tt.wantPages)
			}
			for i := range gotPages {
				if !slices.Equal(gotPages[i], tt.wantPages[i]) {
					t.Errorf("page %d = %v, want %v", i, gotPages[i], tt.wantPages[i])
				}
			}
		})
	}
}

func TestListByUserInvalidCursor(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{name: "not base64", cursor: "!!!"},
		{name: "base64 but not json", cursor: "bm90LWpzb24"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := New().ListByUser(context.Background(), "alice", tt.cursor, 10); !errors.Is(err, link.ErrInvalidCursor) {
				t.Errorf("ListByUser(cursor %q) err = %v, want ErrInvalidCursor", tt.cursor, err)
			}
		})
	}
}
