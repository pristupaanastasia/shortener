package link

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapStore is a minimal Store for service tests.
type mapStore map[string]Link

func (m mapStore) Create(_ context.Context, l Link) error {
	if _, ok := m[l.Key]; ok {
		return ErrKeyTaken
	}
	m[l.Key] = l
	return nil
}

func (m mapStore) Get(_ context.Context, key string) (Link, error) {
	l, ok := m[key]
	if !ok {
		return Link{}, ErrNotFound
	}
	return l, nil
}

func (m mapStore) Update(_ context.Context, l Link, f Fields) error {
	cur, ok := m[l.Key]
	if !ok || cur.DeletedAt != nil {
		return ErrNotFound
	}
	if f.LongURL {
		cur.LongURL = l.LongURL
	}
	if f.ExpiresAt {
		cur.ExpiresAt = l.ExpiresAt
	}
	m[l.Key] = cur
	return nil
}

func (m mapStore) Delete(_ context.Context, l Link) error {
	cur, ok := m[l.Key]
	if !ok {
		return ErrNotFound
	}
	if cur.DeletedAt == nil {
		cur.DeletedAt = l.DeletedAt
		m[l.Key] = cur
	}
	return nil
}

// ListByUser is not exercised here: listing is tested against the real stores.
func (m mapStore) ListByUser(context.Context, string, string, int) ([]Link, string, error) {
	return nil, "", nil
}

var testNow = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

func newTestService(store Store) *Service {
	s := NewService(store, "sho.rt")
	s.now = func() time.Time { return testNow }
	return s
}

func ptr[T any](v T) *T { return &v }

func TestCreateValidation(t *testing.T) {
	tests := []struct {
		name    string
		params  CreateParams
		wantErr error
	}{
		{"valid", CreateParams{LongURL: "https://example.com/a"}, nil},
		{"valid http with alias", CreateParams{LongURL: "http://example.com", Alias: "sale_2026"}, nil},
		{"empty url", CreateParams{}, ErrInvalidURL},
		{"relative url", CreateParams{LongURL: "/path"}, ErrInvalidURL},
		{"ftp scheme", CreateParams{LongURL: "ftp://example.com"}, ErrInvalidURL},
		{"javascript scheme", CreateParams{LongURL: "javascript:alert(1)"}, ErrInvalidURL},
		{"too long", CreateParams{LongURL: "https://example.com/" + strings.Repeat("a", maxURLLength)}, ErrInvalidURL},
		{"own host", CreateParams{LongURL: "https://SHO.RT/abc1234"}, ErrInvalidURL},
		{"own host with default port", CreateParams{LongURL: "https://sho.rt:443/abc1234"}, ErrInvalidURL},
		{"own host with trailing dot", CreateParams{LongURL: "https://sho.rt./abc1234"}, ErrInvalidURL},
		{"own host with another port", CreateParams{LongURL: "http://sho.rt:8080/abc1234"}, ErrInvalidURL},
		{"subdomain is another host", CreateParams{LongURL: "https://api.sho.rt/abc1234"}, nil},
		{"alias too short", CreateParams{LongURL: "https://example.com", Alias: "abc"}, ErrInvalidAlias},
		{"alias too long", CreateParams{LongURL: "https://example.com", Alias: strings.Repeat("a", 31)}, ErrInvalidAlias},
		{"alias bad chars", CreateParams{LongURL: "https://example.com", Alias: "sale/2026"}, ErrInvalidAlias},
		{"alias reserved", CreateParams{LongURL: "https://example.com", Alias: "Admin"}, ErrInvalidAlias},
		{"expiry in past", CreateParams{LongURL: "https://example.com", ExpiresAt: ptr(testNow.Add(-time.Second))}, ErrInvalidExpiry},
		{"expiry now", CreateParams{LongURL: "https://example.com", ExpiresAt: ptr(testNow)}, ErrInvalidExpiry},
		{"expiry in future", CreateParams{LongURL: "https://example.com", ExpiresAt: ptr(testNow.Add(time.Hour))}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestService(mapStore{}).Create(context.Background(), tt.params)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateAlias(t *testing.T) {
	ctx := context.Background()
	s := newTestService(mapStore{})
	p := CreateParams{LongURL: "https://example.com", Alias: "sale2026"}

	steps := []struct {
		name    string
		wantKey string
		wantErr error
	}{
		{name: "first create uses the alias", wantKey: "sale2026"},
		{name: "second create is a conflict", wantErr: ErrAliasTaken},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			l, err := s.Create(ctx, p)
			if !errors.Is(err, step.wantErr) {
				t.Fatalf("Create(%+v) err = %v, want %v", p, err, step.wantErr)
			}
			if l.Key != step.wantKey {
				t.Errorf("Key = %q, want %q", l.Key, step.wantKey)
			}
		})
		if !ok {
			break
		}
	}
}

func TestCreateKeyCollisions(t *testing.T) {
	tests := []struct {
		name         string
		taken        []string
		candidates   []string // returned by the generator for each attempt
		wantKey      string
		wantErr      bool
		wantAttempts int
	}{
		{name: "free on first attempt", candidates: []string{"fresh01"}, wantKey: "fresh01", wantAttempts: 1},
		{name: "retries past collisions", taken: []string{"taken01", "taken02"}, candidates: []string{"taken01", "taken02", "fresh01"}, wantKey: "fresh01", wantAttempts: 3},
		{name: "gives up after max attempts", taken: []string{"taken01"}, candidates: []string{"taken01"}, wantErr: true, wantAttempts: maxKeyAttempts},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := mapStore{}
			for _, k := range tt.taken {
				store[k] = Link{Key: k}
			}
			s := newTestService(store)
			attempts := 0
			s.generate = func(_ string, attempt int) string {
				attempts++
				return tt.candidates[min(attempt, len(tt.candidates)-1)]
			}

			l, err := s.Create(context.Background(), CreateParams{LongURL: "https://example.com"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Create err = %v, want error: %v", err, tt.wantErr)
			}
			if l.Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", l.Key, tt.wantKey)
			}
			if attempts != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	store := mapStore{
		"active1": {Key: "active1", LongURL: "https://example.com"},
		"future1": {Key: "future1", LongURL: "https://example.com", ExpiresAt: ptr(testNow.Add(time.Minute))},
		"expired": {Key: "expired", LongURL: "https://example.com", ExpiresAt: ptr(testNow)},
		"deleted": {Key: "deleted", LongURL: "https://example.com", DeletedAt: ptr(testNow.Add(-time.Hour))},
	}
	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{name: "permanent link", key: "active1", wantErr: nil},
		{name: "expires later", key: "future1", wantErr: nil},
		{name: "expires exactly now", key: "expired", wantErr: ErrGone},
		{name: "deleted", key: "deleted", wantErr: ErrGone},
		{name: "missing", key: "missing", wantErr: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestService(store).Resolve(context.Background(), tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Resolve(%q) err = %v, want %v", tt.key, err, tt.wantErr)
			}
		})
	}
}

const (
	alice = "11111111-1111-4111-8111-111111111111"
	bob   = "22222222-2222-4222-8222-222222222222"
)

func ownershipStore() mapStore {
	return mapStore{
		"alice01": {Key: "alice01", LongURL: "https://example.com/a", UserID: alice},
		"anon001": {Key: "anon001", LongURL: "https://example.com/anon"},
		"gone001": {Key: "gone001", LongURL: "https://example.com/gone", UserID: alice, DeletedAt: ptr(testNow)},
	}
}

func TestGetOwnership(t *testing.T) {
	tests := []struct {
		name    string
		user    string
		key     string
		wantErr error
	}{
		{name: "owner", user: alice, key: "alice01", wantErr: nil},
		{name: "other user", user: bob, key: "alice01", wantErr: ErrNotFound},
		{name: "no user", user: "", key: "alice01", wantErr: ErrNotFound},
		{name: "anonymous link", user: alice, key: "anon001", wantErr: ErrNotFound},
		{name: "deleted link", user: alice, key: "gone001", wantErr: ErrNotFound},
		{name: "missing", user: alice, key: "missing", wantErr: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestService(ownershipStore()).Get(context.Background(), tt.user, tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Get(%q, %q) err = %v, want %v", tt.user, tt.key, err, tt.wantErr)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	newURL := "https://example.com/new"
	bad := "ftp://example.com"
	expires := testNow.Add(time.Hour)
	past := testNow.Add(-time.Hour)
	original := ownershipStore()["alice01"]

	tests := []struct {
		name        string
		user        string
		params      UpdateParams
		wantErr     error
		wantURL     string     // stored long_url after the call
		wantExpires *time.Time // stored expires_at after the call
	}{
		{name: "url and expiry", user: alice, params: UpdateParams{LongURL: &newURL, SetExpiresAt: true, ExpiresAt: &expires}, wantURL: newURL, wantExpires: &expires},
		{name: "url only keeps expiry", user: alice, params: UpdateParams{LongURL: &newURL}, wantURL: newURL},
		{name: "null expiry makes the link permanent", user: alice, params: UpdateParams{SetExpiresAt: true}, wantURL: original.LongURL},
		{name: "other user", user: bob, params: UpdateParams{LongURL: &newURL}, wantErr: ErrNotFound, wantURL: original.LongURL},
		{name: "invalid url", user: alice, params: UpdateParams{LongURL: &bad}, wantErr: ErrInvalidURL, wantURL: original.LongURL},
		{name: "expiry in past", user: alice, params: UpdateParams{SetExpiresAt: true, ExpiresAt: &past}, wantErr: ErrInvalidExpiry, wantURL: original.LongURL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := ownershipStore()
			l, err := newTestService(store).Update(context.Background(), tt.user, "alice01", tt.params)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Update err = %v, want %v", err, tt.wantErr)
			}
			stored := store["alice01"]
			if stored.LongURL != tt.wantURL {
				t.Errorf("stored LongURL = %q, want %q", stored.LongURL, tt.wantURL)
			}
			if (stored.ExpiresAt == nil) != (tt.wantExpires == nil) || (tt.wantExpires != nil && !stored.ExpiresAt.Equal(*tt.wantExpires)) {
				t.Errorf("stored ExpiresAt = %v, want %v", stored.ExpiresAt, tt.wantExpires)
			}
			if err == nil && l.LongURL != stored.LongURL {
				t.Errorf("returned LongURL = %q, stored %q", l.LongURL, stored.LongURL)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	store := ownershipStore()
	s := newTestService(store)

	steps := []struct {
		name    string
		user    string
		key     string
		wantErr error
		check   func(t *testing.T)
	}{
		{name: "other user cannot delete", user: bob, key: "alice01", wantErr: ErrNotFound, check: func(t *testing.T) {
			if store["alice01"].DeletedAt != nil {
				t.Error("link was deleted by another user")
			}
		}},
		{name: "owner deletes", user: alice, key: "alice01", check: func(t *testing.T) {
			if store["alice01"].DeletedAt == nil {
				t.Fatal("link was not soft-deleted")
			}
			if _, err := s.Resolve(ctx, "alice01"); !errors.Is(err, ErrGone) {
				t.Errorf("Resolve after delete err = %v, want ErrGone", err)
			}
		}},
		{name: "repeated delete is idempotent", user: alice, key: "alice01"},
		{name: "anonymous link", user: alice, key: "anon001", wantErr: ErrNotFound},
		{name: "missing link", user: alice, key: "missing", wantErr: ErrNotFound},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if err := s.Delete(ctx, step.user, step.key); !errors.Is(err, step.wantErr) {
				t.Fatalf("Delete(%q, %q) err = %v, want %v", step.user, step.key, err, step.wantErr)
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

// Regression: keys used to be salted with a millisecond timestamp, so
// concurrent creates of one URL collided on every attempt.
func TestCreateConcurrentSameURL(t *testing.T) {
	tests := []struct {
		name       string
		goroutines int
	}{
		{name: "200 concurrent creates of one url", goroutines: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewService(&lockedStore{m: mapStore{}}, "sho.rt")
			var wg sync.WaitGroup
			var failed atomic.Int32
			for range tt.goroutines {
				wg.Go(func() {
					if _, err := s.Create(context.Background(), CreateParams{LongURL: "https://example.com/popular"}); err != nil {
						failed.Add(1)
					}
				})
			}
			wg.Wait()
			if n := failed.Load(); n != 0 {
				t.Errorf("%d of %d creates failed", n, tt.goroutines)
			}
		})
	}
}

// lockedStore makes mapStore safe for concurrent use.
type lockedStore struct {
	mu sync.Mutex
	m  mapStore
}

func (s *lockedStore) Create(ctx context.Context, l Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Create(ctx, l)
}

func (s *lockedStore) Get(ctx context.Context, key string) (Link, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Get(ctx, key)
}

func (s *lockedStore) Update(ctx context.Context, l Link, f Fields) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Update(ctx, l, f)
}

func (s *lockedStore) Delete(ctx context.Context, l Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.Delete(ctx, l)
}

func (s *lockedStore) ListByUser(ctx context.Context, userID, cursor string, limit int) ([]Link, string, error) {
	return nil, "", nil
}

// staleStore serves Get from a snapshot taken before later writes, like an
// instance whose cache has not been invalidated yet.
type staleStore struct {
	mapStore
	snapshot map[string]Link
}

func (s staleStore) Get(_ context.Context, key string) (Link, error) {
	l, ok := s.snapshot[key]
	if !ok {
		return Link{}, ErrNotFound
	}
	return l, nil
}

// Regression: an update based on a stale read wrote every column back,
// undeleting the link and reverting concurrent changes.
func TestUpdateWithStaleRead(t *testing.T) {
	newURL := "https://example.com/new"
	expires := testNow.Add(time.Hour)

	tests := []struct {
		name        string
		change      func(m mapStore) // applied after the stale snapshot
		params      UpdateParams
		wantErr     error
		wantURL     string
		wantExpires bool
		wantDeleted bool
	}{
		{
			name:        "deleted elsewhere stays deleted",
			change:      func(m mapStore) { l := m["alice01"]; l.DeletedAt = ptr(testNow); m["alice01"] = l },
			params:      UpdateParams{LongURL: &newURL},
			wantErr:     ErrNotFound,
			wantURL:     "https://example.com/a",
			wantDeleted: true,
		},
		{
			name:        "expiry set elsewhere survives a url change",
			change:      func(m mapStore) { l := m["alice01"]; l.ExpiresAt = &expires; m["alice01"] = l },
			params:      UpdateParams{LongURL: &newURL},
			wantURL:     newURL,
			wantExpires: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := ownershipStore()
			snapshot := map[string]Link{}
			for k, v := range m {
				snapshot[k] = v
			}
			tt.change(m)

			_, err := newTestService(staleStore{mapStore: m, snapshot: snapshot}).Update(context.Background(), alice, "alice01", tt.params)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Update err = %v, want %v", err, tt.wantErr)
			}
			got := m["alice01"]
			if got.LongURL != tt.wantURL || (got.ExpiresAt != nil) != tt.wantExpires || (got.DeletedAt != nil) != tt.wantDeleted {
				t.Errorf("stored = %+v, want url %s, expiry %v, deleted %v", got, tt.wantURL, tt.wantExpires, tt.wantDeleted)
			}
		})
	}
}
