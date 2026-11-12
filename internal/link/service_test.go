package link

import (
	"context"
	"errors"
	"strings"
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

func TestCreateAliasTaken(t *testing.T) {
	ctx := context.Background()
	s := newTestService(mapStore{})
	p := CreateParams{LongURL: "https://example.com", Alias: "sale2026"}

	l, err := s.Create(ctx, p)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if l.Key != "sale2026" {
		t.Errorf("Key = %q, want alias", l.Key)
	}
	if _, err := s.Create(ctx, p); !errors.Is(err, ErrAliasTaken) {
		t.Errorf("second create: err = %v, want ErrAliasTaken", err)
	}
}

func TestCreateRetriesOnCollision(t *testing.T) {
	store := mapStore{"taken01": {Key: "taken01"}, "taken02": {Key: "taken02"}}
	s := newTestService(store)
	candidates := []string{"taken01", "taken02", "fresh01"}
	s.generate = func(_ string, _ time.Time, attempt int) string { return candidates[attempt] }

	l, err := s.Create(context.Background(), CreateParams{LongURL: "https://example.com"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if l.Key != "fresh01" {
		t.Errorf("Key = %q, want fresh01", l.Key)
	}
}

func TestCreateGivesUpAfterMaxAttempts(t *testing.T) {
	s := newTestService(mapStore{"taken01": {Key: "taken01"}})
	attempts := 0
	s.generate = func(string, time.Time, int) string {
		attempts++
		return "taken01"
	}

	if _, err := s.Create(context.Background(), CreateParams{LongURL: "https://example.com"}); err == nil {
		t.Fatal("Create succeeded, want error")
	}
	if attempts != maxKeyAttempts {
		t.Errorf("attempts = %d, want %d", attempts, maxKeyAttempts)
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
		key     string
		wantErr error
	}{
		{"active1", nil},
		{"future1", nil},
		{"expired", ErrGone},
		{"deleted", ErrGone},
		{"missing", ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			_, err := newTestService(store).Resolve(context.Background(), tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
