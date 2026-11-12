package account

import (
	"context"
	"errors"
	"testing"
)

// mapStore is an in-memory Store that counts key lookups.
type mapStore struct {
	byEmail map[string]bool
	byKey   map[string]User
	lookups int
}

func newMapStore() *mapStore {
	return &mapStore{byEmail: map[string]bool{}, byKey: map[string]User{}}
}

func (m *mapStore) CreateUser(_ context.Context, u User, keyHash []byte) error {
	if m.byEmail[u.Email] {
		return ErrEmailTaken
	}
	m.byEmail[u.Email] = true
	m.byKey[string(keyHash)] = u
	return nil
}

func (m *mapStore) UserByKeyHash(_ context.Context, keyHash []byte) (User, error) {
	m.lookups++
	u, ok := m.byKey[string(keyHash)]
	if !ok {
		return User{}, ErrNotFound
	}
	return u, nil
}

func TestRegisterAndAuthenticate(t *testing.T) {
	ctx := context.Background()
	store := newMapStore()
	s := NewService(store)
	var user User
	var key string

	steps := []struct {
		name        string
		run         func() error
		wantErr     error
		wantLookups int
	}{
		{name: "register", run: func() (err error) {
			user, key, err = s.Register(ctx, " Alice@Example.com ")
			if err == nil && (user.Email != "alice@example.com" || len(key) < 20 || key[:3] != "sk_" || len(user.ID) != 36) {
				t.Errorf("Register = %+v, key %q", user, key)
			}
			return err
		}},
		{name: "duplicate email", run: func() error { _, _, err := s.Register(ctx, "alice@example.com"); return err }, wantErr: ErrEmailTaken},
		{name: "invalid email", run: func() error { _, _, err := s.Register(ctx, "alice"); return err }, wantErr: ErrInvalidEmail},
		{name: "valid key", run: func() error {
			u, err := s.Authenticate(ctx, key)
			if err == nil && u.ID != user.ID {
				t.Errorf("Authenticate = %+v, want %+v", u, user)
			}
			return err
		}, wantLookups: 1},
		{name: "valid key again is cached", run: func() error {
			if _, ok := s.Cached(key); !ok {
				t.Error("key is not cached")
			}
			_, err := s.Authenticate(ctx, key)
			return err
		}, wantLookups: 1},
		{name: "unknown key", run: func() error { _, err := s.Authenticate(ctx, "sk_unknown"); return err }, wantErr: ErrInvalidKey, wantLookups: 2},
		{name: "unknown key is not cached", run: func() error { _, err := s.Authenticate(ctx, "sk_unknown"); return err }, wantErr: ErrInvalidKey, wantLookups: 3},
		{name: "key without prefix skips the store", run: func() error { _, err := s.Authenticate(ctx, "garbage"); return err }, wantErr: ErrInvalidKey, wantLookups: 3},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if err := step.run(); !errors.Is(err, step.wantErr) {
				t.Fatalf("err = %v, want %v", err, step.wantErr)
			}
			if store.lookups != step.wantLookups {
				t.Errorf("store lookups = %d, want %d", store.lookups, step.wantLookups)
			}
		})
		if !ok {
			break
		}
	}
}

func TestNewUUID(t *testing.T) {
	tests := []struct {
		name  string
		calls int
	}{
		{name: "1000 ids are distinct version 4 uuids", calls: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := map[string]bool{}
			for range tt.calls {
				id := newUUID()
				if len(id) != 36 || id[14] != '4' || seen[id] {
					t.Fatalf("newUUID() = %q: wrong format or repeated", id)
				}
				seen[id] = true
			}
		})
	}
}
