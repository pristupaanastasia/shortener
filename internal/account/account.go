// Package account registers users and authenticates API keys.
package account

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
)

// Errors returned by the service.
var (
	ErrNotFound     = errors.New("account not found")
	ErrEmailTaken   = errors.New("email already registered")
	ErrInvalidEmail = errors.New("invalid email")
	ErrInvalidKey   = errors.New("invalid api key")
)

// User is a registered account.
type User struct {
	ID        string // UUID
	Email     string
	CreatedAt time.Time
}

// Store persists users and the hashes of their API keys.
type Store interface {
	// CreateUser stores the user and an API key hash atomically, or returns
	// ErrEmailTaken.
	CreateUser(ctx context.Context, u User, keyHash []byte) error
	// UserByKeyHash returns the owner of an active key, or ErrNotFound.
	UserByKeyHash(ctx context.Context, keyHash []byte) (User, error)
}

const keyPrefix = "sk_"

// Service registers and authenticates users.
type Service struct {
	store Store
	now   func() time.Time
}

// NewService returns a Service backed by store.
func NewService(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

// Register creates a user and returns its API key. The key is shown only
// once: the store keeps just its hash.
func (s *Service) Register(ctx context.Context, email string) (User, string, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if addr, err := mail.ParseAddress(email); err != nil || addr.Address != email {
		return User{}, "", fmt.Errorf("%w: %q", ErrInvalidEmail, email)
	}

	u := User{ID: newUUID(), Email: email, CreatedAt: s.now().UTC().Truncate(time.Millisecond)}
	key := keyPrefix + rand.Text()
	if err := s.store.CreateUser(ctx, u, HashKey(key)); err != nil {
		return User{}, "", err
	}
	return u, key, nil
}

// Authenticate returns the owner of apiKey, or ErrInvalidKey.
func (s *Service) Authenticate(ctx context.Context, apiKey string) (User, error) {
	if !strings.HasPrefix(apiKey, keyPrefix) {
		return User{}, ErrInvalidKey
	}
	u, err := s.store.UserByKeyHash(ctx, HashKey(apiKey))
	if errors.Is(err, ErrNotFound) {
		return User{}, ErrInvalidKey
	}
	if err != nil {
		return User{}, fmt.Errorf("authenticate: %w", err)
	}
	return u, nil
}

// HashKey returns the stored form of an API key. Keys are random with ~130
// bits of entropy, so a fast hash is enough: there is nothing to brute-force.
func HashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

// newUUID returns a random (version 4) UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
