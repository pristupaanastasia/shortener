// Package link holds the link domain: the Link type, validation and the
// service that creates, resolves and manages links.
package link

import (
	"context"
	"errors"
	"time"
)

// Errors returned by the service. Handlers map them to HTTP status codes.
var (
	ErrNotFound      = errors.New("link not found")
	ErrGone          = errors.New("link expired or deleted")
	ErrAliasTaken    = errors.New("alias already taken")
	ErrInvalidURL    = errors.New("invalid url")
	ErrInvalidAlias  = errors.New("invalid alias")
	ErrInvalidExpiry = errors.New("invalid expiration")
	ErrInvalidCursor = errors.New("invalid cursor")
)

// ErrKeyTaken is returned by a Store when a link with the same key exists.
var ErrKeyTaken = errors.New("key already exists")

// Link is a short link.
type Link struct {
	Key       string
	LongURL   string
	UserID    string // empty for anonymous links
	CreatedAt time.Time
	ExpiresAt *time.Time // nil: never expires
	DeletedAt *time.Time // nil: not deleted
}

// Active reports whether the link can be followed at the given time.
func (l Link) Active(now time.Time) bool {
	if l.DeletedAt != nil {
		return false
	}
	return l.ExpiresAt == nil || now.Before(*l.ExpiresAt)
}

// Fields selects the columns Store.Update writes.
type Fields struct {
	LongURL   bool
	ExpiresAt bool
}

// Store persists links.
type Store interface {
	// Create inserts the link if no link with the same key exists,
	// and returns ErrKeyTaken otherwise. The check and insert are atomic.
	Create(ctx context.Context, l Link) error
	// Get returns the link with the given key, or ErrNotFound.
	Get(ctx context.Context, key string) (Link, error)
	// Update writes the fields selected by f from l to an existing, not
	// deleted link, or returns ErrNotFound. Only the selected columns are
	// written, and the "not deleted" check happens in the store, so a stale
	// read can neither revert a concurrent change nor undelete a link.
	Update(ctx context.Context, l Link, f Fields) error
	// Delete soft-deletes the link (DeletedAt is set to l.DeletedAt unless it
	// is already deleted) and removes it from its owner's listing. It is
	// idempotent: repeating it repairs a listing left behind by a failure.
	// It returns ErrNotFound if the link does not exist.
	Delete(ctx context.Context, l Link) error
	// ListByUser returns up to limit non-deleted links of a user, newest
	// first, and a cursor for the next page ("" when there are no more).
	// The cursor is opaque and specific to the store.
	ListByUser(ctx context.Context, userID, cursor string, limit int) ([]Link, string, error)
}
