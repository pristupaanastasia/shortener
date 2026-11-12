package link

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pristupaanastasia/shortener/internal/keygen"
)

// maxKeyAttempts bounds retries on generated key collisions.
const maxKeyAttempts = 5

// Page size limits for List.
const (
	DefaultPageSize = 50
	MaxPageSize     = 100
)

// Service creates, resolves and manages links.
type Service struct {
	store   Store
	ownHost string
	now     func() time.Time
	// generate returns a candidate key; replaced in tests to force collisions.
	generate func(longURL string, attempt int) string
}

// NewService returns a Service backed by store. ownHost is the service's own
// host; links to it are rejected to prevent redirect loops.
func NewService(store Store, ownHost string) *Service {
	return &Service{
		store:    store,
		ownHost:  ownHost,
		now:      time.Now,
		generate: func(longURL string, _ int) string { return keygen.Generate(longURL, keygen.NewSalt()) },
	}
}

// timestamp returns the current time at the precision Cassandra stores, so a
// link reads back exactly as it was returned.
func (s *Service) timestamp() time.Time {
	return s.now().UTC().Truncate(time.Millisecond)
}

// CreateParams are the inputs for Create.
type CreateParams struct {
	LongURL   string
	Alias     string     // optional
	ExpiresAt *time.Time // optional
	UserID    string     // empty for anonymous links
}

// Create validates p and stores a new link. With an alias, it fails with
// ErrAliasTaken if the alias exists. Without one, it generates keys and
// retries on collision.
func (s *Service) Create(ctx context.Context, p CreateParams) (Link, error) {
	now := s.timestamp()
	if err := validateURL(p.LongURL, s.ownHost); err != nil {
		return Link{}, err
	}
	if err := validateExpiry(p.ExpiresAt, now); err != nil {
		return Link{}, err
	}

	l := Link{LongURL: p.LongURL, UserID: p.UserID, CreatedAt: now, ExpiresAt: p.ExpiresAt}

	if p.Alias != "" {
		if err := validateAlias(p.Alias); err != nil {
			return Link{}, err
		}
		l.Key = p.Alias
		err := s.store.Create(ctx, l)
		if errors.Is(err, ErrKeyTaken) {
			return Link{}, fmt.Errorf("%w: %q", ErrAliasTaken, p.Alias)
		}
		if err != nil {
			return Link{}, fmt.Errorf("create link %q: %w", l.Key, err)
		}
		return l, nil
	}

	for attempt := range maxKeyAttempts {
		l.Key = s.generate(p.LongURL, attempt)
		err := s.store.Create(ctx, l)
		if errors.Is(err, ErrKeyTaken) {
			continue
		}
		if err != nil {
			return Link{}, fmt.Errorf("create link %q: %w", l.Key, err)
		}
		return l, nil
	}
	return Link{}, fmt.Errorf("create link: no free key after %d attempts", maxKeyAttempts)
}

// Resolve returns the active link for key: ErrNotFound if it does not exist,
// ErrGone if it has expired or was deleted.
func (s *Service) Resolve(ctx context.Context, key string) (Link, error) {
	l, err := s.store.Get(ctx, key)
	if err != nil {
		return Link{}, err
	}
	if !l.Active(s.now()) {
		return Link{}, ErrGone
	}
	return l, nil
}

// Get returns a link owned by userID. Links of other users, anonymous links
// and deleted links all yield ErrNotFound, so the response does not reveal
// whether someone else's link exists.
func (s *Service) Get(ctx context.Context, userID, key string) (Link, error) {
	l, err := s.store.Get(ctx, key)
	if err != nil {
		return Link{}, err
	}
	if userID == "" || l.UserID != userID || l.DeletedAt != nil {
		return Link{}, ErrNotFound
	}
	return l, nil
}

// UpdateParams are the inputs for Update. Unset fields are left unchanged.
type UpdateParams struct {
	LongURL *string
	// SetExpiresAt applies ExpiresAt, where nil makes the link permanent.
	SetExpiresAt bool
	ExpiresAt    *time.Time
}

// Update changes the target URL or expiration of a link owned by userID.
func (s *Service) Update(ctx context.Context, userID, key string, p UpdateParams) (Link, error) {
	l, err := s.Get(ctx, userID, key)
	if err != nil {
		return Link{}, err
	}
	if p.LongURL != nil {
		if err := validateURL(*p.LongURL, s.ownHost); err != nil {
			return Link{}, err
		}
		l.LongURL = *p.LongURL
	}
	if p.SetExpiresAt {
		if err := validateExpiry(p.ExpiresAt, s.now()); err != nil {
			return Link{}, err
		}
		l.ExpiresAt = p.ExpiresAt
	}
	if err := s.store.Update(ctx, l); err != nil {
		return Link{}, fmt.Errorf("update link %q: %w", key, err)
	}
	return l, nil
}

// Delete soft-deletes a link owned by userID: the key is never reused and
// redirects return 410. Deleting an already deleted link succeeds.
func (s *Service) Delete(ctx context.Context, userID, key string) error {
	l, err := s.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if userID == "" || l.UserID != userID {
		return ErrNotFound
	}
	if l.DeletedAt != nil {
		return nil
	}
	now := s.timestamp()
	l.DeletedAt = &now
	if err := s.store.Update(ctx, l); err != nil {
		return fmt.Errorf("delete link %q: %w", key, err)
	}
	return nil
}

// List returns a page of links owned by userID. limit <= 0 means
// DefaultPageSize; it is capped at MaxPageSize.
func (s *Service) List(ctx context.Context, userID, cursor string, limit int) ([]Link, string, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	return s.store.ListByUser(ctx, userID, cursor, limit)
}
