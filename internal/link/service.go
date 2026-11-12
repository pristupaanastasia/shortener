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

// Service creates and resolves links.
type Service struct {
	store   Store
	ownHost string
	now     func() time.Time
	// generate returns a candidate key; replaced in tests to force collisions.
	generate func(longURL string, now time.Time, attempt int) string
}

// NewService returns a Service backed by store. ownHost is the service's own
// host; links to it are rejected to prevent redirect loops.
func NewService(store Store, ownHost string) *Service {
	return &Service{
		store:    store,
		ownHost:  ownHost,
		now:      time.Now,
		generate: keygen.Generate,
	}
}

// CreateParams are the inputs for Create.
type CreateParams struct {
	LongURL   string
	Alias     string     // optional
	ExpiresAt *time.Time // optional
}

// Create validates p and stores a new link. With an alias, it fails with
// ErrAliasTaken if the alias exists. Without one, it generates keys and
// retries on collision.
func (s *Service) Create(ctx context.Context, p CreateParams) (Link, error) {
	now := s.now().UTC()
	if err := validateURL(p.LongURL, s.ownHost); err != nil {
		return Link{}, err
	}
	if err := validateExpiry(p.ExpiresAt, now); err != nil {
		return Link{}, err
	}

	l := Link{LongURL: p.LongURL, CreatedAt: now, ExpiresAt: p.ExpiresAt}

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
		l.Key = s.generate(p.LongURL, now, attempt)
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
