package memory

import (
	"context"
	"sync"

	"github.com/pristupaanastasia/shortener/internal/account"
)

// Accounts is an in-memory account.Store.
type Accounts struct {
	mu      sync.RWMutex
	byEmail map[string]bool
	byKey   map[string]account.User
}

// NewAccounts returns an empty Accounts.
func NewAccounts() *Accounts {
	return &Accounts{byEmail: make(map[string]bool), byKey: make(map[string]account.User)}
}

// CreateUser implements account.Store.
func (a *Accounts) CreateUser(_ context.Context, u account.User, keyHash []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.byEmail[u.Email] {
		return account.ErrEmailTaken
	}
	a.byEmail[u.Email] = true
	a.byKey[string(keyHash)] = u
	return nil
}

// UserByKeyHash implements account.Store.
func (a *Accounts) UserByKeyHash(_ context.Context, keyHash []byte) (account.User, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	u, ok := a.byKey[string(keyHash)]
	if !ok {
		return account.User{}, account.ErrNotFound
	}
	return u, nil
}
