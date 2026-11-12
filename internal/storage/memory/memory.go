// Package memory is an in-memory link.Store for local runs and tests.
package memory

import (
	"context"
	"sync"

	"github.com/pristupaanastasia/shortener/internal/link"
)

// Store keeps links in a map guarded by a mutex.
type Store struct {
	mu    sync.RWMutex
	links map[string]link.Link
}

// New returns an empty Store.
func New() *Store {
	return &Store{links: make(map[string]link.Link)}
}

// Create implements link.Store.
func (s *Store) Create(_ context.Context, l link.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.links[l.Key]; ok {
		return link.ErrKeyTaken
	}
	s.links[l.Key] = l
	return nil
}

// Get implements link.Store.
func (s *Store) Get(_ context.Context, key string) (link.Link, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.links[key]
	if !ok {
		return link.Link{}, link.ErrNotFound
	}
	return l, nil
}
