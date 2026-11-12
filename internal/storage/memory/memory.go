// Package memory is an in-memory link.Store and account.Store for local runs
// and tests.
package memory

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"slices"
	"sync"
	"time"

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

// Update implements link.Store.
func (s *Store) Update(_ context.Context, l link.Link, f link.Fields) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.links[l.Key]
	if !ok || cur.DeletedAt != nil {
		return link.ErrNotFound
	}
	if f.LongURL {
		cur.LongURL = l.LongURL
	}
	if f.ExpiresAt {
		cur.ExpiresAt = l.ExpiresAt
	}
	s.links[l.Key] = cur
	return nil
}

// Delete implements link.Store.
func (s *Store) Delete(_ context.Context, l link.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.links[l.Key]
	if !ok {
		return link.ErrNotFound
	}
	if cur.DeletedAt == nil {
		cur.DeletedAt = l.DeletedAt
		s.links[l.Key] = cur
	}
	return nil
}

// cursor is the position after the last returned link.
type cursor struct {
	CreatedAt time.Time `json:"c"`
	Key       string    `json:"k"`
}

// ListByUser implements link.Store. Links are ordered like links_by_user in
// Cassandra: created_at descending, then key ascending.
func (s *Store) ListByUser(_ context.Context, userID, rawCursor string, limit int) ([]link.Link, string, error) {
	var after *cursor
	if rawCursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(rawCursor)
		if err != nil {
			return nil, "", link.ErrInvalidCursor
		}
		after = &cursor{}
		if err := json.Unmarshal(data, after); err != nil {
			return nil, "", link.ErrInvalidCursor
		}
	}

	s.mu.RLock()
	var owned []link.Link
	for _, l := range s.links {
		if l.UserID == userID && l.DeletedAt == nil {
			owned = append(owned, l)
		}
	}
	s.mu.RUnlock()

	order := func(a, b link.Link) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.Key, b.Key)
	}
	slices.SortFunc(owned, order)
	if after != nil {
		pos := link.Link{CreatedAt: after.CreatedAt, Key: after.Key}
		i, _ := slices.BinarySearchFunc(owned, pos, func(l, target link.Link) int {
			if order(l, target) <= 0 {
				return -1
			}
			return 1
		})
		owned = owned[i:]
	}

	if len(owned) <= limit {
		return owned, "", nil
	}
	page := owned[:limit]
	last := page[len(page)-1]
	data, _ := json.Marshal(cursor{CreatedAt: last.CreatedAt, Key: last.Key})
	return page, base64.RawURLEncoding.EncodeToString(data), nil
}
