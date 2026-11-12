package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pristupaanastasia/shortener/internal/link"
)

func TestCreateGet(t *testing.T) {
	ctx := context.Background()
	s := New()

	if _, err := s.Get(ctx, "abc1234"); !errors.Is(err, link.ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}

	l := link.Link{Key: "abc1234", LongURL: "https://example.com"}
	if err := s.Create(ctx, l); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, "abc1234")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.LongURL != l.LongURL {
		t.Errorf("LongURL = %q, want %q", got.LongURL, l.LongURL)
	}

	if err := s.Create(ctx, link.Link{Key: "abc1234", LongURL: "https://other.com"}); !errors.Is(err, link.ErrKeyTaken) {
		t.Errorf("Create duplicate: err = %v, want ErrKeyTaken", err)
	}
}

func TestCreateConcurrentSameKey(t *testing.T) {
	ctx := context.Background()
	s := New()

	const n = 50
	var wg sync.WaitGroup
	var created atomic.Int32
	for range n {
		wg.Go(func() {
			if err := s.Create(ctx, link.Link{Key: "same123"}); err == nil {
				created.Add(1)
			}
		})
	}
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Errorf("%d concurrent creates succeeded, want exactly 1", got)
	}
}
