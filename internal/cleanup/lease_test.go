package cleanup

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeLease struct {
	mu        sync.Mutex
	holder    string
	renewOK   bool
	renewErrs int // the first renewals fail with an error
	renewals  int
	released  bool
	acquireEr error
}

func (l *fakeLease) AcquireLease(_ context.Context, _, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acquireEr != nil {
		return false, l.acquireEr
	}
	if l.holder != "" && l.holder != owner {
		return false, nil
	}
	l.holder = owner
	return true, nil
}

func (l *fakeLease) RenewLease(context.Context, string, string, time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renewals++
	if l.renewals <= l.renewErrs {
		return false, errors.New("write timeout")
	}
	return l.renewOK, nil
}

func (l *fakeLease) ReleaseLease(_ context.Context, _, owner string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == owner {
		l.holder, l.released = "", true
	}
	return nil
}

func TestWithLease(t *testing.T) {
	errWork := errors.New("work failed")
	tests := []struct {
		name         string
		holder       string // already holding the lease
		renewOK      bool
		acquireErr   error
		work         func(ctx context.Context) error
		wantHeld     bool
		wantErr      error
		wantRan      bool
		wantReleased bool
	}{
		{name: "runs while holding the lease and releases it", renewOK: true,
			work: func(context.Context) error { return nil }, wantHeld: true, wantRan: true, wantReleased: true},
		{name: "another owner holds it", holder: "other",
			work: func(context.Context) error { return nil }, wantHeld: false},
		{name: "acquire fails", acquireErr: errors.New("cassandra down"),
			work: func(context.Context) error { return nil }, wantErr: errors.New("cassandra down")},
		{name: "work error is returned, lease released", renewOK: true,
			work: func(context.Context) error { return errWork }, wantHeld: true, wantErr: errWork, wantRan: true, wantReleased: true},
		{name: "lost renewal cancels the work", renewOK: false,
			work: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return nil
				}
			}, wantHeld: true, wantErr: ErrLeaseLost, wantRan: true, wantReleased: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &fakeLease{holder: tt.holder, renewOK: tt.renewOK, acquireEr: tt.acquireErr}
			ran := false
			held, err := WithLease(context.Background(), l, "links", "me", 30*time.Millisecond, func(ctx context.Context) error {
				ran = true
				return tt.work(ctx)
			})
			if held != tt.wantHeld || ran != tt.wantRan {
				t.Errorf("held = %v, ran = %v; want %v, %v", held, ran, tt.wantHeld, tt.wantRan)
			}
			switch {
			case tt.wantErr == nil && err != nil,
				tt.wantErr != nil && err == nil,
				tt.wantErr != nil && !errors.Is(err, tt.wantErr) && err.Error() != tt.wantErr.Error():
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if l.released != tt.wantReleased {
				t.Errorf("released = %v, want %v", l.released, tt.wantReleased)
			}
		})
	}
}

func TestWithLeaseRenews(t *testing.T) {
	tests := []struct {
		name         string
		work         time.Duration
		ttl          time.Duration
		renewErrs    int
		wantErr      error
		wantRenewals int // at least
	}{
		{name: "renews every ttl/3 during long work", work: 100 * time.Millisecond, ttl: 30 * time.Millisecond, wantRenewals: 5},
		{name: "one failed renewal is retried", work: 100 * time.Millisecond, ttl: 30 * time.Millisecond, renewErrs: 1, wantRenewals: 5},
		{name: "failing for 2/3 of the ttl loses the lease", work: time.Second, ttl: 30 * time.Millisecond, renewErrs: 1000, wantErr: ErrLeaseLost, wantRenewals: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &fakeLease{renewOK: true, renewErrs: tt.renewErrs}
			_, err := WithLease(context.Background(), l, "links", "me", tt.ttl, func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(tt.work):
					return nil
				}
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if l.renewals < tt.wantRenewals {
				t.Errorf("renewals = %d, want at least %d", l.renewals, tt.wantRenewals)
			}
		})
	}
}
