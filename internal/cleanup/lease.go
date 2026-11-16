package cleanup

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseLost cancels the work when the lease cannot be renewed.
var ErrLeaseLost = errors.New("cleanup lease lost")

// Lease is a named lock with a TTL in the database. It saves work: two
// workers would otherwise archive the same links twice. Correctness does not
// depend on it, since every write is conditional (lightweight transactions),
// so a worker that loses its lease mid-run cannot corrupt anything.
type Lease interface {
	AcquireLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, name, owner string) error
}

// WithLease runs fn while holding the lease, renewing it every ttl/3. It
// returns held=false without running fn if another owner holds the lease.
// If a renewal fails, fn's context is cancelled with ErrLeaseLost.
func WithLease(ctx context.Context, l Lease, name, owner string, ttl time.Duration, fn func(context.Context) error) (held bool, err error) {
	ok, err := l.AcquireLease(ctx, name, owner, ttl)
	if err != nil || !ok {
		return false, err
	}
	defer func() {
		// Released even if ctx is done, so the next run need not wait for
		// the TTL.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if rerr := l.ReleaseLease(rctx, name, owner); err == nil {
			err = rerr
		}
	}()

	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := make(chan struct{})
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-work.Done():
				return
			case <-t.C:
				ok, err := l.RenewLease(work, name, owner, ttl)
				if err != nil || !ok {
					cancel(errors.Join(ErrLeaseLost, err))
					return
				}
			}
		}
	}()
	err = fn(work)
	close(stop)
	<-renewed
	if cause := context.Cause(work); errors.Is(cause, ErrLeaseLost) {
		return true, cause
	}
	return true, err
}
