// Package breaker is a circuit breaker for calls to a dependency that may
// hang or fail: after consecutive failures it opens and rejects calls at once
// for a cooldown, then lets one probe through to decide whether to close.
//
// For Redis it is installed as a go-redis hook (see Hook), so every command
// and pipeline of a client goes through it.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned instead of calling the dependency while the breaker is
// open. Callers treat it like the dependency being unavailable, without the
// wait and without logging every call.
var ErrOpen = errors.New("circuit breaker open")

// State is the breaker's state.
type State int

// The states.
const (
	Closed   State = iota // calls go through; consecutive failures are counted
	HalfOpen              // one probe call is in flight
	Open                  // calls are rejected until the cooldown ends
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	default:
		return "open"
	}
}

// Config tunes a Breaker.
type Config struct {
	// Failures is how many consecutive failures open the breaker.
	Failures int
	// Cooldown is how long the breaker stays open before a probe.
	Cooldown time.Duration
	// OnChange, if set, is called on every state change, outside the lock.
	OnChange func(from, to State)
}

// DefaultConfig opens after 5 consecutive failures, for 2 seconds: with the
// Redis client's 50 ms timeouts, a hang costs at most 5 slow calls per probe.
func DefaultConfig() Config {
	return Config{Failures: 5, Cooldown: 2 * time.Second}
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg Config
	now func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
}

// New returns a closed Breaker.
func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg, now: time.Now}
}

// State returns the current state; an open breaker whose cooldown has ended
// still reports Open until the next call probes.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Outcome is what a call that Allow let through says about the dependency.
type Outcome int

// The outcomes.
const (
	Success Outcome = iota // the dependency answered
	Failure                // the dependency failed or timed out
	Ignored                // nothing learned, e.g. the caller cancelled the call
)

// Allow reports whether a call may proceed. When it returns true, the caller
// must report the outcome with Done.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	var from, to State
	allowed := true
	switch b.state {
	case Closed:
	case HalfOpen:
		allowed = false // a probe is already in flight
	case Open:
		if b.now().Sub(b.openedAt) < b.cfg.Cooldown {
			allowed = false
		} else {
			from, to = b.state, HalfOpen
			b.state = HalfOpen // this call is the probe
		}
	}
	b.mu.Unlock()
	b.notify(from, to)
	return allowed
}

// Done records the outcome of a call that Allow let through.
func (b *Breaker) Done(o Outcome) {
	b.mu.Lock()
	from := b.state
	switch {
	case o == Ignored:
		if b.state == HalfOpen {
			// The probe told nothing: open again with the cooldown already
			// over, so the next call probes.
			b.state = Open
		}
	case o == Success:
		b.failures = 0
		b.state = Closed
	case b.state == HalfOpen:
		b.state, b.openedAt = Open, b.now() // the probe failed
	default:
		b.failures++
		if b.failures >= b.cfg.Failures {
			b.state, b.openedAt, b.failures = Open, b.now(), 0
		}
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

func (b *Breaker) notify(from, to State) {
	if from != to && b.cfg.OnChange != nil {
		b.cfg.OnChange(from, to)
	}
}
