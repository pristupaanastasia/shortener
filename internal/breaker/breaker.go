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

// ErrProbeFailed wraps the error of a half-open probe that failed: the call
// did reach the dependency (unlike ErrOpen), but the breaker has logged the
// outage already, so callers stay quiet about it too.
var ErrProbeFailed = errors.New("circuit breaker probe failed")

// Quiet reports whether a caller should skip logging err: the breaker is open
// or its probe failed, and the breaker logs the outage once.
func Quiet(err error) bool {
	return errors.Is(err, ErrOpen) || errors.Is(err, ErrProbeFailed)
}

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
	// OnChange, if set, is called on every state change, in order. It runs
	// under the breaker's lock, so it must be quick and must not call the
	// breaker.
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
	allowed, _ := b.allow()
	return allowed
}

// allow is Allow that also reports whether the call is the half-open probe.
func (b *Breaker) allow() (allowed, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true, false
	case HalfOpen:
		return false, false // a probe is already in flight
	default:
		if b.now().Sub(b.openedAt) < b.cfg.Cooldown {
			return false, false
		}
		b.setState(HalfOpen) // this call is the probe
		return true, true
	}
}

// Done records the outcome of a call that Allow let through.
func (b *Breaker) Done(o Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case o == Ignored:
		if b.state == HalfOpen {
			// The probe told nothing: open again with the cooldown already
			// over, so the next call probes.
			b.setState(Open)
		}
	case o == Success:
		b.failures = 0
		b.setState(Closed)
	case b.state == HalfOpen:
		b.openedAt = b.now() // the probe failed
		b.setState(Open)
	default:
		b.failures++
		if b.failures >= b.cfg.Failures {
			b.openedAt, b.failures = b.now(), 0
			b.setState(Open)
		}
	}
}

// setState changes the state and notifies OnChange; b.mu must be held, which
// keeps notifications in the order of the changes.
func (b *Breaker) setState(to State) {
	from := b.state
	b.state = to
	if from != to && b.cfg.OnChange != nil {
		b.cfg.OnChange(from, to)
	}
}
