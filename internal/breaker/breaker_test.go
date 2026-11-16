package breaker

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// call is one step against a breaker: either time passes, or a call is made
// and, if allowed, finishes with an outcome.
type call struct {
	wait      time.Duration
	outcome   Outcome
	wantAllow bool
	wantState State // after the step
}

func TestBreaker(t *testing.T) {
	ok := func(s State) call { return call{outcome: Success, wantAllow: true, wantState: s} }
	fail := func(s State) call { return call{outcome: Failure, wantAllow: true, wantState: s} }
	rejected := call{wantAllow: false, wantState: Open}
	wait := func(d time.Duration) call { return call{wait: d, wantState: -1} }

	tests := []struct {
		name  string
		steps []call
	}{
		{name: "successes keep it closed", steps: []call{ok(Closed), ok(Closed), ok(Closed)}},
		{name: "opens after 3 consecutive failures", steps: []call{fail(Closed), fail(Closed), fail(Open), rejected}},
		{name: "a success resets the failure count", steps: []call{fail(Closed), fail(Closed), ok(Closed), fail(Closed), fail(Closed), fail(Open)}},
		{name: "rejects during the cooldown", steps: []call{fail(Closed), fail(Closed), fail(Open), wait(time.Second - time.Nanosecond), rejected}},
		{name: "successful probe closes it", steps: []call{fail(Closed), fail(Closed), fail(Open), wait(time.Second), ok(Closed), ok(Closed)}},
		{name: "failed probe reopens it for another cooldown", steps: []call{
			fail(Closed), fail(Closed), fail(Open), wait(time.Second), fail(Open), wait(time.Second / 2), rejected, wait(time.Second / 2), ok(Closed),
		}},
		{name: "one failed probe is enough to reopen", steps: []call{fail(Closed), fail(Closed), fail(Open), wait(time.Second), fail(Open), rejected}},
		{name: "ignored probe lets the next call probe", steps: []call{
			fail(Closed), fail(Closed), fail(Open), wait(time.Second), {outcome: Ignored, wantAllow: true, wantState: Open}, ok(Closed),
		}},
		{name: "ignored calls do not count as failures", steps: []call{
			fail(Closed), fail(Closed), {outcome: Ignored, wantAllow: true, wantState: Closed}, {outcome: Ignored, wantAllow: true, wantState: Closed}, fail(Open),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(0, 0)
			b := New(Config{Failures: 3, Cooldown: time.Second})
			b.now = func() time.Time { return now }
			for i, s := range tt.steps {
				if s.wait > 0 {
					now = now.Add(s.wait)
					continue
				}
				allowed := b.Allow()
				if allowed != s.wantAllow {
					t.Fatalf("step %d: Allow() = %v, want %v", i, allowed, s.wantAllow)
				}
				if allowed {
					b.Done(s.outcome)
				}
				if got := b.State(); got != s.wantState {
					t.Fatalf("step %d: State() = %v, want %v", i, got, s.wantState)
				}
			}
		})
	}
}

func TestBreakerHalfOpenAdmitsOneProbe(t *testing.T) {
	tests := []struct {
		name       string
		concurrent int
		wantAllow  []bool
	}{
		{name: "single caller probes", concurrent: 1, wantAllow: []bool{true}},
		{name: "callers during the probe are rejected", concurrent: 3, wantAllow: []bool{true, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(0, 0)
			b := New(Config{Failures: 1, Cooldown: time.Second})
			b.now = func() time.Time { return now }
			b.Allow()
			b.Done(Failure)
			now = now.Add(time.Second)
			var got []bool
			for range tt.concurrent {
				got = append(got, b.Allow()) // none finishes: the probe is in flight
			}
			if !slices.Equal(got, tt.wantAllow) {
				t.Errorf("Allow() during half-open = %v, want %v", got, tt.wantAllow)
			}
			if b.State() != HalfOpen {
				t.Errorf("State() = %v, want %v", b.State(), HalfOpen)
			}
		})
	}
}

func TestBreakerOnChange(t *testing.T) {
	type change struct{ from, to State }
	tests := []struct {
		name     string
		outcomes []Outcome // one call each, after a cooldown when open
		want     []change
	}{
		{name: "no changes while healthy", outcomes: []Outcome{Success, Success}, want: nil},
		{name: "open, probe, close", outcomes: []Outcome{Failure, Failure, Success}, want: []change{
			{Closed, Open}, {Open, HalfOpen}, {HalfOpen, Closed},
		}},
		{name: "failed probe", outcomes: []Outcome{Failure, Failure, Failure}, want: []change{
			{Closed, Open}, {Open, HalfOpen}, {HalfOpen, Open},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(0, 0)
			var got []change
			b := New(Config{Failures: 2, Cooldown: time.Second, OnChange: func(from, to State) {
				got = append(got, change{from, to})
			}})
			b.now = func() time.Time { return now }
			for _, o := range tt.outcomes {
				now = now.Add(time.Second)
				if b.Allow() {
					b.Done(o)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("changes = %v, want %v", got, tt.want)
			}
		})
	}
}

// Regression: OnChange ran after the lock was released, so racing changes
// could be reported out of order (e.g. "closed" then "open" while the
// breaker was closed).
func TestBreakerOnChangeOrder(t *testing.T) {
	tests := []struct {
		name       string
		goroutines int
		calls      int
	}{
		{name: "flaky dependency, many callers", goroutines: 8, calls: 2000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type change struct{ from, to State }
			var got []change
			b := New(Config{Failures: 1, Cooldown: 0, OnChange: func(from, to State) {
				got = append(got, change{from, to}) // under the breaker's lock
				time.Sleep(time.Microsecond)        // widen any race window
			}})
			var wg sync.WaitGroup
			for g := range tt.goroutines {
				wg.Go(func() {
					for i := range tt.calls {
						if b.Allow() {
							b.Done(Outcome((g + i) % 2)) // Success and Failure alternate
						}
					}
				})
			}
			wg.Wait()
			if len(got) == 0 {
				t.Fatal("no state changes")
			}
			for i := 1; i < len(got); i++ {
				if got[i].from != got[i-1].to {
					t.Fatalf("change %d: %v->%v follows %v->%v", i, got[i].from, got[i].to, got[i-1].from, got[i-1].to)
				}
			}
			if last := got[len(got)-1].to; last != b.State() {
				t.Errorf("last reported state = %v, actual %v", last, b.State())
			}
		})
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{name: "closed", state: Closed, want: "closed"},
		{name: "half-open", state: HalfOpen, want: "half-open"},
		{name: "open", state: Open, want: "open"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
