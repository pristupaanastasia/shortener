package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newHooked returns a client of mr with a breaker that opens after 2
// failures, and a clock to move past its cooldown.
func newHooked(t *testing.T, mr *miniredis.Miniredis) (*redis.Client, *Breaker, *time.Time) {
	t.Helper()
	now := time.Unix(0, 0)
	b := New(Config{Failures: 2, Cooldown: time.Second})
	b.now = func() time.Time { return now }
	// Production timeouts (cache.RedisOptions, which this package cannot
	// import: cache imports breaker), without command retries.
	rdb := redis.NewClient(&redis.Options{
		Addr:               mr.Addr(),
		DialTimeout:        200 * time.Millisecond,
		ReadTimeout:        50 * time.Millisecond,
		WriteTimeout:       50 * time.Millisecond,
		MaxRetries:         -1,
		DialerRetries:      1,
		DialerRetryTimeout: 5 * time.Millisecond,
	})
	rdb.AddHook(Hook(b))
	t.Cleanup(func() { rdb.Close() })
	return rdb, b, &now
}

func TestHookOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		run       func(ctx context.Context, rdb *redis.Client) error
		down      bool // Redis stopped before the calls
		wantState State
	}{
		{name: "replies keep it closed", run: func(ctx context.Context, rdb *redis.Client) error {
			return rdb.Set(ctx, "k", "v", 0).Err()
		}, wantState: Closed},
		{name: "missing keys are not failures", run: func(ctx context.Context, rdb *redis.Client) error {
			return rdb.Get(ctx, "missing").Err()
		}, wantState: Closed},
		{name: "error replies are not failures", run: func(ctx context.Context, rdb *redis.Client) error {
			return rdb.LPush(ctx, "str", "x").Err() // WRONGTYPE: str is a string
		}, wantState: Closed},
		{name: "cancelled calls are not failures", run: func(_ context.Context, rdb *redis.Client) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return rdb.Get(ctx, "k").Err()
		}, down: true, wantState: Closed},
		{name: "connection failures open it", run: func(ctx context.Context, rdb *redis.Client) error {
			return rdb.Get(ctx, "k").Err()
		}, down: true, wantState: Open},
		{name: "failed pipelines open it", run: func(ctx context.Context, rdb *redis.Client) error {
			_, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
				p.Get(ctx, "a")
				p.Get(ctx, "b")
				return nil
			})
			return err
		}, down: true, wantState: Open},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			mr.Set("str", "v")
			rdb, b, _ := newHooked(t, mr)
			if tt.down {
				mr.Close()
			}
			for range 3 {
				if err := tt.run(t.Context(), rdb); errors.Is(err, ErrOpen) {
					break
				}
			}
			if got := b.State(); got != tt.wantState {
				t.Errorf("State() = %v, want %v", got, tt.wantState)
			}
		})
	}
}

func TestHookOpenSkipsRedis(t *testing.T) {
	tests := []struct {
		name string
		run  func(ctx context.Context, rdb *redis.Client) []error
	}{
		{name: "command", run: func(ctx context.Context, rdb *redis.Client) []error {
			return []error{rdb.Get(ctx, "k").Err()}
		}},
		{name: "pipeline sets the error on every command", run: func(ctx context.Context, rdb *redis.Client) []error {
			cmds, _ := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
				p.Get(ctx, "a")
				p.Incr(ctx, "b")
				return nil
			})
			errs := make([]error, 0, len(cmds))
			for _, c := range cmds {
				errs = append(errs, c.Err())
			}
			return errs
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb, b, _ := newHooked(t, mr)
			mr.Close()
			for range 2 {
				rdb.Get(t.Context(), "k")
			}
			if b.State() != Open {
				t.Fatalf("State() = %v, want open", b.State())
			}
			// Redis is back, but the breaker is open: nothing may reach it.
			if err := mr.Restart(); err != nil {
				t.Fatal(err)
			}
			for i, err := range tt.run(t.Context(), rdb) {
				if !errors.Is(err, ErrOpen) {
					t.Errorf("command %d: err = %v, want ErrOpen", i, err)
				}
			}
			if n := mr.CommandCount(); n != 0 {
				t.Errorf("Redis received %d commands while the breaker was open, want 0", n)
			}
		})
	}
}

// The probe after a cooldown needs a new connection, and go-redis runs the
// connection handshake through the same hooks: the breaker must not reject
// the handshake as a second probe.
func TestHookRecovers(t *testing.T) {
	tests := []struct {
		name      string
		recovered bool
		wantErr   error // nil: success
		wantState State
	}{
		{name: "probe through a new connection closes it", recovered: true, wantState: Closed},
		// Callers skip logging it: the breaker has logged the outage.
		{name: "probe against a dead Redis reopens it", recovered: false, wantErr: ErrProbeFailed, wantState: Open},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb, b, now := newHooked(t, mr)
			mr.Close()
			for range 2 {
				rdb.Get(t.Context(), "k")
			}
			if tt.recovered {
				if err := mr.Restart(); err != nil {
					t.Fatal(err)
				}
				mr.Set("k", "v")
			}
			*now = now.Add(time.Second)
			got, err := rdb.Get(t.Context(), "k").Result()
			if (err == nil) != (tt.wantErr == nil) || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("Get after the cooldown = %q, %v; want error %v", got, err, tt.wantErr)
			}
			if b.State() != tt.wantState {
				t.Errorf("State() = %v, want %v", b.State(), tt.wantState)
			}
		})
	}
}
