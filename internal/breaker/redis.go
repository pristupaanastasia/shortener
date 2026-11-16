package breaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Hook applies a Breaker to a go-redis client: client.AddHook(Hook(b)).
// Several clients of the same Redis can share one Breaker.
func Hook(b *Breaker) redis.Hook {
	return hook{b: b}
}

type hook struct{ b *Breaker }

// Bypass returns a context whose Redis calls skip the breaker and do not
// count towards it. It is for rare writes that must not be dropped while the
// breaker is open, e.g. cache invalidation: losing one would serve a stale
// entry for its whole TTL once Redis answers again.
func Bypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, inCall{}, true)
}

// inCall marks the context of a call the breaker let through. go-redis runs
// the handshake of a new connection (HELLO, then a pipeline) through the same
// hooks with the caller's context; without the mark, the handshake of a
// half-open probe would be rejected as a second probe, and the breaker could
// never close. The outer call reports the outcome for both.
type inCall struct{}

func (h hook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if ctx.Value(inCall{}) != nil {
			return next(ctx, cmd)
		}
		allowed, probe := h.b.allow()
		if !allowed {
			cmd.SetErr(ErrOpen)
			return ErrOpen
		}
		err := next(context.WithValue(ctx, inCall{}, true), cmd)
		o := outcome(ctx, err)
		h.b.Done(o)
		if probe && o == Failure {
			err = fmt.Errorf("%w: %w", ErrProbeFailed, err)
			cmd.SetErr(err)
		}
		return err
	}
}

func (h hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if ctx.Value(inCall{}) != nil {
			return next(ctx, cmds)
		}
		allowed, probe := h.b.allow()
		if !allowed {
			for _, c := range cmds {
				c.SetErr(ErrOpen)
			}
			return ErrOpen
		}
		err := next(context.WithValue(ctx, inCall{}, true), cmds)
		o := outcome(ctx, err)
		h.b.Done(o)
		// The commands keep their own errors: they were sent, unlike with
		// ErrOpen, and callers such as the rate limiter tell the two apart.
		if probe && o == Failure {
			err = fmt.Errorf("%w: %w", ErrProbeFailed, err)
		}
		return err
	}
}

// outcome classifies the result of a call. A missing key (redis.Nil) and an
// error reply from the server (e.g. WRONGTYPE) mean Redis answered; a call
// cancelled by its caller says nothing about Redis.
func outcome(ctx context.Context, err error) Outcome {
	if err == nil || errors.Is(err, redis.Nil) {
		return Success
	}
	if _, ok := errors.AsType[redis.Error](err); ok {
		return Success
	}
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return Ignored
	}
	return Failure
}
