package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAllow(t *testing.T) {
	mr := miniredis.RunT(t)
	l := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	now := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
	l.now = func() time.Time { return now }
	resetAt := time.Date(2026, 11, 12, 10, 1, 0, 0, time.UTC)

	steps := []struct {
		name          string
		subject       string
		advance       time.Duration // moves the clock before the request
		wantAllowed   bool
		wantRemaining int
		wantReset     time.Time
	}{
		{name: "1st request", subject: "ip:1.2.3.4", wantAllowed: true, wantRemaining: 2, wantReset: resetAt},
		{name: "2nd request", subject: "ip:1.2.3.4", wantAllowed: true, wantRemaining: 1, wantReset: resetAt},
		{name: "3rd request uses the last slot", subject: "ip:1.2.3.4", wantAllowed: true, wantRemaining: 0, wantReset: resetAt},
		{name: "4th request is denied", subject: "ip:1.2.3.4", wantAllowed: false, wantRemaining: 0, wantReset: resetAt},
		{name: "another subject has its own quota", subject: "ip:5.6.7.8", wantAllowed: true, wantRemaining: 2, wantReset: resetAt},
		{name: "next window starts fresh", subject: "ip:1.2.3.4", advance: time.Minute, wantAllowed: true, wantRemaining: 2, wantReset: resetAt.Add(time.Minute)},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			now = now.Add(step.advance)
			res, err := l.Allow(context.Background(), step.subject, 3, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if res.Allowed != step.wantAllowed || res.Remaining != step.wantRemaining || res.Limit != 3 || !res.Reset.Equal(step.wantReset) {
				t.Fatalf("Allow(%s) = %+v, want allowed=%v remaining=%d reset=%v",
					step.subject, res, step.wantAllowed, step.wantRemaining, step.wantReset)
			}
		})
		if !ok {
			break
		}
	}
}

func TestCountersExpire(t *testing.T) {
	tests := []struct {
		name    string
		window  time.Duration
		wantMax time.Duration
	}{
		{name: "one minute window", window: time.Minute, wantMax: time.Minute + time.Second},
		{name: "one second window", window: time.Second, wantMax: 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			l := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
			if _, err := l.Allow(context.Background(), "user:u1", 10, tt.window); err != nil {
				t.Fatal(err)
			}
			keys := mr.Keys()
			if len(keys) != 1 {
				t.Fatalf("keys = %v, want one counter", keys)
			}
			if ttl := mr.TTL(keys[0]); ttl <= 0 || ttl > tt.wantMax {
				t.Errorf("TTL = %v, want (0, %v]", ttl, tt.wantMax)
			}
		})
	}
}
