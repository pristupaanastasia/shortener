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
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		res, err := l.Allow(ctx, "ip:1.2.3.4", 3, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Allowed || res.Remaining != 3-i {
			t.Errorf("request %d: %+v, want allowed with %d remaining", i, res, 3-i)
		}
		if want := time.Date(2026, 11, 12, 10, 1, 0, 0, time.UTC); !res.Reset.Equal(want) {
			t.Errorf("Reset = %v, want %v", res.Reset, want)
		}
	}

	res, _ := l.Allow(ctx, "ip:1.2.3.4", 3, time.Minute)
	if res.Allowed || res.Remaining != 0 {
		t.Errorf("4th request: %+v, want denied", res)
	}

	if res, _ := l.Allow(ctx, "ip:5.6.7.8", 3, time.Minute); !res.Allowed {
		t.Error("another subject was limited")
	}

	now = now.Add(time.Minute)
	if res, _ := l.Allow(ctx, "ip:1.2.3.4", 3, time.Minute); !res.Allowed || res.Remaining != 2 {
		t.Errorf("next window: %+v, want a fresh quota", res)
	}
}

func TestCountersExpire(t *testing.T) {
	mr := miniredis.RunT(t)
	l := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	if _, err := l.Allow(context.Background(), "user:u1", 10, time.Minute); err != nil {
		t.Fatal(err)
	}
	keys := mr.Keys()
	if len(keys) != 1 {
		t.Fatalf("keys = %v", keys)
	}
	if ttl := mr.TTL(keys[0]); ttl <= 0 || ttl > time.Minute+time.Second {
		t.Errorf("TTL = %v, want (0, 61s]", ttl)
	}
}
