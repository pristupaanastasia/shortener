package keygen

import (
	"strings"
	"testing"
	"time"
)

func TestToBase62(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		want string
	}{
		{name: "zero is padded", n: 0, want: "0000000"},
		{name: "last single digit", n: 61, want: "000000z"},
		{name: "first two digits", n: 62, want: "0000010"},
		{name: "largest key", n: keySpace - 1, want: "zzzzzzz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toBase62(tt.n, Length); got != tt.want {
				t.Errorf("toBase62(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

func TestGenerate(t *testing.T) {
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	const url = "https://example.com/some/long/path"
	base := Generate(url, now, 0)

	tests := []struct {
		name     string
		url      string
		now      time.Time
		attempt  int
		wantSame bool // whether the key must equal base
	}{
		{name: "same input gives the same key", url: url, now: now, attempt: 0, wantSame: true},
		{name: "a retry gives a new key", url: url, now: now, attempt: 1, wantSame: false},
		{name: "a later call gives a new key", url: url, now: now.Add(time.Nanosecond), attempt: 0, wantSame: false},
		{name: "another url gives a new key", url: url + "/x", now: now, attempt: 0, wantSame: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := Generate(tt.url, tt.now, tt.attempt)
			if len(key) != Length {
				t.Fatalf("len(%q) = %d, want %d", key, len(key), Length)
			}
			for _, c := range key {
				if !strings.ContainsRune(alphabet, c) {
					t.Fatalf("key %q contains non-base62 character %q", key, c)
				}
			}
			if same := key == base; same != tt.wantSame {
				t.Errorf("Generate(%q, %v, %d) = %q, base %q: same = %v, want %v",
					tt.url, tt.now, tt.attempt, key, base, same, tt.wantSame)
			}
		})
	}
}
