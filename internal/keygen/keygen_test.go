package keygen

import (
	"strings"
	"testing"
	"time"
)

func TestToBase62(t *testing.T) {
	tests := []struct {
		n    uint64
		want string
	}{
		{0, "0000000"},
		{61, "000000z"},
		{62, "0000010"},
		{keySpace - 1, "zzzzzzz"},
	}
	for _, tt := range tests {
		if got := toBase62(tt.n, Length); got != tt.want {
			t.Errorf("toBase62(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestGenerate(t *testing.T) {
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	const url = "https://example.com/some/long/path"

	key := Generate(url, now, 0)
	if len(key) != Length {
		t.Fatalf("len(%q) = %d, want %d", key, len(key), Length)
	}
	for _, c := range key {
		if !strings.ContainsRune(alphabet, c) {
			t.Fatalf("key %q contains non-base62 character %q", key, c)
		}
	}

	if again := Generate(url, now, 0); again != key {
		t.Errorf("same input gave different keys: %q and %q", key, again)
	}
	if retry := Generate(url, now, 1); retry == key {
		t.Errorf("a new attempt gave the same key %q", key)
	}
	if later := Generate(url, now.Add(time.Nanosecond), 0); later == key {
		t.Errorf("a different timestamp gave the same key %q", key)
	}
}
