package keygen

import (
	"strings"
	"testing"
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
	const url = "https://example.com/some/long/path"
	salt := []byte("0123456789abcdef")
	base := Generate(url, salt)

	tests := []struct {
		name     string
		url      string
		salt     []byte
		wantSame bool // whether the key must equal base
	}{
		{name: "same input gives the same key", url: url, salt: salt, wantSame: true},
		{name: "another salt gives a new key", url: url, salt: []byte("fedcba9876543210"), wantSame: false},
		{name: "another url gives a new key", url: url + "/x", salt: salt, wantSame: false},
		{name: "fresh random salt gives a new key", url: url, salt: NewSalt(), wantSame: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := Generate(tt.url, tt.salt)
			if len(key) != Length {
				t.Fatalf("len(%q) = %d, want %d", key, len(key), Length)
			}
			for _, c := range key {
				if !strings.ContainsRune(alphabet, c) {
					t.Fatalf("key %q contains non-base62 character %q", key, c)
				}
			}
			if same := key == base; same != tt.wantSame {
				t.Errorf("Generate(%q, %x) = %q, base %q: same = %v, want %v", tt.url, tt.salt, key, base, same, tt.wantSame)
			}
		})
	}
}

func TestNewSalt(t *testing.T) {
	tests := []struct {
		name  string
		calls int
	}{
		{name: "one salt", calls: 1},
		{name: "1000 salts are distinct", calls: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := map[string]bool{}
			for range tt.calls {
				s := NewSalt()
				if len(s) != SaltSize || seen[string(s)] {
					t.Fatalf("salt %x: wrong size or repeated", s)
				}
				seen[string(s)] = true
			}
		})
	}
}
