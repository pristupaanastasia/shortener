// Package keygen generates short link keys: a salted SHA-256 hash encoded
// as a fixed-length base62 string. See "Deep Dive: Key Generation" in README.md.
package keygen

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

// Length is the number of characters in a generated key.
const Length = 7

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// keySpace is 62^Length: the number of distinct keys.
const keySpace uint64 = 3_521_614_606_208

// Generate returns a key for longURL. The timestamp and attempt number act as
// a salt, so the same URL gets a different key on every call, and a retry
// after a collision produces a new candidate.
func Generate(longURL string, now time.Time, attempt int) string {
	input := fmt.Sprintf("%s|%d|%d", longURL, now.UnixNano(), attempt)
	sum := sha256.Sum256([]byte(input))
	n := binary.BigEndian.Uint64(sum[:8]) % keySpace
	return toBase62(n, Length)
}

// toBase62 encodes n as a base62 string left-padded with '0' to width.
func toBase62(n uint64, width int) string {
	buf := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		buf[i] = alphabet[n%62]
		n /= 62
	}
	return string(buf)
}
