// Package keygen generates short link keys: a salted SHA-256 hash encoded
// as a fixed-length base62 string. See "Deep Dive: Key Generation" in README.md.
package keygen

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
)

// Length is the number of characters in a generated key.
const Length = 7

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// keySpace is 62^Length: the number of distinct keys.
const keySpace uint64 = 3_521_614_606_208

// SaltSize is the number of random bytes mixed into every key.
const SaltSize = 16

// NewSalt returns random bytes for Generate.
func NewSalt() []byte {
	salt := make([]byte, SaltSize)
	_, _ = rand.Read(salt) // never fails on supported platforms
	return salt
}

// Generate returns a key for longURL and salt. A fresh random salt per call
// makes keys unpredictable and gives the same URL a different key every time,
// including concurrent requests: a timestamp salt would repeat within one
// clock tick.
func Generate(longURL string, salt []byte) string {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(longURL))
	sum := h.Sum(nil)
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
