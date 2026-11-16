// Command archived reports whether a link is in the cleanup worker's archive:
// it exits 0 if a record with the key is found under ARCHIVE_PREFIX, 1 if
// not. Only the objects of the key's day and bucket are read, so the check
// stays fast however much the archive holds. The smoke test uses it, since
// S3 requests must be signed.
//
//	go run ./scripts/archived KEY EXPIRES_AT(RFC 3339)
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pristupaanastasia/shortener/internal/cleanup"
	"github.com/pristupaanastasia/shortener/internal/storage/cassandra"
	"github.com/pristupaanastasia/shortener/internal/storage/s3"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: archived KEY EXPIRES_AT")
		os.Exit(2)
	}
	expiresAt, err := time.Parse(time.RFC3339, os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "archived:", err)
		os.Exit(2)
	}
	found, err := run(os.Args[1], expiresAt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "archived:", err)
		os.Exit(2)
	}
	if !found {
		os.Exit(1)
	}
}

func run(key string, expiresAt time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, err := s3.Open(ctx, s3.Config{
		Endpoint:  getenv("ARCHIVE_ENDPOINT", "127.0.0.1:8333"),
		Bucket:    getenv("ARCHIVE_BUCKET", "shortener-archive"),
		Prefix:    os.Getenv("ARCHIVE_PREFIX"),
		AccessKey: getenv("ARCHIVE_ACCESS_KEY", "shortener"),
		SecretKey: getenv("ARCHIVE_SECRET_KEY", "shortener"),
	})
	if err != nil {
		return false, err
	}
	prefix := fmt.Sprintf("expired/%s/%03d/", cleanup.Day(expiresAt).Format(time.DateOnly), cassandra.ExpiryBucket(key))
	names, err := a.List(ctx, prefix)
	if err != nil {
		return false, err
	}
	for _, name := range names {
		data, err := a.Get(ctx, name)
		if err != nil {
			return false, err
		}
		if strings.Contains(string(data), `"key":"`+key+`"`) {
			return true, nil
		}
	}
	return false, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
