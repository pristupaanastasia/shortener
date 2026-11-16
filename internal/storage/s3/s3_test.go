//go:build integration

package s3

import (
	"context"
	"crypto/rand"
	"os"
	"slices"
	"strings"
	"testing"
)

// testConfig points at the docker compose S3 (or ARCHIVE_ENDPOINT). Runs
// share one bucket and use a prefix of their own: in SeaweedFS every bucket
// takes volumes of its own. The prefix also checks that List hides it.
func testConfig() Config {
	endpoint := os.Getenv("ARCHIVE_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:8333"
	}
	return Config{Endpoint: endpoint, Bucket: "shortener-test", Prefix: strings.ToLower(rand.Text())[:12] + "/", AccessKey: "shortener", SecretKey: "shortener"}
}

func TestArchive(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	var a *Archive

	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "open creates the bucket if missing", run: func(t *testing.T) {
			var err error
			if a, err = Open(ctx, cfg); err != nil {
				t.Fatalf("Open (is S3 running? try `make up`): %v", err)
			}
		}},
		{name: "open an existing bucket", run: func(t *testing.T) {
			if _, err := Open(ctx, cfg); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "put and get", run: func(t *testing.T) {
			if err := a.Put(ctx, "expired/2026-11-16/001/a.jsonl", []byte(`{"key":"a"}`+"\n")); err != nil {
				t.Fatal(err)
			}
			got, err := a.Get(ctx, "expired/2026-11-16/001/a.jsonl")
			if err != nil || string(got) != `{"key":"a"}`+"\n" {
				t.Errorf("Get = %q, %v", got, err)
			}
		}},
		{name: "list by prefix", run: func(t *testing.T) {
			if err := a.Put(ctx, "expired/2026-11-17/000/b.jsonl", []byte("{}\n")); err != nil {
				t.Fatal(err)
			}
			got, err := a.List(ctx, "expired/2026-11-16/")
			if err != nil || !slices.Equal(got, []string{"expired/2026-11-16/001/a.jsonl"}) {
				t.Errorf("List = %v, %v", got, err)
			}
		}},
		{name: "get a missing object", run: func(t *testing.T) {
			if _, err := a.Get(ctx, "nope"); err == nil {
				t.Error("Get of a missing object succeeded")
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			break
		}
	}
}
