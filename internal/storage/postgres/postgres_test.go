//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/pristupaanastasia/shortener/internal/account"
)

func openTestAccounts(t *testing.T) *Accounts {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://shortener:shortener@127.0.0.1:5432/shortener?sslmode=disable"
	}
	a, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open (is PostgreSQL running? try `make up`): %v", err)
	}
	t.Cleanup(a.Close)
	return a
}

func TestAccounts(t *testing.T) {
	a := openTestAccounts(t)
	ctx := context.Background()
	// Unique per run, since rows from earlier runs stay in the database.
	var b [6]byte
	_, _ = rand.Read(b[:])
	run := hex.EncodeToString(b[:]) // 12 hex digits: the last UUID group
	alice := account.User{
		ID:        "11111111-1111-4111-8111-" + run,
		Email:     "alice-" + run + "@example.com",
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	aliceKey := account.HashKey("sk_alice_" + run)

	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "create user", run: func(t *testing.T) {
			if err := a.CreateUser(ctx, alice, aliceKey); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "find user by key hash", run: func(t *testing.T) {
			got, err := a.UserByKeyHash(ctx, aliceKey)
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != alice.ID || got.Email != alice.Email || !got.CreatedAt.Equal(alice.CreatedAt) {
				t.Errorf("UserByKeyHash = %+v, want %+v", got, alice)
			}
		}},
		{name: "unknown key hash", run: func(t *testing.T) {
			if _, err := a.UserByKeyHash(ctx, account.HashKey("sk_unknown")); !errors.Is(err, account.ErrNotFound) {
				t.Errorf("err = %v, want ErrNotFound", err)
			}
		}},
		{name: "duplicate email", run: func(t *testing.T) {
			dup := alice
			dup.ID = "22222222-2222-4222-8222-" + run
			err := a.CreateUser(ctx, dup, account.HashKey("sk_dup_"+run))
			if !errors.Is(err, account.ErrEmailTaken) {
				t.Fatalf("err = %v, want ErrEmailTaken", err)
			}
		}},
		{name: "failed create leaves no orphan key", run: func(t *testing.T) {
			if _, err := a.UserByKeyHash(ctx, account.HashKey("sk_dup_"+run)); !errors.Is(err, account.ErrNotFound) {
				t.Errorf("key of the rejected user exists: err = %v", err)
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			break
		}
	}
}
