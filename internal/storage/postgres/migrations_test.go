//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
)

// TestMigrations applies every migration to an empty schema and checks that
// a second run is a no-op that keeps the data.
func TestMigrations(t *testing.T) {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, testDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	var b [4]byte
	_, _ = rand.Read(b[:])
	schema := "migtest_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	dsn := testDSN() + "&search_path=" + schema

	all, err := migrate.Load(migrations.Postgres, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	var wantVersions []int
	for _, m := range all {
		wantVersions = append(wantVersions, m.Version)
	}
	user := account.User{ID: "33333333-3333-4333-8333-333333333333", Email: "m@example.com", CreatedAt: time.Now().UTC().Truncate(time.Millisecond)}
	key := account.HashKey("sk_migration_test")

	steps := []struct {
		name   string
		action func(t *testing.T, a *Accounts)
	}{
		{name: "fresh schema", action: func(t *testing.T, a *Accounts) {
			tables := tablesIn(t, admin, schema)
			for _, want := range []string{"users", "api_keys", "schema_migrations"} {
				if !slices.Contains(tables, want) {
					t.Errorf("table %s missing; tables = %v", want, tables)
				}
			}
			if err := a.CreateUser(ctx, user, key); err != nil {
				t.Fatalf("the migrated schema rejects a user: %v", err)
			}
		}},
		{name: "second run is a no-op and keeps data", action: func(t *testing.T, a *Accounts) {
			if got, err := a.UserByKeyHash(ctx, key); err != nil || got.ID != user.ID {
				t.Errorf("user after re-running migrations = %+v, %v", got, err)
			}
		}},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			a, err := Open(ctx, dsn)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer a.Close()
			if got := appliedVersions(t, admin, schema); !slices.Equal(got, wantVersions) {
				t.Errorf("schema_migrations = %v, want %v", got, wantVersions)
			}
			step.action(t, a)
		})
		if !ok {
			break
		}
	}
}

func testDSN() string {
	return "postgres://shortener:shortener@127.0.0.1:5432/shortener?sslmode=disable"
}

func tablesIn(t *testing.T, pool *pgxpool.Pool, schema string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT table_name FROM information_schema.tables WHERE table_schema = $1`, schema)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func appliedVersions(t *testing.T, pool *pgxpool.Pool, schema string) []int {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT version FROM `+schema+`.schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatal(err)
	}
	return versions
}
