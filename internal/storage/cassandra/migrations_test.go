//go:build integration

package cassandra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
)

// TestMigrations applies every migration to an empty keyspace and checks that
// a second run is a no-op that keeps the data.
func TestMigrations(t *testing.T) {
	ctx := context.Background()
	hosts := os.Getenv("CASSANDRA_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	ks := "migtest_" + hex.EncodeToString(b[:])
	cfg := Config{Hosts: strings.Split(hosts, ","), Keyspace: ks, ReplicationFactor: 1}

	admin, err := gocql.NewCluster(cfg.Hosts...).CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Query("DROP KEYSPACE IF EXISTS " + ks).Exec()
		admin.Close()
	})

	all, err := migrate.Load(migrations.Cassandra, "cassandra")
	if err != nil {
		t.Fatal(err)
	}
	var wantVersions []int
	for _, m := range all {
		wantVersions = append(wantVersions, m.Version)
	}
	l := link.Link{Key: "migtest", LongURL: "https://example.com", CreatedAt: time.Now().UTC().Truncate(time.Millisecond)}

	steps := []struct {
		name   string
		action func(t *testing.T, s *Store)
	}{
		{name: "fresh keyspace", action: func(t *testing.T, s *Store) {
			tables := tablesIn(t, admin, ks)
			for _, want := range []string{"links", "links_by_user", "schema_migrations"} {
				if !slices.Contains(tables, want) {
					t.Errorf("table %s missing; tables = %v", want, tables)
				}
			}
			if err := s.Create(ctx, l); err != nil {
				t.Fatalf("the migrated schema rejects a link: %v", err)
			}
		}},
		{name: "second run is a no-op and keeps data", action: func(t *testing.T, s *Store) {
			if got, err := s.Get(ctx, l.Key); err != nil || got.LongURL != l.LongURL {
				t.Errorf("link after re-running migrations = %+v, %v", got, err)
			}
		}},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			s, err := Open(ctx, cfg)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer s.Close()
			if got := appliedVersions(t, admin, ks); !slices.Equal(got, wantVersions) {
				t.Errorf("schema_migrations = %v, want %v", got, wantVersions)
			}
			step.action(t, s)
		})
		if !ok {
			break
		}
	}
}

func tablesIn(t *testing.T, session *gocql.Session, ks string) []string {
	t.Helper()
	iter := session.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`, ks).Iter()
	var names []string
	var name string
	for iter.Scan(&name) {
		names = append(names, name)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	return names
}

func appliedVersions(t *testing.T, session *gocql.Session, ks string) []int {
	t.Helper()
	iter := session.Query(`SELECT version FROM ` + ks + `.schema_migrations`).Iter()
	var versions []int
	var v int
	for iter.Scan(&v) {
		versions = append(versions, v)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(versions)
	return versions
}
