// Package cassandra is a link.Store backed by Apache Cassandra.
// The schema follows "Data Model" in README.md.
package cassandra

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/link"
)

// Config configures the connection and schema.
type Config struct {
	Hosts             []string
	Keyspace          string
	ReplicationFactor int
}

var keyspacePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`)

// Store implements link.Store. Reads and writes use QUORUM, so with
// RF=3 a read always sees the latest acknowledged write (W + R > N).
type Store struct {
	session *gocql.Session

	insertLink       string
	insertLinkByUser string
	selectLink       string
}

// Open connects to Cassandra, creates the keyspace and tables if they do not
// exist, and returns a Store.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if !keyspacePattern.MatchString(cfg.Keyspace) {
		return nil, fmt.Errorf("invalid keyspace name %q", cfg.Keyspace)
	}
	if cfg.ReplicationFactor < 1 {
		return nil, fmt.Errorf("replication factor must be at least 1, got %d", cfg.ReplicationFactor)
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.SerialConsistency = gocql.Serial
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect to cassandra %v: %w", cfg.Hosts, err)
	}

	if err := migrate(ctx, session, cfg.Keyspace, cfg.ReplicationFactor); err != nil {
		session.Close()
		return nil, err
	}

	ks := cfg.Keyspace
	return &Store{
		session: session,
		insertLink: fmt.Sprintf(`INSERT INTO %s.links (key, long_url, user_id, created_at, expires_at, deleted_at)
			VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`, ks),
		insertLinkByUser: fmt.Sprintf(`INSERT INTO %s.links_by_user (user_id, created_at, key, long_url, expires_at)
			VALUES (?, ?, ?, ?, ?)`, ks),
		selectLink: fmt.Sprintf(`SELECT long_url, user_id, created_at, expires_at, deleted_at
			FROM %s.links WHERE key = ?`, ks),
	}, nil
}

// Close closes the session.
func (s *Store) Close() {
	s.session.Close()
}

func migrate(ctx context.Context, session *gocql.Session, ks string, rf int) error {
	statements := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
			WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`, ks, rf),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.links (
			key         text PRIMARY KEY,
			long_url    text,
			user_id     uuid,
			created_at  timestamp,
			expires_at  timestamp,
			deleted_at  timestamp
		)`, ks),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.links_by_user (
			user_id     uuid,
			created_at  timestamp,
			key         text,
			long_url    text,
			expires_at  timestamp,
			PRIMARY KEY ((user_id), created_at, key)
		) WITH CLUSTERING ORDER BY (created_at DESC, key ASC)`, ks),
	}
	for _, stmt := range statements {
		if err := session.Query(stmt).ExecContext(ctx); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Create implements link.Store.
//
// The insert into links is a lightweight transaction, which guarantees key
// uniqueness. Cassandra does not allow a conditional batch to span tables,
// so links_by_user is written afterwards with a plain, idempotent insert.
func (s *Store) Create(ctx context.Context, l link.Link) error {
	userID, err := parseUserID(l.UserID)
	if err != nil {
		return err
	}

	applied, err := s.session.Query(s.insertLink,
		l.Key, l.LongURL, userID, l.CreatedAt, l.ExpiresAt, l.DeletedAt,
	).MapScanCASContext(ctx, map[string]any{})
	if err != nil {
		return fmt.Errorf("insert link %q: %w", l.Key, err)
	}
	if !applied {
		return link.ErrKeyTaken
	}

	if userID == nil {
		return nil
	}
	err = s.session.Query(s.insertLinkByUser,
		userID, l.CreatedAt, l.Key, l.LongURL, l.ExpiresAt,
	).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("insert link %q into links_by_user: %w", l.Key, err)
	}
	return nil
}

// Get implements link.Store.
func (s *Store) Get(ctx context.Context, key string) (link.Link, error) {
	l := link.Link{Key: key}
	var userID *gocql.UUID
	err := s.session.Query(s.selectLink, key).ScanContext(ctx,
		&l.LongURL, &userID, &l.CreatedAt, &l.ExpiresAt, &l.DeletedAt,
	)
	if errors.Is(err, gocql.ErrNotFound) {
		return link.Link{}, link.ErrNotFound
	}
	if err != nil {
		return link.Link{}, fmt.Errorf("select link %q: %w", key, err)
	}
	if userID != nil {
		l.UserID = userID.String()
	}
	return l, nil
}

// parseUserID returns nil for anonymous links (empty id).
func parseUserID(id string) (*gocql.UUID, error) {
	if id == "" {
		return nil, nil
	}
	u, err := gocql.ParseUUID(id)
	if err != nil {
		return nil, fmt.Errorf("invalid user id %q: %w", id, err)
	}
	return &u, nil
}
