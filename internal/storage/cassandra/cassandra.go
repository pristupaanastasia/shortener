// Package cassandra is a link.Store backed by Apache Cassandra.
// The schema follows "Data Model" in README.md.
package cassandra

import (
	"context"
	"encoding/base64"
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
	updateLink       string
	updateLinkByUser string
	deleteLinkByUser string
	listLinksByUser  string
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
		updateLink: fmt.Sprintf(`UPDATE %s.links SET long_url = ?, expires_at = ?, deleted_at = ?
			WHERE key = ? IF EXISTS`, ks),
		updateLinkByUser: fmt.Sprintf(`UPDATE %s.links_by_user SET long_url = ?, expires_at = ?
			WHERE user_id = ? AND created_at = ? AND key = ?`, ks),
		deleteLinkByUser: fmt.Sprintf(`DELETE FROM %s.links_by_user
			WHERE user_id = ? AND created_at = ? AND key = ?`, ks),
		listLinksByUser: fmt.Sprintf(`SELECT key, long_url, created_at, expires_at
			FROM %s.links_by_user WHERE user_id = ?`, ks),
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

// Update implements link.Store. The links row is updated with a lightweight
// transaction (IF EXISTS), so an update never creates a row; links_by_user is
// then brought in line: updated, or removed for a deleted link.
func (s *Store) Update(ctx context.Context, l link.Link) error {
	applied, err := s.session.Query(s.updateLink,
		l.LongURL, l.ExpiresAt, l.DeletedAt, l.Key,
	).MapScanCASContext(ctx, map[string]any{})
	if err != nil {
		return fmt.Errorf("update link %q: %w", l.Key, err)
	}
	if !applied {
		return link.ErrNotFound
	}

	userID, err := parseUserID(l.UserID)
	if err != nil || userID == nil {
		return err
	}
	q := s.session.Query(s.updateLinkByUser, l.LongURL, l.ExpiresAt, userID, l.CreatedAt, l.Key)
	if l.DeletedAt != nil {
		q = s.session.Query(s.deleteLinkByUser, userID, l.CreatedAt, l.Key)
	}
	if err := q.ExecContext(ctx); err != nil {
		return fmt.Errorf("update link %q in links_by_user: %w", l.Key, err)
	}
	return nil
}

// ListByUser implements link.Store. The cursor is Cassandra's paging state,
// base64-encoded: the database itself tracks where the next page starts.
func (s *Store) ListByUser(ctx context.Context, userID, cursor string, limit int) ([]link.Link, string, error) {
	uid, err := parseUserID(userID)
	if err != nil || uid == nil {
		return nil, "", link.ErrNotFound
	}
	var state []byte
	if cursor != "" {
		if state, err = base64.RawURLEncoding.DecodeString(cursor); err != nil {
			return nil, "", link.ErrInvalidCursor
		}
	}

	iter := s.session.Query(s.listLinksByUser, uid).PageSize(limit).PageState(state).IterContext(ctx)
	var links []link.Link
	for range limit {
		l := link.Link{UserID: userID}
		if !iter.Scan(&l.Key, &l.LongURL, &l.CreatedAt, &l.ExpiresAt) {
			break
		}
		links = append(links, l)
	}
	next := iter.PageState()
	if err := iter.Close(); err != nil {
		return nil, "", fmt.Errorf("list links of user %q: %w", userID, err)
	}

	// Cassandra may return a paging state even when the next page is empty;
	// a short page means there is nothing more.
	if len(links) < limit || len(next) == 0 {
		return links, "", nil
	}
	return links, base64.RawURLEncoding.EncodeToString(next), nil
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
