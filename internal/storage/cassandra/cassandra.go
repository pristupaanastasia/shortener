// Package cassandra is a link.Store backed by Apache Cassandra.
// The schema follows "Data Model" in README.md.
package cassandra

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
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
	deleteLink       string
	deleteLinkByUser string
	listLinksByUser  string
	ks               string
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

	// The keyspace depends on the deployment (replication factor), so it is
	// created here; everything inside it comes from migrations.
	if err := createKeyspace(ctx, cluster, cfg.Keyspace, cfg.ReplicationFactor); err != nil {
		return nil, err
	}
	cluster.Keyspace = cfg.Keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect to cassandra %v: %w", cfg.Hosts, err)
	}
	if err := applyMigrations(ctx, session); err != nil {
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
		// created_at never changes, so "created_at = ?" doubles as "the row
		// exists": a condition on a missing row's columns would compare nulls.
		deleteLink: fmt.Sprintf(`UPDATE %s.links SET deleted_at = ?
			WHERE key = ? IF created_at = ? AND deleted_at = null`, ks),
		deleteLinkByUser: fmt.Sprintf(`DELETE FROM %s.links_by_user
			WHERE user_id = ? AND created_at = ? AND key = ?`, ks),
		listLinksByUser: fmt.Sprintf(`SELECT key, long_url, created_at, expires_at
			FROM %s.links_by_user WHERE user_id = ?`, ks),
		ks: ks,
	}, nil
}

// Close closes the session.
func (s *Store) Close() {
	s.session.Close()
}

func createKeyspace(ctx context.Context, cluster *gocql.ClusterConfig, ks string, rf int) error {
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect to cassandra %v: %w", cluster.Hosts, err)
	}
	defer session.Close()
	err = session.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`, ks, rf)).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("create keyspace %s: %w", ks, err)
	}
	return nil
}

// applyMigrations runs the migrations from migrations/cassandra that are not
// recorded in schema_migrations. Cassandra has no transactional DDL, so every
// statement must be idempotent (IF NOT EXISTS): a migration interrupted after
// some statements is safely re-run from the start.
func applyMigrations(ctx context.Context, session *gocql.Session) error {
	all, err := migrate.Load(migrations.Cassandra, "cassandra")
	if err != nil {
		return err
	}
	err = session.Query(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version int PRIMARY KEY, name text, applied_at timestamp)`).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[int]bool{}
	iter := session.Query(`SELECT version FROM schema_migrations`).IterContext(ctx)
	var v int
	for iter.Scan(&v) {
		applied[v] = true
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	for _, m := range migrate.Pending(all, applied) {
		for _, stmt := range m.Statements {
			if err := session.Query(stmt).ExecContext(ctx); err != nil {
				return fmt.Errorf("migration %s: %w", m.Name, err)
			}
		}
		err := session.Query(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
			m.Version, m.Name, time.Now()).ExecContext(ctx)
		if err != nil {
			return fmt.Errorf("record migration %s: %w", m.Name, err)
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
	// The link is stored at this point; a failure here would leave it out of
	// its owner's listing, and a retry of the request could not repair it
	// (the key is now taken). The insert is idempotent, so retry it.
	for attempt := 1; ; attempt++ {
		err = s.session.Query(s.insertLinkByUser,
			userID, l.CreatedAt, l.Key, l.LongURL, l.ExpiresAt,
		).ExecContext(ctx)
		if err == nil {
			return nil
		}
		if attempt == listingWriteAttempts || ctx.Err() != nil {
			return fmt.Errorf("insert link %q into links_by_user: %w", l.Key, err)
		}
		time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
	}
}

// listingWriteAttempts bounds retries of the links_by_user insert in Create.
const listingWriteAttempts = 3

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

// Update implements link.Store. Only the selected columns are written, with
// a lightweight transaction that requires the link to exist (created_at
// matches) and not be deleted, so an update based on a stale read cannot
// undelete a link or revert a concurrent change of another column.
func (s *Store) Update(ctx context.Context, l link.Link, f link.Fields) error {
	var set []string
	var args []any
	if f.LongURL {
		set, args = append(set, "long_url = ?"), append(args, l.LongURL)
	}
	if f.ExpiresAt {
		set, args = append(set, "expires_at = ?"), append(args, l.ExpiresAt)
	}
	if len(set) == 0 {
		return nil
	}
	assignments := strings.Join(set, ", ")

	stmt := fmt.Sprintf(`UPDATE %s.links SET %s WHERE key = ? IF created_at = ? AND deleted_at = null`, s.ks, assignments)
	applied, err := s.session.Query(stmt, append(args, l.Key, l.CreatedAt)...).MapScanCASContext(ctx, map[string]any{})
	if err != nil {
		return fmt.Errorf("update link %q: %w", l.Key, err)
	}
	if !applied {
		return link.ErrNotFound // missing or deleted
	}

	userID, err := parseUserID(l.UserID)
	if err != nil || userID == nil {
		return err
	}
	stmt = fmt.Sprintf(`UPDATE %s.links_by_user SET %s WHERE user_id = ? AND created_at = ? AND key = ?`, s.ks, assignments)
	if err := s.session.Query(stmt, append(args, userID, l.CreatedAt, l.Key)...).ExecContext(ctx); err != nil {
		return fmt.Errorf("update link %q in links_by_user: %w", l.Key, err)
	}
	return nil
}

// Delete implements link.Store. The links row keeps its first deletion time;
// the links_by_user row is deleted on every call, so a retry repairs a listing
// left behind when an earlier call failed between the two writes.
func (s *Store) Delete(ctx context.Context, l link.Link) error {
	current := map[string]any{}
	applied, err := s.session.Query(s.deleteLink, l.DeletedAt, l.Key, l.CreatedAt).MapScanCASContext(ctx, current)
	if err != nil {
		return fmt.Errorf("delete link %q: %w", l.Key, err)
	}
	if !applied {
		// Not applied: either the link does not exist, or it is already
		// deleted (which is fine). The returned row tells them apart.
		if createdAt, _ := current["created_at"].(time.Time); createdAt.IsZero() {
			return link.ErrNotFound
		}
	}

	userID, err := parseUserID(l.UserID)
	if err != nil || userID == nil {
		return err
	}
	if err := s.session.Query(s.deleteLinkByUser, userID, l.CreatedAt, l.Key).ExecContext(ctx); err != nil {
		return fmt.Errorf("delete link %q from links_by_user: %w", l.Key, err)
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
		// The cursor comes from the client: a paging state Cassandra cannot
		// parse is a bad request, not a server error.
		if cursor != "" && isRequestError(err, gocql.ErrCodeProtocol, gocql.ErrCodeInvalid) {
			return nil, "", link.ErrInvalidCursor
		}
		return nil, "", fmt.Errorf("list links of user %q: %w", userID, err)
	}

	// Cassandra may return a paging state even when the next page is empty;
	// a short page means there is nothing more.
	if len(links) < limit || len(next) == 0 {
		return links, "", nil
	}
	return links, base64.RawURLEncoding.EncodeToString(next), nil
}

func isRequestError(err error, codes ...int) bool {
	reqErr, ok := errors.AsType[gocql.RequestError](err)
	return ok && slices.Contains(codes, reqErr.Code())
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
