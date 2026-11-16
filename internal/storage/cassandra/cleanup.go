package cassandra

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/pristupaanastasia/shortener/internal/cleanup"
	"github.com/pristupaanastasia/shortener/internal/link"
)

// expiryBuckets splits each day of links_by_expiry into partitions of
// ~20 MB at 100M links/day.
const expiryBuckets = 256

// ExpiryBuckets implements cleanup.Store.
func (s *Store) ExpiryBuckets() int { return expiryBuckets }

// ExpiryBucket returns the bucket of key in links_by_expiry, which is also
// part of the archive object names.
func ExpiryBucket(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % expiryBuckets)
}

func (s *Store) indexExpiry(ctx context.Context, key string, expiresAt time.Time) error {
	return retry(ctx, func() error {
		err := s.session.Query(s.insertExpiry, cleanup.Day(expiresAt), ExpiryBucket(key), expiresAt, key).ExecContext(ctx)
		if err != nil {
			return fmt.Errorf("insert link %q into links_by_expiry: %w", key, err)
		}
		return nil
	})
}

// Expired implements cleanup.Store. The cursor is a slice on the clustering
// key, so a read never scans the rows before it.
func (s *Store) Expired(ctx context.Context, day time.Time, bucket int, before time.Time, after *cleanup.Entry, limit int) ([]cleanup.Entry, error) {
	q := s.session.Query(fmt.Sprintf(`SELECT expires_at, key FROM %s.links_by_expiry
		WHERE day = ? AND bucket = ? AND expires_at < ? LIMIT ?`, s.ks), day, bucket, before, limit)
	if after != nil {
		q = s.session.Query(fmt.Sprintf(`SELECT expires_at, key FROM %s.links_by_expiry
			WHERE day = ? AND bucket = ? AND (expires_at, key) > (?, ?) AND expires_at < ? LIMIT ?`, s.ks),
			day, bucket, after.ExpiresAt, after.Key, before, limit)
	}
	iter := q.IterContext(ctx)
	var entries []cleanup.Entry
	var e cleanup.Entry
	for iter.Scan(&e.ExpiresAt, &e.Key) {
		entries = append(entries, e)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select expired links of %s/%d: %w", day.Format(time.DateOnly), bucket, err)
	}
	return entries, nil
}

// DeleteExpiryPartition implements cleanup.Store.
func (s *Store) DeleteExpiryPartition(ctx context.Context, day time.Time, bucket int) error {
	err := s.session.Query(fmt.Sprintf(`DELETE FROM %s.links_by_expiry WHERE day = ? AND bucket = ?`, s.ks),
		day, bucket).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("delete links_by_expiry partition %s/%d: %w", day.Format(time.DateOnly), bucket, err)
	}
	return nil
}

// Tombstone implements cleanup.Store. The conditions on expires_at and
// long_url make it lose to a concurrent PATCH: the owner wins over the
// worker, and a URL changed after archiving is never nulled unarchived.
func (s *Store) Tombstone(ctx context.Context, l link.Link, at time.Time) (cleanup.TombstoneResult, error) {
	current := map[string]any{}
	applied, err := s.session.Query(fmt.Sprintf(`UPDATE %s.links SET long_url = null, deleted_at = ?
		WHERE key = ? IF expires_at = ? AND long_url = ? AND deleted_at = null`, s.ks),
		at, l.Key, l.ExpiresAt, l.LongURL).MapScanCASContext(ctx, current)
	if err != nil {
		return 0, fmt.Errorf("tombstone link %q: %w", l.Key, err)
	}
	res := cleanup.Tombstoned
	if !applied {
		if deletedAt, _ := current["deleted_at"].(time.Time); deletedAt.IsZero() {
			return cleanup.Changed, nil
		}
		res = cleanup.AlreadyDeleted
	}
	userID, err := parseUserID(l.UserID)
	if err != nil || userID == nil {
		return res, err
	}
	if err := s.session.Query(s.deleteLinkByUser, userID, l.CreatedAt, l.Key).ExecContext(ctx); err != nil {
		return res, fmt.Errorf("delete link %q from links_by_user: %w", l.Key, err)
	}
	return res, nil
}

// Checkpoint implements cleanup.Store.
func (s *Store) Checkpoint(ctx context.Context, name string) (time.Time, bool, error) {
	var day time.Time
	err := s.session.Query(fmt.Sprintf(`SELECT day FROM %s.cleanup_checkpoint WHERE name = ?`, s.ks), name).ScanContext(ctx, &day)
	if errors.Is(err, gocql.ErrNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read cleanup checkpoint %q: %w", name, err)
	}
	return day.UTC(), true, nil
}

// SetCheckpoint implements cleanup.Store.
func (s *Store) SetCheckpoint(ctx context.Context, name string, day time.Time) error {
	err := s.session.Query(fmt.Sprintf(`INSERT INTO %s.cleanup_checkpoint (name, day) VALUES (?, ?)`, s.ks), name, day).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("write cleanup checkpoint %q: %w", name, err)
	}
	return nil
}

// AcquireLease takes the named lease for ttl unless another owner holds it.
// Holding it again is allowed, so a restarted worker with the same owner
// gets it back.
func (s *Store) AcquireLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	current := map[string]any{}
	applied, err := s.session.Query(fmt.Sprintf(`INSERT INTO %s.cleanup_lease (name, owner) VALUES (?, ?)
		IF NOT EXISTS USING TTL ?`, s.ks), name, owner, int(ttl.Seconds())).MapScanCASContext(ctx, current)
	if err != nil {
		return false, fmt.Errorf("acquire lease %q: %w", name, err)
	}
	if applied {
		return true, nil
	}
	if current["owner"] == owner {
		return s.RenewLease(ctx, name, owner, ttl)
	}
	return false, nil
}

// RenewLease extends a lease the owner holds; false means it was lost (it
// expired, and possibly someone else took it).
func (s *Store) RenewLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	applied, err := s.session.Query(fmt.Sprintf(`UPDATE %s.cleanup_lease USING TTL ? SET owner = ?
		WHERE name = ? IF owner = ?`, s.ks), int(ttl.Seconds()), owner, name, owner).MapScanCASContext(ctx, map[string]any{})
	if err != nil {
		return false, fmt.Errorf("renew lease %q: %w", name, err)
	}
	return applied, nil
}

// ReleaseLease gives the lease up if the owner still holds it.
func (s *Store) ReleaseLease(ctx context.Context, name, owner string) error {
	_, err := s.session.Query(fmt.Sprintf(`DELETE FROM %s.cleanup_lease WHERE name = ? IF owner = ?`, s.ks),
		name, owner).MapScanCASContext(ctx, map[string]any{})
	if err != nil {
		return fmt.Errorf("release lease %q: %w", name, err)
	}
	return nil
}
