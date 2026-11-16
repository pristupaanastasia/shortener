// Package cleanup archives expired links and replaces them with tombstones
// (README "Flow 5: Expired links"): the redirect keeps returning 410, and the
// key is never reused, while the link's data moves to cold storage.
//
// The expiry index is a hint and links stay the source of truth: every link
// is re-read before it is archived. Each step can be repeated after a crash:
// the archive is written before the tombstone, so a link may be archived
// twice (records carry the key, so readers keep one), but never tombstoned
// without being archived.
package cleanup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pristupaanastasia/shortener/internal/link"
)

// Entry is a row of the expiry index.
type Entry struct {
	Key       string
	ExpiresAt time.Time
}

// TombstoneResult is what Store.Tombstone did.
type TombstoneResult int

// The results.
const (
	Tombstoned     TombstoneResult = iota // the link is now a tombstone
	AlreadyDeleted                        // deleted before (by its owner or an earlier run)
	Changed                               // expires_at changed since it was read: left as is
)

// Store is the link storage as the worker needs it.
type Store interface {
	Get(ctx context.Context, key string) (link.Link, error)
	// ExpiryBuckets is the number of index partitions per day.
	ExpiryBuckets() int
	// Expired returns up to limit index entries of a day and bucket that
	// expire before the given time, earliest first.
	Expired(ctx context.Context, day time.Time, bucket int, before time.Time, limit int) ([]Entry, error)
	// DeleteExpiry removes an index entry.
	DeleteExpiry(ctx context.Context, day time.Time, bucket int, e Entry) error
	// DeleteExpiryPartition removes all index entries of a day and bucket.
	DeleteExpiryPartition(ctx context.Context, day time.Time, bucket int) error
	// Tombstone replaces the link with a tombstone (no URL, deleted_at set)
	// unless its expires_at differs from l's, and removes it from its
	// owner's listing.
	Tombstone(ctx context.Context, l link.Link, at time.Time) (TombstoneResult, error)
	// Checkpoint returns the first day that is not fully cleaned up yet
	// (ok is false before the first run); SetCheckpoint records it.
	Checkpoint(ctx context.Context, name string) (day time.Time, ok bool, err error)
	SetCheckpoint(ctx context.Context, name string, day time.Time) error
}

// Archive is cold storage for expired links.
type Archive interface {
	// Put stores an object under a new name.
	Put(ctx context.Context, name string, data []byte) error
}

// Record is a line of an archive object (JSON Lines).
type Record struct {
	Key        string     `json:"key"`
	LongURL    string     `json:"long_url"`
	UserID     string     `json:"user_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	ArchivedAt time.Time  `json:"archived_at"`
}

// Config tunes a Worker.
type Config struct {
	// Name identifies the checkpoint (and the lease, see Lease).
	Name string
	// Lookback is how many days before today the first run starts at, when
	// there is no checkpoint yet.
	Lookback int
	// BatchSize is how many links go into one archive object.
	BatchSize int
	// Parallel is how many buckets are cleaned up at once.
	Parallel int
}

// DefaultConfig returns the production settings.
func DefaultConfig() Config {
	return Config{Name: "links", Lookback: 30, BatchSize: 500, Parallel: 16}
}

// Stats counts what a run did.
type Stats struct {
	Archived int // archived and tombstoned
	Skipped  int // index entries whose link was extended, deleted or missing
	Objects  int // archive objects written
}

// Worker runs cleanups.
type Worker struct {
	store   Store
	archive Archive
	cfg     Config
	logger  *slog.Logger
}

// New returns a Worker.
func New(store Store, archive Archive, cfg Config, logger *slog.Logger) *Worker {
	return &Worker{store: store, archive: archive, cfg: cfg, logger: logger}
}

// Run archives every link that expired before now, from the checkpoint up to
// today, and advances the checkpoint past every day that is fully done.
// Today stays open: links keep expiring until midnight.
func (w *Worker) Run(ctx context.Context, now time.Time) (Stats, error) {
	today := Day(now)
	from, ok, err := w.store.Checkpoint(ctx, w.cfg.Name)
	if err != nil {
		return Stats{}, err
	}
	if !ok {
		from = today.AddDate(0, 0, -w.cfg.Lookback)
	}

	var total Stats
	for day := from; !day.After(today); day = day.AddDate(0, 0, 1) {
		s, err := w.runDay(ctx, day, now, day.Before(today))
		total.Archived += s.Archived
		total.Skipped += s.Skipped
		total.Objects += s.Objects
		if err != nil {
			return total, fmt.Errorf("clean up %s: %w", day.Format(time.DateOnly), err)
		}
		if day.Before(today) {
			if err := w.store.SetCheckpoint(ctx, w.cfg.Name, day.AddDate(0, 0, 1)); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// runDay cleans up every bucket of a day. A past day is complete, so its
// partitions are dropped afterwards: nothing can be added to them, since
// links cannot be created or extended into the past.
func (w *Worker) runDay(ctx context.Context, day, now time.Time, past bool) (Stats, error) {
	var mu sync.Mutex
	var total Stats
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(w.cfg.Parallel)
	for bucket := range w.store.ExpiryBuckets() {
		g.Go(func() error {
			s, err := w.runBucket(ctx, day, bucket, now)
			mu.Lock()
			total.Archived += s.Archived
			total.Skipped += s.Skipped
			total.Objects += s.Objects
			mu.Unlock()
			if err != nil || !past {
				return err
			}
			return w.store.DeleteExpiryPartition(ctx, day, bucket)
		})
	}
	return total, g.Wait()
}

// runBucket processes a partition batch by batch. Every entry it reads is
// deleted from the index once handled, so the next read starts after it.
func (w *Worker) runBucket(ctx context.Context, day time.Time, bucket int, now time.Time) (Stats, error) {
	var s Stats
	for {
		entries, err := w.store.Expired(ctx, day, bucket, now, w.cfg.BatchSize)
		if err != nil || len(entries) == 0 {
			return s, err
		}
		if err := w.runBatch(ctx, day, bucket, now, entries, &s); err != nil {
			return s, err
		}
	}
}

func (w *Worker) runBatch(ctx context.Context, day time.Time, bucket int, now time.Time, entries []Entry, s *Stats) error {
	var expired []link.Link
	var expiredEntries, done []Entry
	for _, e := range entries {
		l, err := w.store.Get(ctx, e.Key)
		switch {
		case errors.Is(err, link.ErrNotFound):
			done = append(done, e)
		case err != nil:
			return err
		case l.DeletedAt != nil:
			// Deleted by its owner, or tombstoned by a run that stopped
			// before dropping the entry. Tombstone again to make sure it
			// left its owner's listing.
			if _, err := w.store.Tombstone(ctx, l, now); err != nil {
				return err
			}
			done = append(done, e)
		case l.ExpiresAt == nil || !l.ExpiresAt.Before(now):
			done = append(done, e) // extended: its new expiry has its own entry
		default:
			expired = append(expired, l)
			expiredEntries = append(expiredEntries, e)
		}
	}
	s.Skipped += len(done)

	if len(expired) > 0 {
		if err := w.archiveBatch(ctx, day, bucket, now, expired); err != nil {
			return err
		}
		s.Objects++
		for _, l := range expired {
			res, err := w.store.Tombstone(ctx, l, now)
			if err != nil {
				return err
			}
			if res == Tombstoned {
				s.Archived++
			} else {
				s.Skipped++ // extended after it was read; the record is an extra
			}
		}
		done = append(done, expiredEntries...)
	}
	for _, e := range done {
		if err := w.store.DeleteExpiry(ctx, day, bucket, e); err != nil {
			return err
		}
	}
	return nil
}

// archiveBatch writes one object per batch, named after the partition, the
// batch's first key and the run's time. An object is never replaced: a
// repeated run's batch may hold fewer links (some already tombstoned), and
// replacing the earlier object would lose their records.
func (w *Worker) archiveBatch(ctx context.Context, day time.Time, bucket int, now time.Time, links []link.Link) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, l := range links {
		err := enc.Encode(Record{
			Key: l.Key, LongURL: l.LongURL, UserID: l.UserID,
			CreatedAt: l.CreatedAt, ExpiresAt: l.ExpiresAt, ArchivedAt: now,
		})
		if err != nil {
			return err
		}
	}
	name := fmt.Sprintf("expired/%s/%03d/%s-%d.jsonl", day.Format(time.DateOnly), bucket, links[0].Key, now.UnixNano())
	if err := w.archive.Put(ctx, name, buf.Bytes()); err != nil {
		return fmt.Errorf("archive %s: %w", name, err)
	}
	return nil
}

// Day returns the UTC day of t, the expiry index's partition day.
func Day(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
