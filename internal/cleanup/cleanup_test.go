package cleanup

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/pristupaanastasia/shortener/internal/link"
)

var now = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func expiring(key string, at time.Time) link.Link {
	return link.Link{Key: key, LongURL: "https://example.com/" + key, UserID: "u1", CreatedAt: at.Add(-24 * time.Hour), ExpiresAt: &at}
}

func newWorker(f *fakeStore, a *fakeArchive, batch int) *Worker {
	cfg := DefaultConfig()
	cfg.BatchSize = batch
	return New(f, a, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestRun(t *testing.T) {
	yesterday := now.Add(-24 * time.Hour)
	tests := []struct {
		name           string
		setup          func(f *fakeStore, a *fakeArchive)
		batch          int
		wantErr        bool
		wantArchived   []string // keys in the archive
		wantTombstoned []string
		wantStats      Stats
		wantIndexed    int
		wantCheckpoint time.Time
	}{
		{
			name:         "expired link is archived and tombstoned",
			setup:        func(f *fakeStore, _ *fakeArchive) { f.add(expiring("a", yesterday)) },
			wantArchived: []string{"a"}, wantTombstoned: []string{"a"},
			wantStats: Stats{Archived: 1, Objects: 1}, wantCheckpoint: Day(now),
		},
		{
			name:         "earlier today is expired too, but today stays open",
			setup:        func(f *fakeStore, _ *fakeArchive) { f.add(expiring("a", now.Add(-time.Hour))) },
			wantArchived: []string{"a"}, wantTombstoned: []string{"a"},
			wantStats: Stats{Archived: 1, Objects: 1}, wantCheckpoint: Day(now),
		},
		{
			name:        "link expiring later today is kept",
			setup:       func(f *fakeStore, _ *fakeArchive) { f.add(expiring("a", now.Add(time.Hour))) },
			wantIndexed: 1, wantCheckpoint: Day(now),
		},
		{
			name: "extended link is skipped and its old entry dropped",
			setup: func(f *fakeStore, _ *fakeArchive) {
				f.add(expiring("a", yesterday))
				l := f.links["a"]
				later := now.Add(48 * time.Hour)
				l.ExpiresAt = &later
				f.links["a"] = l
			},
			wantStats: Stats{Skipped: 1}, wantCheckpoint: Day(now),
		},
		{
			name: "deleted link is not archived",
			setup: func(f *fakeStore, _ *fakeArchive) {
				f.add(expiring("a", yesterday))
				l := f.links["a"]
				l.DeletedAt = &yesterday
				f.links["a"] = l
			},
			wantStats: Stats{Skipped: 1}, wantCheckpoint: Day(now),
		},
		{
			name: "index entry without a link is dropped",
			setup: func(f *fakeStore, _ *fakeArchive) {
				f.add(expiring("a", yesterday))
				delete(f.links, "a")
			},
			wantStats: Stats{Skipped: 1}, wantCheckpoint: Day(now),
		},
		{
			name: "extension between read and tombstone wins",
			setup: func(f *fakeStore, _ *fakeArchive) {
				f.add(expiring("a", yesterday))
				f.extendOnRead = "a"
			},
			wantArchived: []string{"a"}, // an extra record, never a missing one
			wantStats:    Stats{Skipped: 1, Objects: 1}, wantCheckpoint: Day(now),
		},
		{
			name: "archive down: nothing is tombstoned",
			setup: func(f *fakeStore, a *fakeArchive) {
				f.add(expiring("a", yesterday))
				a.fail = true
			},
			// The days before are done; yesterday is the first one that is not.
			wantErr: true, wantIndexed: 1, wantCheckpoint: Day(yesterday),
		},
		{
			name: "batches become separate objects",
			setup: func(f *fakeStore, _ *fakeArchive) {
				for _, k := range []string{"a", "b", "c", "d", "e"} {
					f.add(expiring(k, yesterday))
				}
			},
			batch:        2,
			wantArchived: []string{"a", "b", "c", "d", "e"}, wantTombstoned: []string{"a", "b", "c", "d", "e"},
			wantStats: Stats{Archived: 5, Objects: 3}, wantCheckpoint: Day(now),
		},
		{
			name:        "first run starts at the lookback",
			setup:       func(f *fakeStore, _ *fakeArchive) { f.add(expiring("old", now.AddDate(0, 0, -40))) },
			wantIndexed: 1, wantCheckpoint: Day(now),
		},
		{
			name: "a later run starts at the checkpoint",
			setup: func(f *fakeStore, _ *fakeArchive) {
				f.add(expiring("old", now.AddDate(0, 0, -40)))
				cp := Day(now.AddDate(0, 0, -41))
				f.checkpoint = &cp
			},
			wantArchived: []string{"old"}, wantTombstoned: []string{"old"},
			wantStats: Stats{Archived: 1, Objects: 1}, wantCheckpoint: Day(now),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, a := newFakeStore(2), &fakeArchive{}
			tt.setup(f, a)
			batch := tt.batch
			if batch == 0 {
				batch = 500
			}
			stats, err := newWorker(f, a, batch).Run(context.Background(), now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Run() error = %v, want error: %v", err, tt.wantErr)
			}
			if stats != tt.wantStats {
				t.Errorf("stats = %+v, want %+v", stats, tt.wantStats)
			}
			if got := a.archivedKeys(); !slices.Equal(got, tt.wantArchived) {
				t.Errorf("archived = %v, want %v", got, tt.wantArchived)
			}
			var tombstoned []string
			for k, l := range f.links {
				if l.DeletedAt != nil && l.LongURL == "" {
					tombstoned = append(tombstoned, k)
					if f.listed[k] {
						t.Errorf("%s is still in its owner's listing", k)
					}
				}
			}
			slices.Sort(tombstoned)
			if !slices.Equal(tombstoned, tt.wantTombstoned) {
				t.Errorf("tombstoned = %v, want %v", tombstoned, tt.wantTombstoned)
			}
			if got := f.indexed(); got != tt.wantIndexed {
				t.Errorf("index entries left = %d, want %d", got, tt.wantIndexed)
			}
			var cp time.Time
			if f.checkpoint != nil {
				cp = *f.checkpoint
			}
			if !cp.Equal(tt.wantCheckpoint) {
				t.Errorf("checkpoint = %v, want %v", cp, tt.wantCheckpoint)
			}
		})
	}
}

// A run that fails between archiving and tombstoning leaves the rest for the
// next run; every link ends up archived (possibly twice) and tombstoned.
func TestRunResumesAfterFailure(t *testing.T) {
	f, a := newFakeStore(1), &fakeArchive{}
	yesterday := now.Add(-24 * time.Hour)
	for _, k := range []string{"a", "b", "c"} {
		f.add(expiring(k, yesterday))
	}
	w := newWorker(f, a, 500)

	steps := []struct {
		name           string
		failTombstone  string
		at             time.Time
		wantErr        bool
		wantTombstoned int
		wantIndexed    int
	}{
		{name: "tombstone of b fails after the batch is archived", failTombstone: "b", at: now, wantErr: true, wantTombstoned: 1, wantIndexed: 3},
		{name: "next run finishes", at: now.Add(time.Minute), wantTombstoned: 3, wantIndexed: 0},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			f.failTombstone = step.failTombstone
			_, err := w.Run(context.Background(), step.at)
			if (err != nil) != step.wantErr {
				t.Fatalf("Run() error = %v, want error: %v", err, step.wantErr)
			}
			if f.tombstones != step.wantTombstoned {
				t.Errorf("tombstones = %d, want %d", f.tombstones, step.wantTombstoned)
			}
			if got := f.indexed(); got != step.wantIndexed {
				t.Errorf("index entries = %d, want %d", got, step.wantIndexed)
			}
		})
		if !ok {
			break
		}
	}
	// b and c were archived by both runs, a by the first only.
	if got, want := a.archivedKeys(), []string{"a", "b", "b", "c", "c"}; !slices.Equal(got, want) {
		t.Errorf("archived = %v, want %v", got, want)
	}
}

func TestDay(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{name: "utc", in: time.Date(2026, 11, 16, 23, 59, 0, 0, time.UTC), want: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)},
		{name: "other zone is converted first", in: time.Date(2026, 11, 17, 2, 0, 0, 0, time.FixedZone("UTC+4", 4*3600)), want: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Day(tt.in); !got.Equal(tt.want) {
				t.Errorf("Day(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
