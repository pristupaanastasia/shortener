package cleanup

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pristupaanastasia/shortener/internal/link"
)

// fakeStore is an in-memory Store with the same semantics as the Cassandra
// one: the index is separate from the links, and Tombstone is conditional.
type fakeStore struct {
	mu         sync.Mutex
	buckets    int
	links      map[string]link.Link
	index      map[string][]Entry // "day/bucket" -> entries
	checkpoint *time.Time
	listed     map[string]bool // keys in their owner's listing

	failTombstone   string // Tombstone of this key fails
	extendOnRead    string // the link is extended right after it is read
	changeURLOnRead string // the link's URL is changed right after it is read
	tombstones      int
	reads           int // Expired calls
}

func newFakeStore(buckets int) *fakeStore {
	return &fakeStore{buckets: buckets, links: map[string]link.Link{}, index: map[string][]Entry{}, listed: map[string]bool{}}
}

func partition(day time.Time, bucket int) string {
	return day.Format(time.DateOnly) + "/" + string(rune('a'+bucket))
}

// add stores a link and indexes its expiry in bucket 0.
func (f *fakeStore) add(l link.Link) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links[l.Key] = l
	f.listed[l.Key] = l.UserID != ""
	if l.ExpiresAt != nil {
		p := partition(Day(*l.ExpiresAt), 0)
		f.index[p] = append(f.index[p], Entry{Key: l.Key, ExpiresAt: *l.ExpiresAt})
	}
}

func (f *fakeStore) indexed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, es := range f.index {
		n += len(es)
	}
	return n
}

func (f *fakeStore) Get(_ context.Context, key string) (link.Link, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.links[key]
	if !ok {
		return link.Link{}, link.ErrNotFound
	}
	if key == f.extendOnRead {
		later := l.ExpiresAt.Add(time.Hour)
		f.links[key] = link.Link{Key: l.Key, LongURL: l.LongURL, UserID: l.UserID, CreatedAt: l.CreatedAt, ExpiresAt: &later}
	}
	if key == f.changeURLOnRead {
		changed := l
		changed.LongURL += "/changed"
		f.links[key] = changed
		f.changeURLOnRead = ""
	}
	return l, nil
}

func (f *fakeStore) ExpiryBuckets() int { return f.buckets }

func (f *fakeStore) Expired(_ context.Context, day time.Time, bucket int, before time.Time, after *Entry, limit int) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	entries := slices.Clone(f.index[partition(day, bucket)])
	slices.SortFunc(entries, compareEntries)
	var out []Entry
	for _, e := range entries {
		if after != nil && compareEntries(e, *after) <= 0 {
			continue
		}
		if e.ExpiresAt.Before(before) && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

// compareEntries orders entries like the clustering key (expires_at, key).
func compareEntries(a, b Entry) int {
	if c := a.ExpiresAt.Compare(b.ExpiresAt); c != 0 {
		return c
	}
	return strings.Compare(a.Key, b.Key)
}

func (f *fakeStore) DeleteExpiryPartition(_ context.Context, day time.Time, bucket int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.index, partition(day, bucket))
	return nil
}

func (f *fakeStore) Tombstone(_ context.Context, l link.Link, at time.Time) (TombstoneResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l.Key == f.failTombstone {
		return 0, errors.New("cassandra timeout")
	}
	cur := f.links[l.Key]
	if cur.DeletedAt == nil {
		if cur.ExpiresAt == nil || !cur.ExpiresAt.Equal(*l.ExpiresAt) || cur.LongURL != l.LongURL {
			return Changed, nil
		}
		f.links[l.Key] = link.Link{Key: cur.Key, UserID: cur.UserID, CreatedAt: cur.CreatedAt, ExpiresAt: cur.ExpiresAt, DeletedAt: &at}
		f.tombstones++
		f.listed[l.Key] = false
		return Tombstoned, nil
	}
	f.listed[l.Key] = false
	return AlreadyDeleted, nil
}

func (f *fakeStore) Checkpoint(context.Context, string) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.checkpoint == nil {
		return time.Time{}, false, nil
	}
	return *f.checkpoint, true, nil
}

func (f *fakeStore) SetCheckpoint(_ context.Context, _ string, day time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpoint = &day
	return nil
}

// fakeArchive keeps objects in memory.
type fakeArchive struct {
	mu      sync.Mutex
	objects map[string]string
	fail    bool
}

func (a *fakeArchive) Put(_ context.Context, name string, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return errors.New("s3 unavailable")
	}
	if a.objects == nil {
		a.objects = map[string]string{}
	}
	if _, ok := a.objects[name]; ok {
		return errors.New("object replaced: " + name)
	}
	a.objects[name] = string(data)
	return nil
}

// archivedKeys returns the keys in all objects, sorted, with duplicates.
func (a *fakeArchive) archivedKeys() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var keys []string
	for _, data := range a.objects {
		for line := range strings.Lines(data) {
			_, rest, _ := strings.Cut(line, `"key":"`)
			key, _, _ := strings.Cut(rest, `"`)
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}
