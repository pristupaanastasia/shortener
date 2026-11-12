// Package clickhouse stores click events and answers statistics queries.
// The schema comes from migrations/clickhouse.
package clickhouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
)

// TopValues is how many values per dimension Stats returns.
const TopValues = 10

// Store implements analytics.Sink and serves statistics.
type Store struct {
	conn driver.Conn
}

// Open connects using a DSN like clickhouse://user:pass@host:9000/db and
// applies pending migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	opts.DialTimeout = 2 * time.Second
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("connect to clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if err := applyMigrations(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Store{conn: conn}, nil
}

// Close closes the connection.
func (s *Store) Close() { _ = s.conn.Close() }

// applyMigrations runs the migrations from migrations/clickhouse that are not
// recorded in schema_migrations. ClickHouse DDL is not transactional, so every
// statement must be idempotent (IF NOT EXISTS).
func applyMigrations(ctx context.Context, conn driver.Conn) error {
	all, err := migrate.Load(migrations.ClickHouse, "clickhouse")
	if err != nil {
		return err
	}
	err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version UInt32, name String, applied_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree ORDER BY version`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, `SELECT DISTINCT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied := map[int]bool{}
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		applied[int(v)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	for _, m := range migrate.Pending(all, applied) {
		for _, stmt := range m.Statements {
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s: %w", m.Name, err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES (?, ?)`, uint32(m.Version), m.Name); err != nil {
			return fmt.Errorf("record migration %s: %w", m.Name, err)
		}
	}
	return nil
}

// Insert implements analytics.Sink with one batch insert: ClickHouse handles
// large inserts well and many small ones poorly.
func (s *Store) Insert(ctx context.Context, rows []analytics.Row) error {
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO clicks (key, ts, country, device, referrer)`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(r.Key, r.Time, r.Country, r.Device, r.Referrer); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append click: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert %d clicks: %w", len(rows), err)
	}
	return nil
}

// Stats implements analytics.StatsReader.
func (s *Store) Stats(ctx context.Context, key string, from, to time.Time) (analytics.Stats, error) {
	var st analytics.Stats
	rows, err := s.conn.Query(ctx, `
		SELECT day, sum(clicks) FROM clicks_daily
		WHERE key = ? AND day BETWEEN ? AND ?
		GROUP BY day ORDER BY day`, key, from, to)
	if err != nil {
		return analytics.Stats{}, fmt.Errorf("query timeseries: %w", err)
	}
	for rows.Next() {
		var d analytics.DayCount
		if err := rows.Scan(&d.Day, &d.Clicks); err != nil {
			rows.Close()
			return analytics.Stats{}, fmt.Errorf("scan timeseries: %w", err)
		}
		st.Timeseries = append(st.Timeseries, d)
		st.Total += d.Clicks
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return analytics.Stats{}, fmt.Errorf("query timeseries: %w", err)
	}

	rows, err = s.conn.Query(ctx, `
		SELECT dimension, value, sum(clicks) AS c FROM clicks_by_dimension
		WHERE key = ? AND day BETWEEN ? AND ?
		GROUP BY dimension, value
		ORDER BY dimension, c DESC, value
		LIMIT ? BY dimension`, key, from, to, TopValues)
	if err != nil {
		return analytics.Stats{}, fmt.Errorf("query dimensions: %w", err)
	}
	for rows.Next() {
		var dim string
		var v analytics.ValueCount
		if err := rows.Scan(&dim, &v.Value, &v.Clicks); err != nil {
			rows.Close()
			return analytics.Stats{}, fmt.Errorf("scan dimensions: %w", err)
		}
		switch dim {
		case "country":
			st.ByCountry = append(st.ByCountry, v)
		case "device":
			st.ByDevice = append(st.ByDevice, v)
		case "referrer":
			st.ByReferrer = append(st.ByReferrer, v)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return analytics.Stats{}, fmt.Errorf("query dimensions: %w", err)
	}

	// From clicks_last, not clicks: raw clicks expire after 90 days.
	if err := s.conn.QueryRow(ctx, `SELECT max(last_ts) FROM clicks_last WHERE key = ?`, key).Scan(&st.LastClick); err != nil {
		return analytics.Stats{}, fmt.Errorf("query last click: %w", err)
	}
	if st.LastClick.Unix() <= 0 {
		st.LastClick = time.Time{} // max() of no rows is the epoch
	}
	return st, nil
}

// LazyStats connects on first use, so a service that only reads statistics
// starts and keeps serving (redirects) while ClickHouse is down. Until a
// connection succeeds, Stats returns an error (the API answers 503); failed
// attempts are retried at most every retryInterval.
type LazyStats struct {
	dsn           string
	retryInterval time.Duration

	mu          sync.Mutex
	store       *Store
	lastAttempt time.Time
	lastErr     error
}

// NewLazyStats returns a LazyStats for dsn and tries to connect once.
func NewLazyStats(ctx context.Context, dsn string) *LazyStats {
	l := &LazyStats{dsn: dsn, retryInterval: 10 * time.Second}
	_, _ = l.get(ctx)
	return l
}

// Err returns the last connection error, or nil when connected.
func (l *LazyStats) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store != nil {
		return nil
	}
	return l.lastErr
}

func (l *LazyStats) get(ctx context.Context) (*Store, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store != nil {
		return l.store, nil
	}
	if !l.lastAttempt.IsZero() && time.Since(l.lastAttempt) < l.retryInterval {
		return nil, l.lastErr
	}
	l.lastAttempt = time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := Open(ctx, l.dsn)
	if err != nil {
		l.lastErr = err
		return nil, err
	}
	l.store = s
	return s, nil
}

// Stats implements analytics.StatsReader.
func (l *LazyStats) Stats(ctx context.Context, key string, from, to time.Time) (analytics.Stats, error) {
	s, err := l.get(ctx)
	if err != nil {
		return analytics.Stats{}, err
	}
	return s.Stats(ctx, key, from, to)
}

// Close closes the connection, if any.
func (l *LazyStats) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store != nil {
		l.store.Close()
	}
}
