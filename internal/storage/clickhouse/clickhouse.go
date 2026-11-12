// Package clickhouse stores click events and answers statistics queries.
// The schema comes from migrations/clickhouse.
package clickhouse

import (
	"context"
	"fmt"
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

	// The sort key starts with key, so this reads only this link's rows.
	if err := s.conn.QueryRow(ctx, `SELECT max(ts) FROM clicks WHERE key = ?`, key).Scan(&st.LastClick); err != nil {
		return analytics.Stats{}, fmt.Errorf("query last click: %w", err)
	}
	if st.LastClick.Unix() <= 0 {
		st.LastClick = time.Time{} // max() of no rows is the epoch
	}
	return st, nil
}
