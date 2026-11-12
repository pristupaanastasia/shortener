//go:build integration

package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
)

func baseDSN() string {
	if dsn := os.Getenv("CLICKHOUSE_DSN"); dsn != "" {
		return dsn
	}
	return "clickhouse://shortener:shortener@127.0.0.1:9000/shortener"
}

// freshDatabase creates an empty database and returns a DSN for it.
func freshDatabase(t *testing.T) (dsn string, admin driver.Conn) {
	t.Helper()
	opts, err := ch.ParseDSN(baseDSN())
	if err != nil {
		t.Fatal(err)
	}
	admin, err = ch.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var b [4]byte
	_, _ = rand.Read(b[:])
	db := "migtest_" + hex.EncodeToString(b[:])
	if err := admin.Exec(context.Background(), "CREATE DATABASE "+db); err != nil {
		t.Fatalf("create database (is ClickHouse running? try `make up`): %v", err)
	}
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	return "clickhouse://shortener:shortener@" + opts.Addr[0] + "/" + db, admin
}

func TestMigrations(t *testing.T) {
	ctx := context.Background()
	dsn, admin := freshDatabase(t)
	opts, _ := ch.ParseDSN(dsn)
	db := opts.Auth.Database

	all, err := migrate.Load(migrations.ClickHouse, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	var wantVersions []uint32
	for _, m := range all {
		wantVersions = append(wantVersions, uint32(m.Version))
	}
	row := analytics.Row{Key: "migtest", Time: time.Now().UTC().Truncate(time.Millisecond), Country: "DE", Device: "mobile", Referrer: "direct"}

	steps := []struct {
		name   string
		action func(t *testing.T, s *Store)
	}{
		{name: "fresh database", action: func(t *testing.T, s *Store) {
			var tables []string
			rows, err := admin.Query(ctx, "SELECT name FROM system.tables WHERE database = ?", db)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var name string
				_ = rows.Scan(&name)
				tables = append(tables, name)
			}
			rows.Close()
			for _, want := range []string{"clicks", "clicks_daily", "clicks_daily_mv", "clicks_by_dimension", "clicks_by_dimension_mv", "schema_migrations"} {
				if !slices.Contains(tables, want) {
					t.Errorf("table %s missing; tables = %v", want, tables)
				}
			}
			if err := s.Insert(ctx, []analytics.Row{row}); err != nil {
				t.Fatalf("the migrated schema rejects a click: %v", err)
			}
		}},
		{name: "second run is a no-op and keeps data", action: func(t *testing.T, s *Store) {
			st, err := s.Stats(ctx, "migtest", row.Time.AddDate(0, 0, -1), row.Time.AddDate(0, 0, 1))
			if err != nil || st.Total != 1 {
				t.Errorf("stats after re-running migrations = %+v, %v; want 1 click", st, err)
			}
		}},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			s, err := Open(ctx, dsn)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer s.Close()
			var versions []uint32
			rows, err := admin.Query(ctx, "SELECT DISTINCT version FROM "+db+".schema_migrations ORDER BY version")
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var v uint32
				_ = rows.Scan(&v)
				versions = append(versions, v)
			}
			rows.Close()
			if !slices.Equal(versions, wantVersions) {
				t.Errorf("schema_migrations = %v, want %v", versions, wantVersions)
			}
			step.action(t, s)
		})
		if !ok {
			break
		}
	}
}

func TestInsertAndStats(t *testing.T) {
	ctx := context.Background()
	dsn, _ := freshDatabase(t)
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	d1 := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 11, 12, 18, 30, 0, 0, time.UTC)
	rows := []analytics.Row{
		{Key: "abc", Time: d1, Country: "DE", Device: "mobile", Referrer: "twitter.com"},
		{Key: "abc", Time: d1.Add(time.Hour), Country: "DE", Device: "desktop", Referrer: "direct"},
		{Key: "abc", Time: d2, Country: "US", Device: "mobile", Referrer: "twitter.com"},
		{Key: "other", Time: d2, Country: "FR", Device: "bot", Referrer: "direct"},
	}
	// Two inserts, so the aggregates span several parts before merging.
	for _, batch := range [][]analytics.Row{rows[:2], rows[2:]} {
		if err := s.Insert(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	day := func(d time.Time) time.Time { return d.Truncate(24 * time.Hour) }

	tests := []struct {
		name          string
		key           string
		from, to      time.Time
		wantTotal     uint64
		wantDays      []string
		wantCountries []analytics.ValueCount
		wantLast      time.Time
	}{
		{
			name: "whole range", key: "abc", from: day(d1), to: day(d2), wantTotal: 3,
			wantDays:      []string{"2026-11-10:2", "2026-11-12:1"},
			wantCountries: []analytics.ValueCount{{Value: "DE", Clicks: 2}, {Value: "US", Clicks: 1}},
			wantLast:      d2,
		},
		{
			name: "range excludes later days", key: "abc", from: day(d1), to: day(d1), wantTotal: 2,
			wantDays:      []string{"2026-11-10:2"},
			wantCountries: []analytics.ValueCount{{Value: "DE", Clicks: 2}},
			wantLast:      d2, // the latest click overall, not within the range
		},
		{name: "other links do not leak in", key: "other", from: day(d1), to: day(d2), wantTotal: 1, wantDays: []string{"2026-11-12:1"},
			wantCountries: []analytics.ValueCount{{Value: "FR", Clicks: 1}}, wantLast: d2},
		{name: "no clicks", key: "nothing", from: day(d1), to: day(d2), wantTotal: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := s.Stats(ctx, tt.key, tt.from, tt.to)
			if err != nil {
				t.Fatal(err)
			}
			var days []string
			for _, d := range st.Timeseries {
				days = append(days, d.Day.Format(time.DateOnly)+":"+strconv.FormatUint(d.Clicks, 10))
			}
			if st.Total != tt.wantTotal || !slices.Equal(days, tt.wantDays) || !slices.Equal(st.ByCountry, tt.wantCountries) {
				t.Errorf("Stats = total %d, days %v, countries %v; want %d, %v, %v", st.Total, days, st.ByCountry, tt.wantTotal, tt.wantDays, tt.wantCountries)
			}
			if !st.LastClick.Equal(tt.wantLast) {
				t.Errorf("LastClick = %v, want %v", st.LastClick, tt.wantLast)
			}
		})
	}

}

func TestTopValues(t *testing.T) {
	ctx := context.Background()
	dsn, _ := freshDatabase(t)
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	day := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		countries     int // country i gets i+1 clicks
		wantCountries int
		wantFirst     analytics.ValueCount
		wantTotal     uint64
	}{
		{name: "fewer values than the limit", countries: 3, wantCountries: 3, wantFirst: analytics.ValueCount{Value: "CX", Clicks: 3}, wantTotal: 6},
		{name: "more values than the limit", countries: 12, wantCountries: TopValues, wantFirst: analytics.ValueCount{Value: "LX", Clicks: 12}, wantTotal: 78},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "top" + strconv.Itoa(tt.countries)
			var rows []analytics.Row
			for i := range tt.countries {
				for range i + 1 {
					rows = append(rows, analytics.Row{Key: key, Time: day, Country: string(rune('A'+i)) + "X", Device: "desktop", Referrer: "direct"})
				}
			}
			if err := s.Insert(ctx, rows); err != nil {
				t.Fatal(err)
			}
			st, err := s.Stats(ctx, key, day, day)
			if err != nil {
				t.Fatal(err)
			}
			if len(st.ByCountry) != tt.wantCountries || st.ByCountry[0] != tt.wantFirst || st.Total != tt.wantTotal || len(st.ByDevice) != 1 {
				t.Errorf("Stats = %d countries starting %v, total %d, devices %v; want %d starting %v, total %d, one device",
					len(st.ByCountry), st.ByCountry[0], st.Total, st.ByDevice, tt.wantCountries, tt.wantFirst, tt.wantTotal)
			}
		})
	}
}
