package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

// recordingPublisher keeps published clicks.
type recordingPublisher struct {
	mu     sync.Mutex
	clicks []analytics.Click
}

func (p *recordingPublisher) Publish(c analytics.Click) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clicks = append(p.clicks, c)
}

// fakeStats returns fixed statistics and records the requested range.
type fakeStats struct {
	stats    analytics.Stats
	err      error
	from, to time.Time
}

func (f *fakeStats) Stats(_ context.Context, _ string, from, to time.Time) (analytics.Stats, error) {
	f.from, f.to = from, to
	return f.stats, f.err
}

var statsNow = time.Date(2026, 11, 12, 15, 30, 0, 0, time.UTC)

func newAnalyticsHandler(t *testing.T, cfg Config) (*Handler, *memory.Store) {
	t.Helper()
	store := memory.New()
	cfg.Links = link.NewService(store, "sho.rt")
	cfg.Accounts = account.NewService(memory.NewAccounts())
	cfg.BaseURL = "https://sho.rt"
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(cfg)
	h.now = func() time.Time { return statsNow }
	return h, store
}

func TestRedirectPublishesClicks(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	tests := []struct {
		name          string
		countryHeader string // configured header name
		path          string
		headers       map[string]string
		wantClick     *analytics.Click
	}{
		{
			name: "redirect publishes a click", path: "/abc1234",
			headers:   map[string]string{"User-Agent": "curl/8.7.1", "Referer": "https://news.example/x"},
			wantClick: &analytics.Click{Key: "abc1234", Time: statsNow, UserAgent: "curl/8.7.1", Referrer: "https://news.example/x"},
		},
		{
			name: "country from the configured header", countryHeader: "CF-IPCountry", path: "/abc1234",
			headers:   map[string]string{"CF-IPCountry": "de"},
			wantClick: &analytics.Click{Key: "abc1234", Time: statsNow, Country: "DE"},
		},
		{
			name: "country header ignored unless configured", path: "/abc1234",
			headers:   map[string]string{"CF-IPCountry": "DE"},
			wantClick: &analytics.Click{Key: "abc1234", Time: statsNow},
		},
		{name: "missing link publishes nothing", path: "/missing"},
		{name: "expired link publishes nothing", path: "/expired"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &recordingPublisher{}
			h, store := newAnalyticsHandler(t, Config{Clicks: pub, CountryHeader: tt.countryHeader})
			for _, l := range []link.Link{
				{Key: "abc1234", LongURL: "https://example.com"},
				{Key: "expired", LongURL: "https://example.com", ExpiresAt: &past},
			} {
				if err := store.Create(context.Background(), l); err != nil {
					t.Fatal(err)
				}
			}
			req := httptest.NewRequest("GET", tt.path, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)

			switch {
			case tt.wantClick == nil && len(pub.clicks) != 0:
				t.Errorf("published %+v, want nothing", pub.clicks)
			case tt.wantClick != nil && (len(pub.clicks) != 1 || pub.clicks[0] != *tt.wantClick):
				t.Errorf("published %+v, want %+v", pub.clicks, *tt.wantClick)
			}
		})
	}
}

func TestStats(t *testing.T) {
	day := func(s string) time.Time { d, _ := time.Parse(time.DateOnly, s); return d }
	last := time.Date(2026, 11, 12, 15, 0, 0, 0, time.UTC)
	reader := &fakeStats{stats: analytics.Stats{
		Total:      5,
		Timeseries: []analytics.DayCount{{Day: day("2026-11-10"), Clicks: 2}, {Day: day("2026-11-12"), Clicks: 3}},
		ByCountry:  []analytics.ValueCount{{Value: "DE", Clicks: 4}, {Value: "unknown", Clicks: 1}},
		ByDevice:   []analytics.ValueCount{{Value: "mobile", Clicks: 5}},
		LastClick:  last,
	}}
	h, _ := newAnalyticsHandler(t, Config{Stats: reader})
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	l := createLink(t, h, alice, `{"long_url":"https://example.com"}`)
	stats := func(query, apiKey string) request {
		return request{method: "GET", path: "/api/v1/links/" + l.Key + "/stats" + query, apiKey: apiKey}
	}

	tests := []struct {
		name       string
		req        request
		readerErr  error
		wantStatus int
		wantFrom   string
		wantTo     string
		check      func(t *testing.T, resp statsResponse)
	}{
		{
			name: "default range is the last 30 days", req: stats("", alice), wantStatus: http.StatusOK,
			wantFrom: "2026-10-14", wantTo: "2026-11-12",
			check: func(t *testing.T, resp statsResponse) {
				if resp.TotalClicks != 5 || len(resp.Timeseries) != 2 || resp.Timeseries[1] != (dayClicks{Date: "2026-11-12", Clicks: 3}) {
					t.Errorf("response = %+v", resp)
				}
				if len(resp.ByCountry) != 2 || resp.ByCountry[0] != (valueCount{Value: "DE", Clicks: 4}) || len(resp.ByReferrer) != 0 || resp.ByReferrer == nil {
					t.Errorf("dimensions = %+v %+v (empty lists must be [] not null)", resp.ByCountry, resp.ByReferrer)
				}
				if resp.UpdatedAt == nil || !resp.UpdatedAt.Equal(last) {
					t.Errorf("updated_at = %v, want %v", resp.UpdatedAt, last)
				}
			},
		},
		{name: "explicit range", req: stats("?from=2026-11-01&to=2026-11-07&granularity=day", alice), wantStatus: http.StatusOK, wantFrom: "2026-11-01", wantTo: "2026-11-07"},
		{name: "one day", req: stats("?from=2026-11-05&to=2026-11-05", alice), wantStatus: http.StatusOK, wantFrom: "2026-11-05", wantTo: "2026-11-05"},
		{name: "without key", req: stats("", ""), wantStatus: http.StatusUnauthorized},
		{name: "other user's link", req: stats("", bob), wantStatus: http.StatusNotFound},
		{name: "missing link", req: request{method: "GET", path: "/api/v1/links/missing1/stats", apiKey: alice}, wantStatus: http.StatusNotFound},
		{name: "bad from", req: stats("?from=yesterday", alice), wantStatus: http.StatusBadRequest},
		{name: "bad to", req: stats("?to=2026-13-01", alice), wantStatus: http.StatusBadRequest},
		{name: "from after to", req: stats("?from=2026-11-10&to=2026-11-01", alice), wantStatus: http.StatusBadRequest},
		{name: "range above a year", req: stats("?from=2025-01-01&to=2026-11-01", alice), wantStatus: http.StatusBadRequest},
		{name: "hourly granularity", req: stats("?granularity=hour", alice), wantStatus: http.StatusBadRequest},
		{name: "analytics store down", req: stats("", alice), readerErr: errors.New("clickhouse: connection refused"), wantStatus: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader.err, reader.from, reader.to = tt.readerErr, time.Time{}, time.Time{}
			rec := send(h, tt.req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantFrom != "" && (reader.from.Format(time.DateOnly) != tt.wantFrom || reader.to.Format(time.DateOnly) != tt.wantTo) {
				t.Errorf("queried %s..%s, want %s..%s", reader.from.Format(time.DateOnly), reader.to.Format(time.DateOnly), tt.wantFrom, tt.wantTo)
			}
			if tt.check != nil {
				var resp statsResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatal(err)
				}
				tt.check(t, resp)
			}
		})
	}
}

func TestStatsWithoutAnalytics(t *testing.T) {
	h, _ := newAnalyticsHandler(t, Config{})
	key := registerUser(t, h, "alice@example.com")
	l := createLink(t, h, key, `{"long_url":"https://example.com"}`)

	tests := []struct {
		name       string
		wantStatus int
		wantCode   string
	}{
		{name: "stats are unavailable", wantStatus: http.StatusServiceUnavailable, wantCode: "analytics_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := send(h, request{method: "GET", path: "/api/v1/links/" + l.Key + "/stats", apiKey: key})
			if rec.Code != tt.wantStatus || !strings.Contains(rec.Body.String(), tt.wantCode) {
				t.Errorf("status = %d, body %s; want %d %s", rec.Code, rec.Body, tt.wantStatus, tt.wantCode)
			}
		})
	}
}
