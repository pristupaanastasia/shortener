package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

// newTestHandler returns a handler without Redis-backed features.
func newTestHandler(t *testing.T) (*Handler, *memory.Store) {
	t.Helper()
	store := memory.New()
	return New(Config{
		Links:    link.NewService(store, "sho.rt"),
		Accounts: account.NewService(memory.NewAccounts()),
		BaseURL:  "https://sho.rt/",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}), store
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorBody {
	t.Helper()
	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return resp.Error
}

func TestCreateLink(t *testing.T) {
	h, _ := newTestHandler(t)
	var firstKey string

	steps := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		check      func(t *testing.T, rec *httptest.ResponseRecorder)
	}{
		{
			name: "generated key", method: http.MethodPost, path: "/api/v1/links",
			body: `{"long_url":"https://example.com"}`, wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				var resp linkResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatal(err)
				}
				if len(resp.Key) != 7 || resp.ShortURL != "https://sho.rt/"+resp.Key || resp.LongURL != "https://example.com" {
					t.Errorf("response = %+v", resp)
				}
				firstKey = resp.Key
			},
		},
		{
			name: "same url again gets a new key", method: http.MethodPost, path: "/api/v1/links",
			body: `{"long_url":"https://example.com"}`, wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				var resp linkResponse
				_ = json.NewDecoder(rec.Body).Decode(&resp)
				if resp.Key == firstKey {
					t.Errorf("key %q was reused", resp.Key)
				}
			},
		},
		{
			name: "alias", method: http.MethodPost, path: "/api/v1/links",
			body: `{"long_url":"https://example.com/x","alias":"mylink"}`, wantStatus: http.StatusCreated,
		},
		{
			name: "alias taken", method: http.MethodPost, path: "/api/v1/links",
			body: `{"long_url":"https://example.com/y","alias":"mylink"}`, wantStatus: http.StatusConflict,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				if got := decodeError(t, rec).Code; got != "alias_taken" {
					t.Errorf("code = %q, want alias_taken", got)
				}
			},
		},
		{
			name: "alias redirects to the first url", method: http.MethodGet, path: "/mylink",
			wantStatus: http.StatusMovedPermanently,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				if got := rec.Header().Get("Location"); got != "https://example.com/x" {
					t.Errorf("Location = %q", got)
				}
			},
		},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			rec := do(h, step.method, step.path, step.body)
			if rec.Code != step.wantStatus {
				t.Fatalf("%s %s: status = %d, want %d; body: %s", step.method, step.path, rec.Code, step.wantStatus, rec.Body)
			}
			if step.check != nil {
				step.check(t, rec)
			}
		})
		if !ok {
			break
		}
	}
}

func TestCreateLinkErrors(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "malformed json", body: `{`, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unknown field", body: `{"long_url":"https://example.com","foo":1}`, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "invalid url", body: `{"long_url":"ftp://example.com"}`, wantStatus: http.StatusBadRequest, wantCode: "invalid_url"},
		{name: "invalid alias", body: `{"long_url":"https://example.com","alias":"a/b"}`, wantStatus: http.StatusBadRequest, wantCode: "invalid_alias"},
		{name: "expiry in past", body: `{"long_url":"https://example.com","expires_at":"2000-01-01T00:00:00Z"}`, wantStatus: http.StatusBadRequest, wantCode: "invalid_expires_at"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler(t)
			rec := do(h, http.MethodPost, "/api/v1/links", tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := decodeError(t, rec); got.Code != tt.wantCode || got.Message == "" {
				t.Errorf("error = %+v, want code %q with a message", got, tt.wantCode)
			}
		})
	}
}

func TestRedirect(t *testing.T) {
	// The service checks expiry with the real clock, so times are relative to
	// time.Now(); the handler's clock is pinned to it for exact max-age values.
	now := time.Now()
	soon := now.Add(30 * time.Second)
	past := now.Add(-time.Hour)

	h, store := newTestHandler(t)
	h.now = func() time.Time { return now }
	for _, l := range []link.Link{
		{Key: "forever", LongURL: "https://example.com/forever"},
		{Key: "soon123", LongURL: "https://example.com/soon", ExpiresAt: &soon},
		{Key: "expired", LongURL: "https://example.com/expired", ExpiresAt: &past},
		{Key: "deleted", LongURL: "https://example.com/deleted", DeletedAt: &past},
	} {
		if err := store.Create(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name         string
		path         string
		wantStatus   int
		wantLocation string
		wantCache    string
	}{
		{name: "permanent link", path: "/forever", wantStatus: http.StatusMovedPermanently, wantLocation: "https://example.com/forever", wantCache: "private, max-age=90"},
		{name: "max-age capped by expiry", path: "/soon123", wantStatus: http.StatusMovedPermanently, wantLocation: "https://example.com/soon", wantCache: "private, max-age=30"},
		{name: "missing", path: "/missing", wantStatus: http.StatusNotFound},
		{name: "expired", path: "/expired", wantStatus: http.StatusGone},
		{name: "deleted", path: "/deleted", wantStatus: http.StatusGone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, tt.path, "")
			if rec.Code != tt.wantStatus {
				t.Fatalf("GET %s: status = %d, want %d", tt.path, rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Location"); got != tt.wantLocation {
				t.Errorf("Location = %q, want %q", got, tt.wantLocation)
			}
			if got := rec.Header().Get("Cache-Control"); got != tt.wantCache {
				t.Errorf("Cache-Control = %q, want %q", got, tt.wantCache)
			}
			if tt.wantStatus != http.StatusMovedPermanently && !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
				t.Errorf("Content-Type = %q, want an HTML error page", rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestRoutes(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{name: "health check", method: http.MethodGet, path: "/healthz", wantStatus: http.StatusOK},
		{name: "root has no link", method: http.MethodGet, path: "/", wantStatus: http.StatusNotFound},
		{name: "wrong method on links", method: http.MethodPut, path: "/api/v1/links", wantStatus: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler(t)
			if rec := do(h, tt.method, tt.path, ""); rec.Code != tt.wantStatus {
				t.Errorf("%s %s: status = %d, want %d", tt.method, tt.path, rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestRedirectCacheAge(t *testing.T) {
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time { t := now.Add(d); return &t }
	tests := []struct {
		name      string
		expiresAt *time.Time
		want      int
	}{
		{name: "no expiry", expiresAt: nil, want: 90},
		{name: "expires later", expiresAt: at(time.Hour), want: 90},
		{name: "expires sooner", expiresAt: at(10 * time.Second), want: 10},
		{name: "sub-second", expiresAt: at(500 * time.Millisecond), want: 0},
		{name: "already expired", expiresAt: at(-time.Second), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redirectCacheAge(link.Link{ExpiresAt: tt.expiresAt}, now); got != tt.want {
				t.Errorf("redirectCacheAge(expires %v) = %d, want %d", tt.expiresAt, got, tt.want)
			}
		})
	}
}
