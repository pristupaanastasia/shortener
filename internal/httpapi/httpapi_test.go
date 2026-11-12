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

	rec := do(h, http.MethodPost, "/api/v1/links", `{"long_url":"https://example.com/a"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rec.Code, rec.Body)
	}
	var resp linkResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Key) != 7 {
		t.Errorf("key = %q, want 7 characters", resp.Key)
	}
	if resp.ShortURL != "https://sho.rt/"+resp.Key {
		t.Errorf("short_url = %q", resp.ShortURL)
	}
	if resp.LongURL != "https://example.com/a" {
		t.Errorf("long_url = %q", resp.LongURL)
	}
}

func TestCreateLinkSameURLTwiceGivesNewKeys(t *testing.T) {
	h, _ := newTestHandler(t)
	keys := map[string]bool{}
	for range 2 {
		rec := do(h, http.MethodPost, "/api/v1/links", `{"long_url":"https://example.com"}`)
		var resp linkResponse
		_ = json.NewDecoder(rec.Body).Decode(&resp)
		keys[resp.Key] = true
	}
	if len(keys) != 2 {
		t.Errorf("got keys %v, want two different keys", keys)
	}
}

func TestCreateLinkErrors(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"malformed json", `{`, http.StatusBadRequest, "invalid_request"},
		{"unknown field", `{"long_url":"https://example.com","foo":1}`, http.StatusBadRequest, "invalid_request"},
		{"invalid url", `{"long_url":"ftp://example.com"}`, http.StatusBadRequest, "invalid_url"},
		{"invalid alias", `{"long_url":"https://example.com","alias":"a/b"}`, http.StatusBadRequest, "invalid_alias"},
		{"expiry in past", `{"long_url":"https://example.com","expires_at":"2000-01-01T00:00:00Z"}`, http.StatusBadRequest, "invalid_expires_at"},
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

func TestCreateLinkAliasTaken(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"long_url":"https://example.com","alias":"sale2026"}`

	if rec := do(h, http.MethodPost, "/api/v1/links", body); rec.Code != http.StatusCreated {
		t.Fatalf("first create: status = %d", rec.Code)
	}
	rec := do(h, http.MethodPost, "/api/v1/links", body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("second create: status = %d, want 409", rec.Code)
	}
	if got := decodeError(t, rec); got.Code != "alias_taken" {
		t.Errorf("code = %q, want alias_taken", got.Code)
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
	ctx := context.Background()
	for _, l := range []link.Link{
		{Key: "forever", LongURL: "https://example.com/forever"},
		{Key: "soon123", LongURL: "https://example.com/soon", ExpiresAt: &soon},
		{Key: "expired", LongURL: "https://example.com/expired", ExpiresAt: &past},
		{Key: "deleted", LongURL: "https://example.com/deleted", DeletedAt: &past},
	} {
		if err := store.Create(ctx, l); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		path         string
		wantStatus   int
		wantLocation string
		wantCache    string
	}{
		{"/forever", http.StatusMovedPermanently, "https://example.com/forever", "private, max-age=90"},
		{"/soon123", http.StatusMovedPermanently, "https://example.com/soon", "private, max-age=30"},
		{"/missing", http.StatusNotFound, "", ""},
		{"/expired", http.StatusGone, "", ""},
		{"/deleted", http.StatusGone, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			rec := do(h, http.MethodGet, tt.path, "")
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
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

func TestCreateThenRedirect(t *testing.T) {
	h, _ := newTestHandler(t)
	rec := do(h, http.MethodPost, "/api/v1/links", `{"long_url":"https://example.com/x","alias":"mylink"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d", rec.Code)
	}
	rec = do(h, http.MethodGet, "/mylink", "")
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "https://example.com/x" {
		t.Errorf("redirect: status = %d, Location = %q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestHealthz(t *testing.T) {
	h, _ := newTestHandler(t)
	if rec := do(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
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
		{"no expiry", nil, 90},
		{"expires later", at(time.Hour), 90},
		{"expires sooner", at(10 * time.Second), 10},
		{"sub-second", at(500 * time.Millisecond), 0},
		{"already expired", at(-time.Second), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redirectCacheAge(link.Link{ExpiresAt: tt.expiresAt}, now); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}
