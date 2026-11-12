package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/ratelimit"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

// newFullHandler returns a handler with rate limiting and idempotency backed
// by an in-process Redis.
func newFullHandler(t *testing.T, anonymousLimit int) *Handler {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(Config{
		Links:          link.NewService(memory.New(), "sho.rt"),
		Accounts:       account.NewService(memory.NewAccounts()),
		Limiter:        ratelimit.New(rdb),
		Idempotency:    idempotency.New(rdb, idempotency.DefaultTTL),
		BaseURL:        "https://sho.rt",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		AnonymousLimit: anonymousLimit,
		UserLimit:      1000,
	})
}

type request struct {
	method, path, body string
	apiKey             string
	idempotencyKey     string
	remoteAddr         string
}

func send(h http.Handler, r request) *httptest.ResponseRecorder {
	req := httptest.NewRequest(r.method, r.path, strings.NewReader(r.body))
	if r.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.apiKey)
	}
	if r.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", r.idempotencyKey)
	}
	if r.remoteAddr != "" {
		req.RemoteAddr = r.remoteAddr
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func registerUser(t *testing.T, h http.Handler, email string) string {
	t.Helper()
	rec := send(h, request{method: "POST", path: "/api/v1/users", body: `{"email":"` + email + `"}`})
	if rec.Code != http.StatusCreated {
		t.Fatalf("register %s: status = %d, body: %s", email, rec.Code, rec.Body)
	}
	var resp registerResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.APIKey, "sk_") || resp.UserID == "" {
		t.Fatalf("register response = %+v", resp)
	}
	return resp.APIKey
}

func createLink(t *testing.T, h http.Handler, apiKey, body string) linkResponse {
	t.Helper()
	rec := send(h, request{method: "POST", path: "/api/v1/links", body: body, apiKey: apiKey})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, body: %s", rec.Code, rec.Body)
	}
	var l linkResponse
	if err := json.NewDecoder(rec.Body).Decode(&l); err != nil {
		t.Fatal(err)
	}
	return l
}

func TestRegister(t *testing.T) {
	h, _ := newTestHandler(t)
	registerUser(t, h, "alice@example.com")

	tests := []struct {
		body       string
		wantStatus int
		wantCode   string
	}{
		{`{"email":"Alice@Example.com"}`, http.StatusConflict, "email_taken"},
		{`{"email":"not-an-email"}`, http.StatusBadRequest, "invalid_email"},
		{`{"email":"Bob <bob@example.com>"}`, http.StatusBadRequest, "invalid_email"},
	}
	for _, tt := range tests {
		rec := send(h, request{method: "POST", path: "/api/v1/users", body: tt.body})
		if rec.Code != tt.wantStatus || decodeError(t, rec).Code != tt.wantCode {
			t.Errorf("%s: status = %d, want %d %s", tt.body, rec.Code, tt.wantStatus, tt.wantCode)
		}
	}
}

func TestAuth(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")

	tests := []struct {
		name       string
		req        request
		wantStatus int
		wantCode   string
	}{
		{"list without key", request{method: "GET", path: "/api/v1/links"}, http.StatusUnauthorized, "unauthorized"},
		{"list with wrong key", request{method: "GET", path: "/api/v1/links", apiKey: "sk_wrong"}, http.StatusUnauthorized, "invalid_api_key"},
		{"list with garbage key", request{method: "GET", path: "/api/v1/links", apiKey: "garbage"}, http.StatusUnauthorized, "invalid_api_key"},
		{"create with wrong key", request{method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com"}`, apiKey: "sk_wrong"}, http.StatusUnauthorized, "invalid_api_key"},
		{"list with key", request{method: "GET", path: "/api/v1/links", apiKey: key}, http.StatusOK, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := send(h, tt.req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body)
			}
			if tt.wantCode == "" {
				return
			}
			if got := decodeError(t, rec).Code; got != tt.wantCode {
				t.Errorf("code = %q, want %q", got, tt.wantCode)
			}
			if rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Error("missing WWW-Authenticate header")
			}
		})
	}

	req := httptest.NewRequest("GET", "/api/v1/links", nil)
	req.Header.Set("Authorization", "Basic "+key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("non-Bearer scheme: status = %d, want 401", rec.Code)
	}
}

func TestOwnership(t *testing.T) {
	h, _ := newTestHandler(t)
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	owned := createLink(t, h, alice, `{"long_url":"https://example.com/alice"}`)
	anon := createLink(t, h, "", `{"long_url":"https://example.com/anon"}`)

	if rec := send(h, request{method: "GET", path: "/api/v1/links/" + owned.Key, apiKey: alice}); rec.Code != http.StatusOK {
		t.Errorf("owner GET: status = %d", rec.Code)
	}
	for _, r := range []request{
		{method: "GET", path: "/api/v1/links/" + owned.Key, apiKey: bob},
		{method: "PATCH", path: "/api/v1/links/" + owned.Key, apiKey: bob, body: `{"long_url":"https://evil.example"}`},
		{method: "DELETE", path: "/api/v1/links/" + owned.Key, apiKey: bob},
		{method: "GET", path: "/api/v1/links/" + anon.Key, apiKey: alice},
		{method: "DELETE", path: "/api/v1/links/" + anon.Key, apiKey: alice},
		{method: "GET", path: "/api/v1/links/missing1", apiKey: alice},
	} {
		if rec := send(h, r); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s by another user: status = %d, want 404", r.method, r.path, rec.Code)
		}
	}
	// Bob's attempts changed nothing.
	if rec := send(h, request{method: "GET", path: "/" + owned.Key}); rec.Header().Get("Location") != "https://example.com/alice" {
		t.Errorf("link was modified by another user: Location = %q", rec.Header().Get("Location"))
	}
}

func TestPatch(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")
	l := createLink(t, h, key, `{"long_url":"https://example.com/old","expires_at":"2099-01-01T00:00:00Z"}`)
	path := "/api/v1/links/" + l.Key

	rec := send(h, request{method: "PATCH", path: path, apiKey: key, body: `{"long_url":"https://example.com/new"}`})
	if rec.Code != http.StatusOK {
		t.Fatalf("PATCH: status = %d, body: %s", rec.Code, rec.Body)
	}
	var got linkResponse
	_ = json.NewDecoder(rec.Body).Decode(&got)
	if got.LongURL != "https://example.com/new" || got.ExpiresAt == nil {
		t.Errorf("after PATCH long_url: %+v (expires_at must stay)", got)
	}
	if loc := send(h, request{method: "GET", path: "/" + l.Key}).Header().Get("Location"); loc != "https://example.com/new" {
		t.Errorf("redirect Location = %q after PATCH", loc)
	}

	rec = send(h, request{method: "PATCH", path: path, apiKey: key, body: `{"expires_at":null}`})
	got = linkResponse{}
	_ = json.NewDecoder(rec.Body).Decode(&got)
	if rec.Code != http.StatusOK || got.ExpiresAt != nil || got.LongURL != "https://example.com/new" {
		t.Errorf("after PATCH expires_at:null: status %d, %+v", rec.Code, got)
	}

	for _, tt := range []struct{ body, wantCode string }{
		{`{"key":"other"}`, "invalid_request"},
		{`{"long_url":null}`, "invalid_request"},
		{`{"long_url":42}`, "invalid_request"},
		{`{"long_url":"ftp://example.com"}`, "invalid_url"},
		{`{"expires_at":"2000-01-01T00:00:00Z"}`, "invalid_expires_at"},
		{`not json`, "invalid_request"},
	} {
		rec := send(h, request{method: "PATCH", path: path, apiKey: key, body: tt.body})
		if rec.Code != http.StatusBadRequest || decodeError(t, rec).Code != tt.wantCode {
			t.Errorf("PATCH %s: status = %d, want 400 %s", tt.body, rec.Code, tt.wantCode)
		}
	}
}

func TestDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")
	l := createLink(t, h, key, `{"long_url":"https://example.com","alias":"deleteme"}`)

	for i := range 2 {
		if rec := send(h, request{method: "DELETE", path: "/api/v1/links/deleteme", apiKey: key}); rec.Code != http.StatusNoContent {
			t.Fatalf("DELETE #%d: status = %d, want 204", i+1, rec.Code)
		}
	}
	if rec := send(h, request{method: "GET", path: "/" + l.Key}); rec.Code != http.StatusGone {
		t.Errorf("redirect after delete: status = %d, want 410", rec.Code)
	}
	if rec := send(h, request{method: "GET", path: "/api/v1/links/deleteme", apiKey: key}); rec.Code != http.StatusNotFound {
		t.Errorf("GET after delete: status = %d, want 404", rec.Code)
	}
	// The key is never reused.
	rec := send(h, request{method: "POST", path: "/api/v1/links", apiKey: key, body: `{"long_url":"https://example.com","alias":"deleteme"}`})
	if rec.Code != http.StatusConflict {
		t.Errorf("reuse deleted alias: status = %d, want 409", rec.Code)
	}
}

func TestListPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	createLink(t, h, bob, `{"long_url":"https://example.com/bob"}`)

	const total = 5
	for range total {
		createLink(t, h, alice, `{"long_url":"https://example.com/alice"}`)
	}
	deleted := createLink(t, h, alice, `{"long_url":"https://example.com/deleted"}`)
	send(h, request{method: "DELETE", path: "/api/v1/links/" + deleted.Key, apiKey: alice})

	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		rec := send(h, request{method: "GET", path: "/api/v1/links?limit=2&cursor=" + cursor, apiKey: alice})
		if rec.Code != http.StatusOK {
			t.Fatalf("list: status = %d, body: %s", rec.Code, rec.Body)
		}
		var page listResponse
		_ = json.NewDecoder(rec.Body).Decode(&page)
		pages++
		for _, l := range page.Items {
			if seen[l.Key] || l.LongURL != "https://example.com/alice" {
				t.Errorf("unexpected item %+v (duplicate, deleted or another user's)", l)
			}
			seen[l.Key] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > total {
			t.Fatal("pagination does not terminate")
		}
	}
	if len(seen) != total || pages != 3 {
		t.Errorf("listed %d links in %d pages, want %d in 3", len(seen), pages, total)
	}

	for _, q := range []string{"limit=0", "limit=101", "limit=abc"} {
		if rec := send(h, request{method: "GET", path: "/api/v1/links?" + q, apiKey: alice}); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
	}
	// A well-formed query parameter that is not a valid cursor.
	rec := send(h, request{method: "GET", path: "/api/v1/links?cursor=not-a-cursor", apiKey: alice})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad cursor: status = %d, want 400", rec.Code)
	}
}

func TestRateLimit(t *testing.T) {
	h := newFullHandler(t, 3)
	create := request{method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com"}`, remoteAddr: "203.0.113.7:5000"}

	for i := 1; i <= 3; i++ {
		rec := send(h, create)
		if rec.Code != http.StatusCreated {
			t.Fatalf("request %d: status = %d", i, rec.Code)
		}
		if got := rec.Header().Get("X-RateLimit-Remaining"); got != strconv.Itoa(3-i) {
			t.Errorf("request %d: X-RateLimit-Remaining = %q, want %d", i, got, 3-i)
		}
		if rec.Header().Get("X-RateLimit-Limit") != "3" || rec.Header().Get("X-RateLimit-Reset") == "" {
			t.Errorf("request %d: rate limit headers = %v", i, rec.Header())
		}
	}

	rec := send(h, create)
	if rec.Code != http.StatusTooManyRequests || decodeError(t, rec).Code != "rate_limited" {
		t.Fatalf("over limit: status = %d, want 429", rec.Code)
	}
	if s, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || s < 1 || s > 60 {
		t.Errorf("Retry-After = %q, want 1..60 seconds", rec.Header().Get("Retry-After"))
	}

	// Another client has its own quota.
	other := create
	other.remoteAddr = "198.51.100.1:5000"
	if rec := send(h, other); rec.Code != http.StatusCreated {
		t.Errorf("another IP: status = %d, want 201", rec.Code)
	}
	// Redirects are not rate limited.
	if rec := send(h, request{method: "GET", path: "/missing1", remoteAddr: "203.0.113.7:5000"}); rec.Code != http.StatusNotFound {
		t.Errorf("redirect over the API limit: status = %d, want 404", rec.Code)
	}
}

// Guessing API keys must count against the client's IP quota.
func TestRateLimitInvalidKeys(t *testing.T) {
	h := newFullHandler(t, 2)
	guess := request{method: "GET", path: "/api/v1/links", apiKey: "sk_guess", remoteAddr: "203.0.113.7:5000"}
	for range 2 {
		if rec := send(h, guess); rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	}
	if rec := send(h, guess); rec.Code != http.StatusTooManyRequests {
		t.Errorf("3rd guess: status = %d, want 429", rec.Code)
	}
}

func TestIdempotency(t *testing.T) {
	h := newFullHandler(t, 100)
	key := registerUser(t, h, "alice@example.com")
	create := request{method: "POST", path: "/api/v1/links", apiKey: key, idempotencyKey: "req-1",
		body: `{"long_url":"https://example.com/once"}`}

	first := send(h, create)
	retry := send(h, create)
	if first.Code != http.StatusCreated || retry.Code != http.StatusCreated {
		t.Fatalf("statuses = %d, %d, want 201, 201", first.Code, retry.Code)
	}
	if first.Body.String() != retry.Body.String() {
		t.Errorf("retry returned a different response:\n%s\n%s", first.Body, retry.Body)
	}
	if retry.Header().Get("Idempotent-Replayed") != "true" || first.Header().Get("Idempotent-Replayed") != "" {
		t.Error("Idempotent-Replayed header must be set on the replay only")
	}

	var page listResponse
	_ = json.NewDecoder(send(h, request{method: "GET", path: "/api/v1/links", apiKey: key}).Body).Decode(&page)
	if len(page.Items) != 1 {
		t.Errorf("%d links created, want 1", len(page.Items))
	}

	reused := create
	reused.body = `{"long_url":"https://example.com/other"}`
	if rec := send(h, reused); rec.Code != http.StatusUnprocessableEntity || decodeError(t, rec).Code != "idempotency_key_reused" {
		t.Errorf("same key, other body: status = %d, want 422", rec.Code)
	}

	// Keys are scoped per user: another user's "req-1" is independent.
	bob := registerUser(t, h, "bob@example.com")
	other := create
	other.apiKey = bob
	rec := send(h, other)
	if rec.Code != http.StatusCreated || rec.Header().Get("Idempotent-Replayed") != "" {
		t.Errorf("another user, same key: status = %d, replayed = %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}

	// Client errors are replayed too: they are deterministic.
	bad := request{method: "POST", path: "/api/v1/links", apiKey: key, idempotencyKey: "req-bad", body: `{"long_url":"ftp://x"}`}
	send(h, bad)
	if rec := send(h, bad); rec.Code != http.StatusBadRequest || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("replayed 400: status = %d, replayed = %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
}
