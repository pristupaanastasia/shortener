package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/ratelimit"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

// newFullHandler returns a handler with rate limiting and idempotency backed
// by an in-process Redis. The limiter's clock is fixed in the middle of a
// window, so a test never straddles a window boundary.
func newFullHandler(t *testing.T, anonymousLimit int) *Handler {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	fixed := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
	h := New(Config{
		Links:          link.NewService(memory.New(), "sho.rt"),
		Accounts:       account.NewService(memory.NewAccounts()),
		Limiter:        ratelimit.New(rdb, ratelimit.WithClock(func() time.Time { return fixed })),
		Idempotency:    idempotency.New(rdb, idempotency.DefaultTTL),
		BaseURL:        "https://sho.rt",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		AnonymousLimit: anonymousLimit,
		UserLimit:      1000,
	})
	h.now = func() time.Time { return fixed } // for Retry-After
	return h
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

// apiStep is one request in a stateful API scenario.
type apiStep struct {
	name       string
	req        func() request // built lazily: it may use values from earlier steps
	wantStatus int
	wantCode   string // error code, for error responses
	check      func(t *testing.T, rec *httptest.ResponseRecorder)
}

func runAPISteps(t *testing.T, h http.Handler, steps []apiStep) {
	t.Helper()
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			r := step.req()
			rec := send(h, r)
			if rec.Code != step.wantStatus {
				t.Fatalf("%s %s: status = %d, want %d; body: %s", r.method, r.path, rec.Code, step.wantStatus, rec.Body)
			}
			if step.wantCode != "" {
				if got := decodeError(t, rec).Code; got != step.wantCode {
					t.Errorf("error code = %q, want %q", got, step.wantCode)
				}
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

func static(r request) func() request { return func() request { return r } }

func TestRegister(t *testing.T) {
	h, _ := newTestHandler(t)
	register := func(body string) func() request {
		return static(request{method: "POST", path: "/api/v1/users", body: body})
	}
	runAPISteps(t, h, []apiStep{
		{
			name: "new user", req: register(`{"email":"alice@example.com"}`), wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				var resp registerResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(resp.APIKey, "sk_") || resp.UserID == "" || resp.Email != "alice@example.com" {
					t.Errorf("response = %+v", resp)
				}
			},
		},
		{name: "same email in another case", req: register(`{"email":"Alice@Example.com"}`), wantStatus: http.StatusConflict, wantCode: "email_taken"},
		{name: "not an email", req: register(`{"email":"not-an-email"}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_email"},
		{name: "email with display name", req: register(`{"email":"Bob <bob@example.com>"}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_email"},
		{name: "unknown field", req: register(`{"email":"bob@example.com","admin":true}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
	})
}

func TestAuth(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")

	tests := []struct {
		name          string
		method        string
		path          string
		body          string
		authorization string
		wantStatus    int
		wantCode      string
	}{
		{name: "list without key", method: "GET", path: "/api/v1/links", wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "list with wrong key", method: "GET", path: "/api/v1/links", authorization: "Bearer sk_wrong", wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "list with garbage key", method: "GET", path: "/api/v1/links", authorization: "Bearer garbage", wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "non-bearer scheme", method: "GET", path: "/api/v1/links", authorization: "Basic " + key, wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "create with wrong key", method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com"}`, authorization: "Bearer sk_wrong", wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "create without key is anonymous", method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com"}`, wantStatus: http.StatusCreated},
		{name: "list with key", method: "GET", path: "/api/v1/links", authorization: "Bearer " + key, wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
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
				t.Error("missing WWW-Authenticate: Bearer")
			}
		})
	}
}

func TestOwnership(t *testing.T) {
	h, _ := newTestHandler(t)
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	owned := createLink(t, h, alice, `{"long_url":"https://example.com/alice"}`)
	anon := createLink(t, h, "", `{"long_url":"https://example.com/anon"}`)

	tests := []struct {
		name       string
		req        request
		wantStatus int
	}{
		{name: "owner reads", req: request{method: "GET", path: "/api/v1/links/" + owned.Key, apiKey: alice}, wantStatus: http.StatusOK},
		{name: "other user reads", req: request{method: "GET", path: "/api/v1/links/" + owned.Key, apiKey: bob}, wantStatus: http.StatusNotFound},
		{name: "other user patches", req: request{method: "PATCH", path: "/api/v1/links/" + owned.Key, apiKey: bob, body: `{"long_url":"https://evil.example"}`}, wantStatus: http.StatusNotFound},
		{name: "other user deletes", req: request{method: "DELETE", path: "/api/v1/links/" + owned.Key, apiKey: bob}, wantStatus: http.StatusNotFound},
		{name: "anonymous link cannot be read", req: request{method: "GET", path: "/api/v1/links/" + anon.Key, apiKey: alice}, wantStatus: http.StatusNotFound},
		{name: "anonymous link cannot be deleted", req: request{method: "DELETE", path: "/api/v1/links/" + anon.Key, apiKey: alice}, wantStatus: http.StatusNotFound},
		{name: "missing link", req: request{method: "GET", path: "/api/v1/links/missing1", apiKey: alice}, wantStatus: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := send(h, tt.req); rec.Code != tt.wantStatus {
				t.Errorf("%s %s: status = %d, want %d", tt.req.method, tt.req.path, rec.Code, tt.wantStatus)
			}
			// Nothing above may change Alice's link.
			if loc := send(h, request{method: "GET", path: "/" + owned.Key}).Header().Get("Location"); loc != "https://example.com/alice" {
				t.Errorf("link was modified: Location = %q", loc)
			}
		})
	}
}

func TestPatch(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")
	l := createLink(t, h, key, `{"long_url":"https://example.com/old","expires_at":"2099-01-01T00:00:00Z"}`)
	patch := func(body string) func() request {
		return static(request{method: "PATCH", path: "/api/v1/links/" + l.Key, apiKey: key, body: body})
	}
	wantLink := func(wantURL string, wantExpires bool) func(*testing.T, *httptest.ResponseRecorder) {
		return func(t *testing.T, rec *httptest.ResponseRecorder) {
			var got linkResponse
			_ = json.NewDecoder(rec.Body).Decode(&got)
			if got.LongURL != wantURL || (got.ExpiresAt != nil) != wantExpires {
				t.Errorf("link = %+v, want long_url %s, has expiry %v", got, wantURL, wantExpires)
			}
		}
	}

	runAPISteps(t, h, []apiStep{
		{name: "change url keeps expiry", req: patch(`{"long_url":"https://example.com/new"}`), wantStatus: http.StatusOK, check: wantLink("https://example.com/new", true)},
		{
			name: "redirect follows the new url", req: static(request{method: "GET", path: "/" + l.Key}), wantStatus: http.StatusMovedPermanently,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				if loc := rec.Header().Get("Location"); loc != "https://example.com/new" {
					t.Errorf("Location = %q", loc)
				}
			},
		},
		{name: "null expiry makes the link permanent", req: patch(`{"expires_at":null}`), wantStatus: http.StatusOK, check: wantLink("https://example.com/new", false)},
		{name: "key cannot be changed", req: patch(`{"key":"other"}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "url cannot be null", req: patch(`{"long_url":null}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "url must be a string", req: patch(`{"long_url":42}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "invalid url", req: patch(`{"long_url":"ftp://example.com"}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_url"},
		{name: "expiry in past", req: patch(`{"expires_at":"2000-01-01T00:00:00Z"}`), wantStatus: http.StatusBadRequest, wantCode: "invalid_expires_at"},
		{name: "not json", req: patch(`not json`), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "rejected patches changed nothing", req: static(request{method: "GET", path: "/api/v1/links/" + l.Key, apiKey: key}), wantStatus: http.StatusOK, check: wantLink("https://example.com/new", false)},
	})
}

func TestDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")
	del := static(request{method: "DELETE", path: "/api/v1/links/deleteme", apiKey: key})

	runAPISteps(t, h, []apiStep{
		{name: "create", req: static(request{method: "POST", path: "/api/v1/links", apiKey: key, body: `{"long_url":"https://example.com","alias":"deleteme"}`}), wantStatus: http.StatusCreated},
		{name: "delete", req: del, wantStatus: http.StatusNoContent},
		{name: "delete again is idempotent", req: del, wantStatus: http.StatusNoContent},
		{name: "redirect is gone", req: static(request{method: "GET", path: "/deleteme"}), wantStatus: http.StatusGone},
		{name: "deleted link is hidden", req: static(request{method: "GET", path: "/api/v1/links/deleteme", apiKey: key}), wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "key is never reused", req: static(request{method: "POST", path: "/api/v1/links", apiKey: key, body: `{"long_url":"https://example.com","alias":"deleteme"}`}), wantStatus: http.StatusConflict, wantCode: "alias_taken"},
	})
}

func TestListPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	createLink(t, h, bob, `{"long_url":"https://example.com/bob"}`)
	for range 5 {
		createLink(t, h, alice, `{"long_url":"https://example.com/alice"}`)
	}
	deleted := createLink(t, h, alice, `{"long_url":"https://example.com/deleted"}`)
	send(h, request{method: "DELETE", path: "/api/v1/links/" + deleted.Key, apiKey: alice})

	tests := []struct {
		name      string
		limit     string
		wantPages int
		wantTotal int
	}{
		{name: "pages of two", limit: "2", wantPages: 3, wantTotal: 5},
		{name: "pages of five", limit: "5", wantPages: 1, wantTotal: 5},
		{name: "default page size", limit: "", wantPages: 1, wantTotal: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := map[string]bool{}
			cursor, pages := "", 0
			for pages <= tt.wantPages {
				rec := send(h, request{method: "GET", path: "/api/v1/links?limit=" + tt.limit + "&cursor=" + cursor, apiKey: alice})
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
			}
			if len(seen) != tt.wantTotal || pages != tt.wantPages {
				t.Errorf("listed %d links in %d pages, want %d in %d", len(seen), pages, tt.wantTotal, tt.wantPages)
			}
		})
	}
}

func TestListErrors(t *testing.T) {
	h, _ := newTestHandler(t)
	key := registerUser(t, h, "alice@example.com")

	tests := []struct {
		name     string
		query    string
		wantCode string
	}{
		{name: "zero limit", query: "limit=0", wantCode: "invalid_request"},
		{name: "limit above max", query: "limit=101", wantCode: "invalid_request"},
		{name: "limit not a number", query: "limit=abc", wantCode: "invalid_request"},
		{name: "invalid cursor", query: "cursor=not-a-cursor", wantCode: "invalid_cursor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := send(h, request{method: "GET", path: "/api/v1/links?" + tt.query, apiKey: key})
			if rec.Code != http.StatusBadRequest || decodeError(t, rec).Code != tt.wantCode {
				t.Errorf("GET ?%s: status = %d, want 400 %s", tt.query, rec.Code, tt.wantCode)
			}
		})
	}
}

func TestRateLimit(t *testing.T) {
	h := newFullHandler(t, 3)
	const client = "203.0.113.7:5000"
	create := request{method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com"}`, remoteAddr: client}
	wantRemaining := func(n string) func(*testing.T, *httptest.ResponseRecorder) {
		return func(t *testing.T, rec *httptest.ResponseRecorder) {
			if got := rec.Header().Get("X-RateLimit-Remaining"); got != n {
				t.Errorf("X-RateLimit-Remaining = %q, want %s", got, n)
			}
			if rec.Header().Get("X-RateLimit-Limit") != "3" || rec.Header().Get("X-RateLimit-Reset") == "" {
				t.Errorf("rate limit headers = %v", rec.Header())
			}
		}
	}
	otherClient := create
	otherClient.remoteAddr = "198.51.100.1:5000"

	runAPISteps(t, h, []apiStep{
		{name: "1st request", req: static(create), wantStatus: http.StatusCreated, check: wantRemaining("2")},
		{name: "2nd request", req: static(create), wantStatus: http.StatusCreated, check: wantRemaining("1")},
		{name: "3rd request", req: static(create), wantStatus: http.StatusCreated, check: wantRemaining("0")},
		{
			name: "4th request is limited", req: static(create), wantStatus: http.StatusTooManyRequests, wantCode: "rate_limited",
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				if s, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || s < 1 || s > 60 {
					t.Errorf("Retry-After = %q, want 1..60 seconds", rec.Header().Get("Retry-After"))
				}
			},
		},
		{name: "another client has its own quota", req: static(otherClient), wantStatus: http.StatusCreated},
		{name: "redirects are not limited", req: static(request{method: "GET", path: "/missing1", remoteAddr: client}), wantStatus: http.StatusNotFound},
	})
}

// Guessing API keys must count against the client's IP quota.
func TestRateLimitInvalidKeys(t *testing.T) {
	h := newFullHandler(t, 2)
	guess := static(request{method: "GET", path: "/api/v1/links", apiKey: "sk_guess", remoteAddr: "203.0.113.7:5000"})
	runAPISteps(t, h, []apiStep{
		{name: "1st guess", req: guess, wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "2nd guess", req: guess, wantStatus: http.StatusUnauthorized, wantCode: "invalid_api_key"},
		{name: "3rd guess is limited", req: guess, wantStatus: http.StatusTooManyRequests, wantCode: "rate_limited"},
	})
}

func TestIdempotency(t *testing.T) {
	h := newFullHandler(t, 100)
	alice := registerUser(t, h, "alice@example.com")
	bob := registerUser(t, h, "bob@example.com")
	create := request{method: "POST", path: "/api/v1/links", apiKey: alice, idempotencyKey: "req-1", body: `{"long_url":"https://example.com/once"}`}
	var firstBody string
	replayed := func(want bool) func(*testing.T, *httptest.ResponseRecorder) {
		return func(t *testing.T, rec *httptest.ResponseRecorder) {
			if got := rec.Header().Get("Idempotent-Replayed") == "true"; got != want {
				t.Errorf("Idempotent-Replayed = %v, want %v", got, want)
			}
		}
	}
	reused, otherUser, bad := create, create, request{method: "POST", path: "/api/v1/links", apiKey: alice, idempotencyKey: "req-bad", body: `{"long_url":"ftp://x"}`}
	reused.body = `{"long_url":"https://example.com/other"}`
	otherUser.apiKey = bob

	runAPISteps(t, h, []apiStep{
		{
			name: "first request runs", req: static(create), wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				replayed(false)(t, rec)
				firstBody = rec.Body.String()
			},
		},
		{
			name: "retry replays the response", req: static(create), wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				replayed(true)(t, rec)
				if rec.Body.String() != firstBody {
					t.Errorf("replayed body = %s, want %s", rec.Body, firstBody)
				}
			},
		},
		{
			name: "only one link was created", req: static(request{method: "GET", path: "/api/v1/links", apiKey: alice}), wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				var page listResponse
				_ = json.NewDecoder(rec.Body).Decode(&page)
				if len(page.Items) != 1 {
					t.Errorf("%d links, want 1", len(page.Items))
				}
			},
		},
		{name: "same key with another body", req: static(reused), wantStatus: http.StatusUnprocessableEntity, wantCode: "idempotency_key_reused"},
		{name: "keys are scoped per user", req: static(otherUser), wantStatus: http.StatusCreated, check: replayed(false)},
		{name: "client error is stored", req: static(bad), wantStatus: http.StatusBadRequest, check: replayed(false)},
		{name: "client error is replayed", req: static(bad), wantStatus: http.StatusBadRequest, check: replayed(true)},
	})
}

// cancelingStore cancels the request context right after a successful create,
// like a client that times out and disconnects while the link is being stored.
type cancelingStore struct {
	*memory.Store
	cancel context.CancelFunc
}

func (s *cancelingStore) Create(ctx context.Context, l link.Link) error {
	err := s.Store.Create(ctx, l)
	s.cancel()
	return err
}

// Regression: the response was stored with the request context, so after a
// disconnect the key stayed "in progress" and every retry got 409 for 24h.
func TestIdempotencyAfterClientDisconnect(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := &cancelingStore{Store: memory.New()}
	h := New(Config{
		Links:       link.NewService(store, "sho.rt"),
		Accounts:    account.NewService(memory.NewAccounts()),
		Limiter:     ratelimit.New(rdb),
		Idempotency: idempotency.New(rdb, idempotency.DefaultTTL),
		BaseURL:     "https://sho.rt",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	tests := []struct {
		name         string
		disconnect   bool
		wantStatus   int
		wantReplayed bool
	}{
		{name: "client disconnects after the link is stored", disconnect: true, wantStatus: http.StatusCreated},
		{name: "retry gets the original response", wantStatus: http.StatusCreated, wantReplayed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store.cancel = func() {}
			if tt.disconnect {
				store.cancel = cancel
			}
			req := httptest.NewRequestWithContext(ctx, "POST", "/api/v1/links", strings.NewReader(`{"long_url":"https://example.com"}`))
			req.Header.Set("Idempotency-Key", "disconnect-1")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body)
			}
			if got := rec.Header().Get("Idempotent-Replayed") == "true"; got != tt.wantReplayed {
				t.Errorf("Idempotent-Replayed = %v, want %v", got, tt.wantReplayed)
			}
		})
	}
}

// countingAccounts counts database lookups of API keys.
type countingAccounts struct {
	*memory.Accounts
	lookups atomic.Int32
}

func (a *countingAccounts) UserByKeyHash(ctx context.Context, hash []byte) (account.User, error) {
	a.lookups.Add(1)
	return a.Accounts.UserByKeyHash(ctx, hash)
}

// Regression: every request looked the key up in the database, even guesses
// that were already rate limited.
func TestKeyLookups(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	accounts := &countingAccounts{Accounts: memory.NewAccounts()}
	fixed := time.Date(2026, 11, 12, 10, 0, 30, 0, time.UTC)
	h := New(Config{
		Links:          link.NewService(memory.New(), "sho.rt"),
		Accounts:       account.NewService(accounts),
		Limiter:        ratelimit.New(rdb, ratelimit.WithClock(func() time.Time { return fixed })),
		BaseURL:        "https://sho.rt",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		AnonymousLimit: 2,
		UserLimit:      100,
	})
	key := registerUser(t, h, "alice@example.com")
	list := func(apiKey, ip string) request {
		return request{method: "GET", path: "/api/v1/links", apiKey: apiKey, remoteAddr: ip + ":5000"}
	}

	steps := []struct {
		name        string
		req         request
		wantStatus  int
		wantLookups int32 // total so far
	}{
		{name: "valid key is looked up once", req: list(key, "198.51.100.1"), wantStatus: http.StatusOK, wantLookups: 1},
		{name: "then served from the cache", req: list(key, "198.51.100.1"), wantStatus: http.StatusOK, wantLookups: 1},
		{name: "cached key is not limited by the ip quota", req: list(key, "198.51.100.1"), wantStatus: http.StatusOK, wantLookups: 1},
		{name: "1st guess is looked up", req: list("sk_guess1", "203.0.113.7"), wantStatus: http.StatusUnauthorized, wantLookups: 2},
		{name: "2nd guess is looked up", req: list("sk_guess2", "203.0.113.7"), wantStatus: http.StatusUnauthorized, wantLookups: 3},
		{name: "rate limited guess costs no lookup", req: list("sk_guess3", "203.0.113.7"), wantStatus: http.StatusTooManyRequests, wantLookups: 3},
		{name: "another rate limited guess costs no lookup", req: list("sk_guess4", "203.0.113.7"), wantStatus: http.StatusTooManyRequests, wantLookups: 3},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if rec := send(h, step.req); rec.Code != step.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, step.wantStatus, rec.Body)
			}
			if got := accounts.lookups.Load(); got != step.wantLookups {
				t.Errorf("database lookups = %d, want %d", got, step.wantLookups)
			}
		})
		if !ok {
			break
		}
	}
}
