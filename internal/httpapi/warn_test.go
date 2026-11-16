package httpapi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/breaker"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

func TestWarnRedis(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantLog bool
	}{
		{name: "redis failure is logged", err: errors.New("i/o timeout"), wantLog: true},
		{name: "open breaker is not logged", err: breaker.ErrOpen, wantLog: false},
		{name: "wrapped open breaker is not logged", err: fmt.Errorf("get: %w", breaker.ErrOpen), wantLog: false},
		{name: "failed probe is not logged", err: fmt.Errorf("%w: %w", breaker.ErrProbeFailed, errors.New("i/o timeout")), wantLog: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := &Handler{logger: slog.New(slog.NewTextHandler(&buf, nil))}
			h.warnRedis("rate limiter unavailable", tt.err)
			if got := buf.Len() > 0; got != tt.wantLog {
				t.Errorf("logged = %v, want %v; log: %s", got, tt.wantLog, buf.String())
			}
		})
	}
}

// Regression: the breaker opening during a request dropped the write of its
// idempotent response, so the key stayed pending and retries got 409.
func TestIdempotencyWriteBypassesBreaker(t *testing.T) {
	tests := []struct {
		name         string
		openDuring   bool // the breaker opens while the link is being created
		wantReplayed bool
	}{
		{name: "breaker stays closed", openDuring: false, wantReplayed: true},
		{name: "breaker opens during the request", openDuring: true, wantReplayed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			cb := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Hour})
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			rdb.AddHook(breaker.Hook(cb))
			t.Cleanup(func() { _ = rdb.Close() })
			store := &cancelingStore{Store: memory.New(), cancel: func() {}}
			if tt.openDuring {
				store.cancel = func() { cb.Allow(); cb.Done(breaker.Failure) }
			}
			h := New(Config{
				Links:       link.NewService(store, "sho.rt"),
				Accounts:    account.NewService(memory.NewAccounts()),
				Idempotency: idempotency.New(rdb, idempotency.DefaultTTL),
				BaseURL:     "https://sho.rt",
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			post := func() *httptest.ResponseRecorder {
				req := httptest.NewRequest("POST", "/api/v1/links", strings.NewReader(`{"long_url":"https://example.com"}`))
				req.Header.Set("Idempotency-Key", "k1")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				return rec
			}
			if rec := post(); rec.Code != http.StatusCreated {
				t.Fatalf("first POST status = %d, body: %s", rec.Code, rec.Body)
			}
			// Redis is back: a retry must see the stored response.
			store.cancel = func() {}
			cb.Allow()
			cb.Done(breaker.Success)
			rec := post()
			if got := rec.Header().Get("Idempotent-Replayed") == "true"; got != tt.wantReplayed {
				t.Errorf("retry: status %d, Idempotent-Replayed = %v, want %v", rec.Code, got, tt.wantReplayed)
			}
		})
	}
}
