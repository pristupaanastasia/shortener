package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
)

type authMode int

const (
	authOptional authMode = iota
	authRequired
)

type ctxKey int

const (
	userKey    ctxKey = iota // authenticated user ID
	subjectKey               // rate limit and idempotency scope
)

func userFrom(ctx context.Context) string {
	id, _ := ctx.Value(userKey).(string)
	return id
}

func subjectFrom(ctx context.Context) string {
	s, _ := ctx.Value(subjectKey).(string)
	return s
}

// api wraps an API handler with authentication and rate limiting.
//
// The rate limit is applied before rejecting an invalid API key: otherwise
// guessing keys would be unlimited. Requests are counted per user when the key
// is valid, and per client IP otherwise.
func (h *Handler) api(next http.HandlerFunc, mode authMode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, authErr := h.authenticate(r)

		subject, limit := "ip:"+clientIP(r), h.anonymousLimit
		if userID != "" {
			subject, limit = "user:"+userID, h.userLimit
		}
		if !h.checkRateLimit(w, r, subject, limit) {
			return
		}

		switch {
		case errors.Is(authErr, account.ErrInvalidKey):
			writeUnauthorized(w, "invalid_api_key", "the API key is invalid or revoked")
			return
		case authErr != nil:
			h.internalError(w, "authenticate", authErr)
			return
		case mode == authRequired && userID == "":
			writeUnauthorized(w, "unauthorized", "this endpoint requires an API key: Authorization: Bearer <api_key>")
			return
		}

		ctx := context.WithValue(r.Context(), userKey, userID)
		ctx = context.WithValue(ctx, subjectKey, subject)
		next(w, r.WithContext(ctx))
	}
}

// authenticate returns the user ID for a valid Bearer key, "" without one.
func (h *Handler) authenticate(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", nil
	}
	key, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return "", account.ErrInvalidKey
	}
	u, err := h.accounts.Authenticate(r.Context(), key)
	if err != nil {
		return "", err
	}
	return u.ID, nil
}

func writeUnauthorized(w http.ResponseWriter, code, message string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, http.StatusUnauthorized, code, message)
}

// checkRateLimit sets the X-RateLimit-* headers and writes 429 when the
// subject is over its limit. A limiter failure lets the request through: rate
// limiting must not take the API down.
func (h *Handler) checkRateLimit(w http.ResponseWriter, r *http.Request, subject string, limit int) bool {
	if h.limiter == nil {
		return true
	}
	res, err := h.limiter.Allow(r.Context(), subject, limit, time.Minute)
	if err != nil {
		h.logger.Warn("rate limiter unavailable, allowing request", "err", err)
		return true
	}
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.Reset.Unix(), 10))
	if res.Allowed {
		return true
	}
	retryAfter := int(math.Ceil(res.Reset.Sub(h.now()).Seconds()))
	w.Header().Set("Retry-After", strconv.Itoa(max(1, retryAfter)))
	writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests, retry after the time in the Retry-After header")
	return false
}

// clientIP is the TCP peer address. X-Forwarded-For is not trusted: behind a
// load balancer, it must be read only from the balancer's own header.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

const maxIdempotencyKeyLength = 255

// withIdempotency reads the request body and runs handle. With an
// Idempotency-Key header, the first response is stored and replayed to
// retries with the same key and body.
func (h *Handler) withIdempotency(w http.ResponseWriter, r *http.Request, handle func(http.ResponseWriter, []byte)) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is too large or unreadable")
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if key == "" || h.idempotency == nil {
		handle(w, body)
		return
	}
	if len(key) > maxIdempotencyKeyLength {
		writeError(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key is longer than 255 characters")
		return
	}

	scope := subjectFrom(r.Context())
	sum := sha256.Sum256(body)
	fingerprint := hex.EncodeToString(sum[:])

	rec, started, err := h.idempotency.Begin(r.Context(), scope, key, fingerprint)
	if err != nil {
		h.logger.Warn("idempotency store unavailable, processing without it", "err", err)
		handle(w, body)
		return
	}
	if !started {
		switch {
		case rec.Fingerprint != fingerprint:
			writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "this Idempotency-Key was used with a different request")
		case rec.Pending:
			writeError(w, http.StatusConflict, "idempotency_in_progress", "a request with this Idempotency-Key is still being processed")
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(rec.Status)
			_, _ = w.Write(rec.Body)
		}
		return
	}

	rw := &recorder{header: http.Header{}, status: http.StatusOK}
	handle(rw, body)

	// A server error may be transient: release the key so a retry runs again.
	// Client errors are deterministic and are replayed like successes.
	if rw.status >= http.StatusInternalServerError {
		if err := h.idempotency.Abort(r.Context(), scope, key); err != nil {
			h.logger.Warn("release idempotency key", "err", err)
		}
	} else {
		err := h.idempotency.Complete(r.Context(), scope, key, idempotency.Record{
			Fingerprint: fingerprint, Status: rw.status, Body: rw.body.Bytes(),
		})
		if err != nil {
			h.logger.Warn("store idempotent response", "err", err)
		}
	}
	rw.flush(w)
}

// recorder buffers a response so it can be stored before being sent.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recorder) WriteHeader(status int)      { r.status = status }

func (r *recorder) flush(w http.ResponseWriter) {
	for k, v := range r.header {
		w.Header()[k] = v
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body.Bytes())
}
