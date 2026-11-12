// Package httpapi exposes the link service over HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/ratelimit"
)

// maxRedirectCacheAge caps how long browsers may cache a redirect, so edits,
// deletions and expirations reach clients quickly.
const maxRedirectCacheAge = 90 * time.Second

const maxBodyBytes = 64 << 10

// Default API rate limits, per minute.
const (
	DefaultAnonymousLimit = 10
	DefaultUserLimit      = 100
)

// Config wires the handler's dependencies.
type Config struct {
	Links    *link.Service
	Accounts *account.Service
	// Limiter and Idempotency are optional (nil disables them); both need Redis.
	Limiter     *ratelimit.Limiter
	Idempotency *idempotency.Store
	// Clicks receives an event per redirect and Stats serves statistics;
	// both are optional (nil disables analytics).
	Clicks analytics.Publisher
	Stats  analytics.StatsReader
	// CountryHeader names the header with the client's country code, set by
	// the CDN or load balancer (e.g. CF-IPCountry). Empty: country unknown.
	// Only set it when every request passes through that edge, since clients
	// can send the header themselves.
	CountryHeader string
	// BaseURL prefixes keys in short_url, e.g. "https://sho.rt".
	BaseURL string
	Logger  *slog.Logger
	// Requests per minute; zero means the defaults.
	AnonymousLimit int
	UserLimit      int
}

// Handler serves the HTTP API.
type Handler struct {
	links          *link.Service
	accounts       *account.Service
	limiter        *ratelimit.Limiter
	idempotency    *idempotency.Store
	clicks         analytics.Publisher
	stats          analytics.StatsReader
	countryHeader  string
	baseURL        string
	logger         *slog.Logger
	anonymousLimit int
	userLimit      int
	now            func() time.Time
	mux            *http.ServeMux
}

// New returns a Handler.
func New(cfg Config) *Handler {
	h := &Handler{
		links:          cfg.Links,
		accounts:       cfg.Accounts,
		limiter:        cfg.Limiter,
		idempotency:    cfg.Idempotency,
		clicks:         cfg.Clicks,
		stats:          cfg.Stats,
		countryHeader:  cfg.CountryHeader,
		baseURL:        strings.TrimRight(cfg.BaseURL, "/"),
		logger:         cfg.Logger,
		anonymousLimit: cmpOr(cfg.AnonymousLimit, DefaultAnonymousLimit),
		userLimit:      cmpOr(cfg.UserLimit, DefaultUserLimit),
		now:            time.Now,
		mux:            http.NewServeMux(),
	}
	h.mux.HandleFunc("POST /api/v1/users", h.api(h.register, authOptional))
	h.mux.HandleFunc("POST /api/v1/links", h.api(h.createLink, authOptional))
	h.mux.HandleFunc("GET /api/v1/links", h.api(h.listLinks, authRequired))
	h.mux.HandleFunc("GET /api/v1/links/{key}", h.api(h.getLink, authRequired))
	h.mux.HandleFunc("PATCH /api/v1/links/{key}", h.api(h.updateLink, authRequired))
	h.mux.HandleFunc("DELETE /api/v1/links/{key}", h.api(h.deleteLink, authRequired))
	h.mux.HandleFunc("GET /api/v1/links/{key}/stats", h.api(h.linkStats, authRequired))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	h.mux.HandleFunc("GET /{key}", h.redirect)
	return h
}

func cmpOr(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type linkResponse struct {
	Key       string     `json:"key"`
	ShortURL  string     `json:"short_url"`
	LongURL   string     `json:"long_url"`
	ExpiresAt *time.Time `json:"expires_at"`
	CreatedAt time.Time  `json:"created_at"`
}

func (h *Handler) linkView(l link.Link) linkResponse {
	return linkResponse{
		Key:       l.Key,
		ShortURL:  h.baseURL + "/" + l.Key,
		LongURL:   l.LongURL,
		ExpiresAt: l.ExpiresAt,
		CreatedAt: l.CreatedAt,
	}
}

type registerRequest struct {
	Email string `json:"email"`
}

type registerResponse struct {
	UserID    string    `json:"user_id"`
	Email     string    `json:"email"`
	APIKey    string    `json:"api_key"`
	CreatedAt time.Time `json:"created_at"`
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	u, key, err := h.accounts.Register(r.Context(), req.Email)
	switch {
	case errors.Is(err, account.ErrInvalidEmail):
		writeError(w, http.StatusBadRequest, "invalid_email", err.Error())
	case errors.Is(err, account.ErrEmailTaken):
		writeError(w, http.StatusConflict, "email_taken", err.Error())
	case err != nil:
		h.internalError(w, "register", err)
	default:
		writeJSON(w, http.StatusCreated, registerResponse{UserID: u.ID, Email: u.Email, APIKey: key, CreatedAt: u.CreatedAt})
	}
}

type createRequest struct {
	LongURL   string     `json:"long_url"`
	Alias     string     `json:"alias,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (h *Handler) createLink(w http.ResponseWriter, r *http.Request) {
	h.withIdempotency(w, r, func(w http.ResponseWriter, body []byte) {
		var req createRequest
		if err := strictUnmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "request body must be a valid JSON object")
			return
		}
		l, err := h.links.Create(r.Context(), link.CreateParams{
			LongURL:   req.LongURL,
			Alias:     req.Alias,
			ExpiresAt: req.ExpiresAt,
			UserID:    userFrom(r.Context()),
		})
		if err != nil {
			h.writeLinkError(w, "create link", err)
			return
		}
		writeJSON(w, http.StatusCreated, h.linkView(l))
	})
}

type listResponse struct {
	Items      []linkResponse `json:"items"`
	NextCursor string         `json:"next_cursor"`
}

func (h *Handler) listLinks(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > link.MaxPageSize {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and "+strconv.Itoa(link.MaxPageSize))
			return
		}
		limit = n
	}
	links, next, err := h.links.List(r.Context(), userFrom(r.Context()), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.writeLinkError(w, "list links", err)
		return
	}
	resp := listResponse{Items: make([]linkResponse, 0, len(links)), NextCursor: next}
	for _, l := range links {
		resp.Items = append(resp.Items, h.linkView(l))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getLink(w http.ResponseWriter, r *http.Request) {
	l, err := h.links.Get(r.Context(), userFrom(r.Context()), r.PathValue("key"))
	if err != nil {
		h.writeLinkError(w, "get link", err)
		return
	}
	writeJSON(w, http.StatusOK, h.linkView(l))
}

// updateLink applies a JSON merge patch: absent fields stay unchanged, and
// "expires_at": null makes the link permanent.
func (h *Handler) updateLink(w http.ResponseWriter, r *http.Request) {
	var fields map[string]json.RawMessage
	if !decodeJSON(w, r, &fields) {
		return
	}
	var p link.UpdateParams
	for name, raw := range fields {
		var err error
		switch name {
		case "long_url":
			p.LongURL = new(string)
			if string(raw) == "null" {
				err = errors.New("long_url cannot be null")
			} else {
				err = json.Unmarshal(raw, p.LongURL)
			}
		case "expires_at":
			p.SetExpiresAt = true
			err = json.Unmarshal(raw, &p.ExpiresAt)
		default:
			err = errors.New("unknown field " + strconv.Quote(name))
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}

	l, err := h.links.Update(r.Context(), userFrom(r.Context()), r.PathValue("key"), p)
	if err != nil {
		h.writeLinkError(w, "update link", err)
		return
	}
	writeJSON(w, http.StatusOK, h.linkView(l))
}

func (h *Handler) deleteLink(w http.ResponseWriter, r *http.Request) {
	if err := h.links.Delete(r.Context(), userFrom(r.Context()), r.PathValue("key")); err != nil {
		h.writeLinkError(w, "delete link", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeLinkError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, link.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "link not found")
	case errors.Is(err, link.ErrInvalidURL):
		writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
	case errors.Is(err, link.ErrInvalidAlias):
		writeError(w, http.StatusBadRequest, "invalid_alias", err.Error())
	case errors.Is(err, link.ErrInvalidExpiry):
		writeError(w, http.StatusBadRequest, "invalid_expires_at", err.Error())
	case errors.Is(err, link.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "invalid_cursor", "cursor is invalid or expired")
	case errors.Is(err, link.ErrAliasTaken):
		writeError(w, http.StatusConflict, "alias_taken", err.Error())
	default:
		h.internalError(w, op, err)
	}
}

func (h *Handler) internalError(w http.ResponseWriter, op string, err error) {
	h.logger.Error(op, "err", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal server error")
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request) {
	l, err := h.links.Resolve(r.Context(), r.PathValue("key"))
	switch {
	case errors.Is(err, link.ErrNotFound):
		writeHTMLError(w, http.StatusNotFound, "Link not found")
		return
	case errors.Is(err, link.ErrGone):
		writeHTMLError(w, http.StatusGone, "This link has expired or was deleted")
		return
	case err != nil:
		h.logger.Error("resolve link", "err", err)
		writeHTMLError(w, http.StatusInternalServerError, "Something went wrong")
		return
	}

	w.Header().Set("Location", l.LongURL)
	w.Header().Set("Cache-Control", "private, max-age="+strconv.Itoa(redirectCacheAge(l, h.now())))
	w.WriteHeader(http.StatusMovedPermanently)

	if h.clicks != nil {
		var country string
		if h.countryHeader != "" {
			country = r.Header.Get(h.countryHeader)
		}
		h.clicks.Publish(analytics.NewClick(l.Key, h.now(), country, r.UserAgent(), r.Referer())) // never blocks
	}
}

// redirectCacheAge returns max-age in seconds: maxRedirectCacheAge, or less if
// the link expires sooner, so a browser never caches it past its lifetime.
func redirectCacheAge(l link.Link, now time.Time) int {
	age := maxRedirectCacheAge
	if l.ExpiresAt != nil {
		age = min(age, l.ExpiresAt.Sub(now))
	}
	return max(0, int(age/time.Second))
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// decodeJSON decodes a JSON object body, rejecting unknown fields, and writes
// a 400 response on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be a valid JSON object")
		return false
	}
	return true
}

func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeHTMLError renders an error page for redirect requests, which are opened
// in a browser. message must be a constant: it is not escaped.
func writeHTMLError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("<!doctype html><title>" + message + "</title><h1>" + message + "</h1>\n"))
}
