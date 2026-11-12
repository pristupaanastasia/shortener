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

	"github.com/pristupaanastasia/shortener/internal/link"
)

// maxRedirectCacheAge caps how long browsers may cache a redirect, so edits,
// deletions and expirations reach clients quickly.
const maxRedirectCacheAge = 90 * time.Second

const maxBodyBytes = 64 << 10

// Handler serves the HTTP API.
type Handler struct {
	links   *link.Service
	baseURL string
	logger  *slog.Logger
	now     func() time.Time
	mux     *http.ServeMux
}

// New returns a Handler. baseURL prefixes keys in short_url, e.g. "https://sho.rt".
func New(links *link.Service, baseURL string, logger *slog.Logger) *Handler {
	h := &Handler{
		links:   links,
		baseURL: strings.TrimRight(baseURL, "/"),
		logger:  logger,
		now:     time.Now,
		mux:     http.NewServeMux(),
	}
	h.mux.HandleFunc("POST /api/v1/links", h.createLink)
	h.mux.HandleFunc("GET /healthz", h.healthz)
	h.mux.HandleFunc("GET /{key}", h.redirect)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type createRequest struct {
	LongURL   string     `json:"long_url"`
	Alias     string     `json:"alias,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type linkResponse struct {
	Key       string     `json:"key"`
	ShortURL  string     `json:"short_url"`
	LongURL   string     `json:"long_url"`
	ExpiresAt *time.Time `json:"expires_at"`
	CreatedAt time.Time  `json:"created_at"`
}

func (h *Handler) createLink(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be a valid JSON object")
		return
	}

	l, err := h.links.Create(r.Context(), link.CreateParams{
		LongURL:   req.LongURL,
		Alias:     req.Alias,
		ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		h.writeCreateError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, linkResponse{
		Key:       l.Key,
		ShortURL:  h.baseURL + "/" + l.Key,
		LongURL:   l.LongURL,
		ExpiresAt: l.ExpiresAt,
		CreatedAt: l.CreatedAt,
	})
}

func (h *Handler) writeCreateError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, link.ErrInvalidURL):
		writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
	case errors.Is(err, link.ErrInvalidAlias):
		writeError(w, http.StatusBadRequest, "invalid_alias", err.Error())
	case errors.Is(err, link.ErrInvalidExpiry):
		writeError(w, http.StatusBadRequest, "invalid_expires_at", err.Error())
	case errors.Is(err, link.ErrAliasTaken):
		writeError(w, http.StatusConflict, "alias_taken", err.Error())
	default:
		h.logger.Error("create link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
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
