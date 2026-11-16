// Package metrics exposes Prometheus metrics: HTTP request rate and
// latency per route, cache lookups per level, and any extra collectors.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the service's collectors in their own registry.
type Metrics struct {
	registry     *prometheus.Registry
	requests     *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	cacheLookups *prometheus.CounterVec
}

// New returns Metrics with Go runtime and process collectors registered.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route pattern, method and status code.",
		}, []string{"route", "method", "code"}),
		// Buckets from 0.5 ms: the redirect budget is 100 ms, and cache hits
		// take well under a millisecond.
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"route"}),
		cacheLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_lookups_total",
			Help: "Link lookups by the level that answered: l1, redis or store.",
		}, []string{"level"}),
	}
	m.registry.MustRegister(
		m.requests, m.duration, m.cacheLookups,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Register adds extra collectors, e.g. a CounterFunc over a component's own
// counter.
func (m *Metrics) Register(c ...prometheus.Collector) {
	m.registry.MustRegister(c...)
}

// CacheLookup counts a lookup answered by level.
func (m *Metrics) CacheLookup(level string) {
	m.cacheLookups.WithLabelValues(level).Inc()
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// Middleware records every request. The route label is the ServeMux pattern
// (e.g. "GET /{key}"), not the path: one time series per short link would
// make the number of series unbounded.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)

		route := r.Pattern // set by ServeMux when a route matched
		if route == "" {
			route = "unmatched"
		}
		m.requests.WithLabelValues(route, r.Method, strconv.Itoa(sw.status)).Inc()
		m.duration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}
