package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{key}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("key") == "missing" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusMovedPermanently)
	})
	mux.HandleFunc("POST /api/v1/links", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}")) // implicit 200
	})
	return mux
}

func TestMiddleware(t *testing.T) {
	tests := []struct {
		name     string
		requests [][2]string // method, path
		route    string
		method   string
		code     string
		want     float64
	}{
		{name: "redirects share one series per pattern", requests: [][2]string{{"GET", "/abc"}, {"GET", "/xyz"}, {"GET", "/def"}},
			route: "GET /{key}", method: "GET", code: "301", want: 3},
		{name: "status codes are separate series", requests: [][2]string{{"GET", "/abc"}, {"GET", "/missing"}},
			route: "GET /{key}", method: "GET", code: "404", want: 1},
		{name: "implicit 200", requests: [][2]string{{"POST", "/api/v1/links"}},
			route: "POST /api/v1/links", method: "POST", code: "200", want: 1},
		{name: "unmatched routes", requests: [][2]string{{"PUT", "/a/b/c"}, {"DELETE", "/x/y"}},
			route: "unmatched", method: "PUT", code: "404", want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			h := m.Middleware(newTestMux())
			for _, r := range tt.requests {
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(r[0], r[1], nil))
			}
			if got := testutil.ToFloat64(m.requests.WithLabelValues(tt.route, tt.method, tt.code)); got != tt.want {
				t.Errorf("http_requests_total{%s,%s,%s} = %v, want %v", tt.route, tt.method, tt.code, got, tt.want)
			}
			if n := testutil.CollectAndCount(m.duration); n == 0 {
				t.Error("no latency observations")
			}
		})
	}
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(m *Metrics)
		wantLine string
	}{
		{name: "cache lookups", setup: func(m *Metrics) { m.CacheLookup("l1"); m.CacheLookup("l1"); m.CacheLookup("redis") },
			wantLine: `cache_lookups_total{level="l1"} 2`},
		{name: "extra collectors", setup: func(m *Metrics) {
			m.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "clicks_dropped_total", Help: "test"}, func() float64 { return 7 }))
		}, wantLine: "clicks_dropped_total 7"},
		{name: "runtime metrics", setup: func(*Metrics) {}, wantLine: "go_goroutines"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			tt.setup(m)
			rec := httptest.NewRecorder()
			m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
			body, _ := io.ReadAll(rec.Body)
			if !strings.Contains(string(body), tt.wantLine) {
				t.Errorf("metrics output has no %q", tt.wantLine)
			}
		})
	}
}

// Regression: the raw method was a label, so arbitrary methods created
// unlimited time series.
func TestMethodCardinality(t *testing.T) {
	tests := []struct {
		name       string
		methods    int
		wantSeries int
	}{
		{name: "500 made-up methods share one series", methods: 500, wantSeries: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			srv := httptest.NewServer(m.Middleware(newTestMux()))
			defer srv.Close()
			for i := range tt.methods {
				req, err := http.NewRequest("M"+strconv.Itoa(i)+"X", srv.URL+"/a/b", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
			}
			if got := testutil.CollectAndCount(m.requests); got != tt.wantSeries {
				t.Errorf("http_requests_total has %d series, want %d", got, tt.wantSeries)
			}
		})
	}
}

func TestMethodLabel(t *testing.T) {
	tests := []struct {
		name, method, want string
	}{
		{name: "get", method: "GET", want: "GET"},
		{name: "patch", method: "PATCH", want: "PATCH"},
		{name: "lowercase is not normalized", method: "get", want: "other"},
		{name: "made up", method: "M1X", want: "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := methodLabel(tt.method); got != tt.want {
				t.Errorf("methodLabel(%q) = %q, want %q", tt.method, got, tt.want)
			}
		})
	}
}
