package analytics

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDevice(t *testing.T) {
	tests := []struct {
		name string
		ua   string
		want string
	}{
		{name: "no user agent", ua: "", want: "unknown"},
		{name: "desktop chrome", ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 Chrome/130.0 Safari/537.36", want: "desktop"},
		{name: "iphone", ua: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) Mobile/15E148", want: "mobile"},
		{name: "android phone", ua: "Mozilla/5.0 (Linux; Android 15; Pixel 9) Chrome/130.0 Mobile Safari/537.36", want: "mobile"},
		{name: "android tablet", ua: "Mozilla/5.0 (Linux; Android 15; SM-X910) Chrome/130.0 Safari/537.36", want: "tablet"},
		{name: "ipad", ua: "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X)", want: "tablet"},
		{name: "googlebot", ua: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", want: "bot"},
		{name: "curl", ua: "curl/8.7.1", want: "bot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Device(tt.ua); got != tt.want {
				t.Errorf("Device(%q) = %q, want %q", tt.ua, got, tt.want)
			}
		})
	}
}

func TestCountry(t *testing.T) {
	tests := []struct {
		name, header, want string
	}{
		{name: "iso code", header: "DE", want: "DE"},
		{name: "lowercase with spaces", header: " us ", want: "US"},
		{name: "unknown marker", header: "XX", want: ""},
		{name: "tor marker", header: "T1", want: ""},
		{name: "too long", header: "DEU", want: ""},
		{name: "empty", header: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Country(tt.header); got != tt.want {
				t.Errorf("Country(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

func TestReferrerHost(t *testing.T) {
	tests := []struct {
		name, ref, want string
	}{
		{name: "no referrer", ref: "", want: "direct"},
		{name: "path and query are dropped", ref: "https://twitter.com/user/status/1?token=secret", want: "twitter.com"},
		{name: "www prefix and case", ref: "https://WWW.Example.COM/a", want: "example.com"},
		{name: "port is dropped", ref: "http://localhost:3000/page", want: "localhost"},
		{name: "not a url", ref: "::garbage", want: "direct"},
		{name: "relative", ref: "/just/a/path", want: "direct"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReferrerHost(tt.ref); got != tt.want {
				t.Errorf("ReferrerHost(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	ts := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		data    string
		want    Row
		wantErr bool
	}{
		{
			name: "full event",
			data: `{"key":"abc1234","ts":"2026-11-12T14:00:00+04:00","country":"de","ua":"curl/8.7.1","ref":"https://news.ycombinator.com/item?id=1"}`,
			want: Row{Key: "abc1234", Time: ts, Country: "DE", Device: "bot", Referrer: "news.ycombinator.com"},
		},
		{
			name: "minimal event",
			data: `{"key":"abc1234","ts":"2026-11-12T10:00:00Z"}`,
			want: Row{Key: "abc1234", Time: ts, Country: "unknown", Device: "unknown", Referrer: "direct"},
		},
		{
			name: "referrer as a host",
			data: `{"key":"abc1234","ts":"2026-11-12T10:00:00Z","ref":"News.Example"}`,
			want: Row{Key: "abc1234", Time: ts, Country: "unknown", Device: "unknown", Referrer: "news.example"},
		},
		{name: "not json", data: `not json`, wantErr: true},
		{name: "missing key", data: `{"ts":"2026-11-12T10:00:00Z"}`, wantErr: true},
		{name: "missing time", data: `{"key":"abc1234"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode([]byte(tt.data))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode err = %v, want error: %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Decode = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// A redirect must never wait for Kafka, even when no broker is reachable.
func TestPublishDoesNotBlock(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		events  int
	}{
		{name: "nothing listening", brokers: []string{"127.0.0.1:1"}, events: 1000},
		{name: "unresolvable host", brokers: []string{"kafka.invalid:9092"}, events: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewKafkaPublisher(tt.brokers, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				done := make(chan struct{})
				go func() { p.Close(); close(done) }()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("Close did not return within 10s")
				}
			}()

			start := time.Now()
			for range tt.events {
				p.Publish(Click{Key: "abc1234", Time: time.Now()})
			}
			if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
				t.Errorf("%d publishes took %v with Kafka down, want < 100ms", tt.events, elapsed)
			}
		})
	}
}

func TestNewClick(t *testing.T) {
	ts := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	long := strings.Repeat("a", MaxUserAgent+100)
	tests := []struct {
		name                  string
		country, ua, referrer string
		want                  Click
	}{
		{name: "full request", country: "de", ua: "curl/8.7.1", referrer: "https://www.News.example/path?token=secret",
			want: Click{Key: "k", Time: ts, Country: "DE", UserAgent: "curl/8.7.1", Referrer: "news.example"}},
		{name: "nothing known", want: Click{Key: "k", Time: ts}},
		{name: "long user agent is truncated", ua: long, want: Click{Key: "k", Time: ts, UserAgent: long[:MaxUserAgent]}},
		{name: "invalid country is dropped", country: "Germany", want: Click{Key: "k", Time: ts}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClick("k", ts, tt.country, tt.ua, tt.referrer); got != tt.want {
				t.Errorf("NewClick = %+v, want %+v", got, tt.want)
			}
		})
	}
}
