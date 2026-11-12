// Package analytics carries click events from the redirect path to the
// analytics store: the redirect publishes to Kafka without waiting, and a
// consumer enriches events and writes them to ClickHouse in batches.
// See "Flow 4: Analytics" in README.md.
package analytics

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Click is the event published for every successful redirect. It carries no
// IP address: the country comes from a header set by the edge (CDN or load
// balancer), so personal data never enters the pipeline.
type Click struct {
	Key       string    `json:"key"`
	Time      time.Time `json:"ts"`
	Country   string    `json:"country,omitempty"` // ISO 3166-1 alpha-2, "" if unknown
	UserAgent string    `json:"ua,omitempty"`
	Referrer  string    `json:"ref,omitempty"`
}

// Publisher sends click events. Publish never blocks and never fails the
// caller: losing an event is acceptable, slowing down a redirect is not.
type Publisher interface {
	Publish(Click)
}

// Row is an enriched click as stored in ClickHouse.
type Row struct {
	Key      string
	Time     time.Time
	Country  string // ISO code or "unknown"
	Device   string // desktop, mobile, tablet, bot or unknown
	Referrer string // host of the referring page or "direct"
}

// Decode parses and enriches a click event from Kafka.
func Decode(data []byte) (Row, error) {
	var c Click
	if err := json.Unmarshal(data, &c); err != nil {
		return Row{}, fmt.Errorf("decode click: %w", err)
	}
	if c.Key == "" || c.Time.IsZero() {
		return Row{}, fmt.Errorf("decode click: key and ts are required")
	}
	return Row{
		Key:      c.Key,
		Time:     c.Time.UTC(),
		Country:  orUnknown(Country(c.Country)),
		Device:   Device(c.UserAgent),
		Referrer: ReferrerHost(c.Referrer),
	}, nil
}

var countryPattern = regexp.MustCompile(`^[A-Z]{2}$`)

// Country normalizes a country header value to an ISO 3166-1 alpha-2 code,
// or "" if it is not one (e.g. Cloudflare's "XX" and "T1" for Tor).
func Country(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if !countryPattern.MatchString(v) || v == "XX" {
		return ""
	}
	return v
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// Device classifies a User-Agent. A heuristic, not a full parser: it is good
// enough for aggregate statistics and has no dependencies.
func Device(ua string) string {
	ua = strings.ToLower(ua)
	switch {
	case ua == "":
		return "unknown"
	case containsAny(ua, "bot", "crawler", "spider", "curl/", "wget/", "python-requests", "go-http-client"):
		return "bot"
	case containsAny(ua, "ipad", "tablet") || (strings.Contains(ua, "android") && !strings.Contains(ua, "mobile")):
		return "tablet"
	case containsAny(ua, "mobi", "iphone", "android"):
		return "mobile"
	default:
		return "desktop"
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// ReferrerHost returns the host of a Referer header, or "direct" when there
// is none. Only the host is kept: paths and queries may contain personal data.
func ReferrerHost(ref string) string {
	u, err := url.Parse(strings.TrimSpace(ref))
	if err != nil || u.Host == "" {
		return "direct"
	}
	return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
}
