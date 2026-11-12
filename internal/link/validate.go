package link

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const maxURLLength = 2048

var aliasPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{4,30}$`)

// reservedAliases collide with service paths or are likely to be confusing.
var reservedAliases = map[string]bool{
	"api":     true,
	"admin":   true,
	"login":   true,
	"logout":  true,
	"healthz": true,
	"static":  true,
}

// validateURL checks that raw is an absolute http(s) URL that does not point
// back to the service itself (ownHost), which would create a redirect loop.
func validateURL(raw, ownHost string) error {
	if raw == "" {
		return fmt.Errorf("%w: long_url is required", ErrInvalidURL)
	}
	if len(raw) > maxURLLength {
		return fmt.Errorf("%w: longer than %d characters", ErrInvalidURL, maxURLLength)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https", ErrInvalidURL)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: host is required", ErrInvalidURL)
	}
	if ownHost != "" && strings.EqualFold(u.Host, ownHost) {
		return fmt.Errorf("%w: cannot shorten links to this service", ErrInvalidURL)
	}
	return nil
}

func validateAlias(alias string) error {
	if !aliasPattern.MatchString(alias) {
		return fmt.Errorf("%w: must be 4-30 characters of [a-zA-Z0-9_-]", ErrInvalidAlias)
	}
	if reservedAliases[strings.ToLower(alias)] {
		return fmt.Errorf("%w: %q is reserved", ErrInvalidAlias, alias)
	}
	return nil
}

func validateExpiry(expiresAt *time.Time, now time.Time) error {
	if expiresAt != nil && !expiresAt.After(now) {
		return fmt.Errorf("%w: expires_at must be in the future", ErrInvalidExpiry)
	}
	return nil
}
