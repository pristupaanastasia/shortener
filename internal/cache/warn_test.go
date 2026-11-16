package cache

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/pristupaanastasia/shortener/internal/breaker"
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			s := &Store{logger: slog.New(slog.NewTextHandler(&buf, nil))}
			s.warnRedis("cache get failed", "k", tt.err)
			if got := buf.Len() > 0; got != tt.wantLog {
				t.Errorf("logged = %v, want %v; log: %s", got, tt.wantLog, buf.String())
			}
		})
	}
}
