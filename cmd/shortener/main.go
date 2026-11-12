// Command shortener runs the URL shortener HTTP service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pristupaanastasia/shortener/internal/httpapi"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/storage/cassandra"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
)

type config struct {
	addr      string
	baseURL   string
	storage   string
	cassandra cassandra.Config
}

func loadConfig() (config, error) {
	cfg := config{
		addr:    getenv("ADDR", ":8080"),
		baseURL: getenv("BASE_URL", "http://localhost:8080"),
		storage: getenv("STORAGE", "memory"),
		cassandra: cassandra.Config{
			Hosts:    strings.Split(getenv("CASSANDRA_HOSTS", "127.0.0.1"), ","),
			Keyspace: getenv("CASSANDRA_KEYSPACE", "shortener"),
		},
	}
	if u, err := url.Parse(cfg.baseURL); err != nil || u.Host == "" {
		return config{}, fmt.Errorf("BASE_URL %q must be an absolute URL", cfg.baseURL)
	}
	rf, err := strconv.Atoi(getenv("CASSANDRA_REPLICATION_FACTOR", "1"))
	if err != nil {
		return config{}, fmt.Errorf("CASSANDRA_REPLICATION_FACTOR: %w", err)
	}
	cfg.cassandra.ReplicationFactor = rf
	return cfg, nil
}

// openStore returns the configured link store and a function that releases it.
func openStore(ctx context.Context, cfg config) (link.Store, func(), error) {
	switch cfg.storage {
	case "memory":
		return memory.New(), func() {}, nil
	case "cassandra":
		s, err := cassandra.Open(ctx, cfg.cassandra)
		if err != nil {
			return nil, nil, err
		}
		return s, s.Close, nil
	default:
		return nil, nil, fmt.Errorf("STORAGE must be memory or cassandra, got %q", cfg.storage)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("shortener stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	baseURL, _ := url.Parse(cfg.baseURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStore()

	links := link.NewService(store, baseURL.Host)
	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           httpapi.New(links, cfg.baseURL, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.addr, "base_url", cfg.baseURL, "storage", cfg.storage)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}
