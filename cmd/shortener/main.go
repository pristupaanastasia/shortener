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

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/cache"
	"github.com/pristupaanastasia/shortener/internal/httpapi"
	"github.com/pristupaanastasia/shortener/internal/idempotency"
	"github.com/pristupaanastasia/shortener/internal/link"
	"github.com/pristupaanastasia/shortener/internal/ratelimit"
	"github.com/pristupaanastasia/shortener/internal/storage/cassandra"
	"github.com/pristupaanastasia/shortener/internal/storage/clickhouse"
	"github.com/pristupaanastasia/shortener/internal/storage/memory"
	"github.com/pristupaanastasia/shortener/internal/storage/postgres"
)

type config struct {
	addr        string
	baseURL     string
	storage     string
	cassandra   cassandra.Config
	redisAddr   string // empty: no cache, rate limiting or idempotency
	postgresDSN string // empty: in-memory accounts

	kafkaBrokers  []string // empty: clicks are not published
	clickhouseDSN string   // empty: no statistics endpoint
	countryHeader string   // header with the client's country, set by the edge
}

func loadConfig() (config, error) {
	cfg := config{
		addr:        getenv("ADDR", ":8080"),
		baseURL:     getenv("BASE_URL", "http://localhost:8080"),
		storage:     getenv("STORAGE", "memory"),
		redisAddr:   os.Getenv("REDIS_ADDR"),
		postgresDSN: os.Getenv("POSTGRES_DSN"),

		clickhouseDSN: os.Getenv("CLICKHOUSE_DSN"),
		countryHeader: os.Getenv("COUNTRY_HEADER"),
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
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.kafkaBrokers = strings.Split(brokers, ",")
	}
	return cfg, nil
}

// openAccounts returns the configured account store and a function that
// releases it.
func openAccounts(ctx context.Context, cfg config) (account.Store, func(), error) {
	if cfg.postgresDSN == "" {
		return memory.NewAccounts(), func() {}, nil
	}
	a, err := postgres.Open(ctx, cfg.postgresDSN)
	if err != nil {
		return nil, nil, err
	}
	return a, a.Close, nil
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

	accounts, closeAccounts, err := openAccounts(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeAccounts()

	api := httpapi.Config{BaseURL: cfg.baseURL, Logger: logger}
	if cfg.redisAddr != "" {
		rdb := cache.NewRedisClient(cfg.redisAddr)
		defer rdb.Close()
		cached := cache.New(store, rdb, cache.DefaultConfig(), logger)
		// Redis is optional at runtime, so an unreachable one is only a warning.
		if err := cached.Ping(ctx); err != nil {
			logger.Warn("redis unavailable, serving from storage until it recovers", "err", err)
		}
		store = cached
		api.Limiter = ratelimit.New(rdb)
		api.Idempotency = idempotency.New(rdb, idempotency.DefaultTTL)
	} else {
		logger.Warn("REDIS_ADDR is not set: caching, rate limiting and Idempotency-Key are disabled")
	}
	api.Links = link.NewService(store, baseURL.Host)
	api.Accounts = account.NewService(accounts)

	if len(cfg.kafkaBrokers) > 0 {
		publisher, err := analytics.NewKafkaPublisher(cfg.kafkaBrokers, logger)
		if err != nil {
			return err
		}
		defer publisher.Close() // after the server has stopped: flushes buffered clicks
		api.Clicks = publisher
		api.CountryHeader = cfg.countryHeader
	}
	if cfg.clickhouseDSN != "" {
		stats, err := clickhouse.Open(ctx, cfg.clickhouseDSN)
		if err != nil {
			return err
		}
		defer stats.Close()
		api.Stats = stats
	}

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           httpapi.New(api),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    64 << 10,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.addr, "base_url", cfg.baseURL, "storage", cfg.storage, "redis", cfg.redisAddr != "", "postgres", cfg.postgresDSN != "",
			"kafka", len(cfg.kafkaBrokers) > 0, "clickhouse", cfg.clickhouseDSN != "")
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
