// Command cleanup archives expired links to cold storage and leaves
// tombstones in their place (README "Flow 5: Expired links"). It runs once
// and exits; a scheduler (a Kubernetes CronJob) starts it daily.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pristupaanastasia/shortener/internal/cleanup"
	"github.com/pristupaanastasia/shortener/internal/storage/cassandra"
	"github.com/pristupaanastasia/shortener/internal/storage/s3"
)

// leaseTTL bounds how long a crashed worker blocks the next run.
const leaseTTL = 5 * time.Minute

type config struct {
	cassandra cassandra.Config
	archive   s3.Config
	cleanup   cleanup.Config
}

func loadConfig() (config, error) {
	cfg := config{
		cassandra: cassandra.Config{
			Hosts:    strings.Split(getenv("CASSANDRA_HOSTS", "127.0.0.1"), ","),
			Keyspace: getenv("CASSANDRA_KEYSPACE", "shortener"),
		},
		archive: s3.Config{
			Endpoint:  getenv("ARCHIVE_ENDPOINT", "127.0.0.1:8333"),
			Bucket:    getenv("ARCHIVE_BUCKET", "shortener-archive"),
			Prefix:    os.Getenv("ARCHIVE_PREFIX"),
			AccessKey: credential("ARCHIVE_ACCESS_KEY"),
			SecretKey: credential("ARCHIVE_SECRET_KEY"),
			Secure:    os.Getenv("ARCHIVE_SECURE") == "true",
		},
		cleanup: cleanup.DefaultConfig(),
	}
	if cfg.archive.AccessKey == "" || cfg.archive.SecretKey == "" {
		return config{}, fmt.Errorf("ARCHIVE_ACCESS_KEY and ARCHIVE_SECRET_KEY must not be empty")
	}
	var err error
	if cfg.cassandra.ReplicationFactor, err = strconv.Atoi(getenv("CASSANDRA_REPLICATION_FACTOR", "1")); err != nil || cfg.cassandra.ReplicationFactor < 1 {
		return config{}, fmt.Errorf("CASSANDRA_REPLICATION_FACTOR must be a positive integer")
	}
	if cfg.cleanup.Lookback, err = strconv.Atoi(getenv("CLEANUP_LOOKBACK_DAYS", strconv.Itoa(cfg.cleanup.Lookback))); err != nil || cfg.cleanup.Lookback < 0 {
		return config{}, fmt.Errorf("CLEANUP_LOOKBACK_DAYS must be a non-negative integer")
	}
	return cfg, nil
}

// credential returns the variable, or the docker compose development value
// if it is unset. Set but empty is an error (see loadConfig): the Kubernetes
// base declares the credentials empty for overlays to fill, and a forgotten
// one must not fall back to development credentials.
func credential(key string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return "shortener"
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
		logger.Error("cleanup failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := cassandra.Open(ctx, cfg.cassandra)
	if err != nil {
		return err
	}
	defer store.Close()
	archive, err := s3.Open(ctx, cfg.archive)
	if err != nil {
		return err
	}

	worker := cleanup.New(store, archive, cfg.cleanup, logger)
	start := time.Now()
	var stats cleanup.Stats
	held, err := cleanup.WithLease(ctx, store, cfg.cleanup.Name, owner(), leaseTTL, func(ctx context.Context) error {
		var err error
		stats, err = worker.Run(ctx, time.Now())
		return err
	})
	attrs := []any{"archived", stats.Archived, "skipped", stats.Skipped, "objects", stats.Objects, "took", time.Since(start).Round(time.Millisecond).String()}
	if err != nil {
		// Whatever was done is kept: the next run continues from there.
		return fmt.Errorf("after archiving %d links: %w", stats.Archived, err)
	}
	if !held {
		logger.Info("another cleanup is running; nothing to do")
		return nil
	}
	logger.Info("cleanup done", attrs...)
	return nil
}

// owner identifies this process in the lease, for debugging who holds it.
func owner() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}
