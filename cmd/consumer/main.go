// Command consumer reads click events from Kafka and writes them to
// ClickHouse in batches. It scales independently of the redirect service.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pristupaanastasia/shortener/internal/analytics"
	"github.com/pristupaanastasia/shortener/internal/storage/clickhouse"
)

type config struct {
	brokers       []string
	group         string
	clickhouseDSN string
	batchSize     int
	flushInterval time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		brokers:       strings.Split(getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ","),
		group:         getenv("KAFKA_GROUP", "analytics"),
		clickhouseDSN: getenv("CLICKHOUSE_DSN", "clickhouse://shortener:shortener@127.0.0.1:9000/shortener"),
	}
	var err error
	if cfg.batchSize, err = strconv.Atoi(getenv("BATCH_SIZE", "10000")); err != nil || cfg.batchSize < 1 {
		return config{}, fmt.Errorf("BATCH_SIZE must be a positive integer")
	}
	if cfg.flushInterval, err = time.ParseDuration(getenv("FLUSH_INTERVAL", "1s")); err != nil || cfg.flushInterval <= 0 {
		return config{}, fmt.Errorf("FLUSH_INTERVAL must be a positive duration like 1s")
	}
	return cfg, nil
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
		logger.Error("consumer stopped", "err", err)
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

	store, err := clickhouse.Open(ctx, cfg.clickhouseDSN)
	if err != nil {
		return err
	}
	defer store.Close()

	consumer, err := analytics.NewConsumer(analytics.ConsumerConfig{
		Brokers:       cfg.brokers,
		Group:         cfg.group,
		BatchSize:     cfg.batchSize,
		FlushInterval: cfg.flushInterval,
	}, store, logger)
	if err != nil {
		return err
	}
	logger.Info("consuming", "brokers", cfg.brokers, "group", cfg.group, "batch_size", cfg.batchSize, "flush_interval", cfg.flushInterval.String())
	if err := consumer.Run(ctx); err != nil {
		return err
	}
	logger.Info("shutting down")
	return nil
}
