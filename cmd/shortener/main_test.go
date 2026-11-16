package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, cfg config)
	}{
		{name: "defaults", check: func(t *testing.T, cfg config) {
			if cfg.addr != ":8080" || cfg.metricsAddr != "127.0.0.1:9091" || cfg.baseURL != "http://localhost:8080" || cfg.storage != "memory" {
				t.Errorf("cfg = %+v", cfg)
			}
			if cfg.cassandra.Keyspace != "shortener" || cfg.cassandra.ReplicationFactor != 1 || len(cfg.cassandra.Hosts) != 1 {
				t.Errorf("cassandra = %+v", cfg.cassandra)
			}
			if cfg.rateLimitSync != 100*time.Millisecond {
				t.Errorf("rateLimitSync = %v, want 100ms", cfg.rateLimitSync)
			}
			if cfg.kafkaTopic != "clicks" {
				t.Errorf("kafkaTopic = %q, want clicks", cfg.kafkaTopic)
			}
			if cfg.redisAddr != "" || cfg.postgresDSN != "" || cfg.kafkaBrokers != nil || cfg.clickhouseDSN != "" || cfg.countryHeader != "" {
				t.Errorf("optional dependencies enabled by default: %+v", cfg)
			}
		}},
		{
			name: "all set",
			env: map[string]string{
				"ADDR": ":9000", "BASE_URL": "https://sho.rt", "STORAGE": "cassandra",
				"CASSANDRA_HOSTS": "c1,c2,c3", "CASSANDRA_KEYSPACE": "links", "CASSANDRA_REPLICATION_FACTOR": "3",
				"REDIS_ADDR": "redis:6379", "POSTGRES_DSN": "postgres://db",
				"KAFKA_BROKERS": "k1:9092,k2:9092", "KAFKA_TOPIC": "events", "CLICKHOUSE_DSN": "clickhouse://ch", "COUNTRY_HEADER": "CF-IPCountry",
				"RATE_LIMIT_ANONYMOUS": "50", "RATE_LIMIT_USER": "500",
			},
			check: func(t *testing.T, cfg config) {
				if cfg.addr != ":9000" || cfg.baseURL != "https://sho.rt" || cfg.storage != "cassandra" || cfg.redisAddr != "redis:6379" || cfg.postgresDSN != "postgres://db" {
					t.Errorf("cfg = %+v", cfg)
				}
				if len(cfg.cassandra.Hosts) != 3 || cfg.cassandra.Keyspace != "links" || cfg.cassandra.ReplicationFactor != 3 {
					t.Errorf("cassandra = %+v", cfg.cassandra)
				}
				if len(cfg.kafkaBrokers) != 2 || cfg.kafkaTopic != "events" || cfg.clickhouseDSN != "clickhouse://ch" || cfg.countryHeader != "CF-IPCountry" {
					t.Errorf("analytics = %v %q %q", cfg.kafkaBrokers, cfg.clickhouseDSN, cfg.countryHeader)
				}
				if cfg.anonymousLimit != 50 || cfg.userLimit != 500 {
					t.Errorf("rate limits = %d, %d", cfg.anonymousLimit, cfg.userLimit)
				}
			},
		},
		{name: "metrics on all interfaces when asked", env: map[string]string{"METRICS_ADDR": ":9091"},
			check: func(t *testing.T, cfg config) {
				if cfg.metricsAddr != ":9091" {
					t.Errorf("metricsAddr = %q", cfg.metricsAddr)
				}
			}},
		{name: "empty metrics address disables the server", env: map[string]string{"METRICS_ADDR": ""},
			check: func(t *testing.T, cfg config) {
				if cfg.metricsAddr != "" {
					t.Errorf("metricsAddr = %q, want empty", cfg.metricsAddr)
				}
			}},
		{name: "relative base url", env: map[string]string{"BASE_URL": "/short"}, wantErr: true},
		{name: "base url without scheme", env: map[string]string{"BASE_URL": "sho.rt"}, wantErr: true},
		{name: "replication factor not a number", env: map[string]string{"CASSANDRA_REPLICATION_FACTOR": "three"}, wantErr: true},
		{name: "cassandra without postgres", env: map[string]string{"STORAGE": "cassandra"}, wantErr: true},
		{name: "rate limit not a number", env: map[string]string{"RATE_LIMIT_ANONYMOUS": "lots"}, wantErr: true},
		{name: "rate limit zero", env: map[string]string{"RATE_LIMIT_USER": "0"}, wantErr: true},
		{name: "exact rate limiting", env: map[string]string{"RATE_LIMIT_SYNC_INTERVAL": "0"}, check: func(t *testing.T, cfg config) {
			if cfg.rateLimitSync != 0 {
				t.Errorf("rateLimitSync = %v, want 0", cfg.rateLimitSync)
			}
		}},
		{name: "sync interval not a duration", env: map[string]string{"RATE_LIMIT_SYNC_INTERVAL": "fast"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"ADDR", "METRICS_ADDR", "BASE_URL", "STORAGE", "CASSANDRA_HOSTS", "CASSANDRA_KEYSPACE", "CASSANDRA_REPLICATION_FACTOR", "REDIS_ADDR", "POSTGRES_DSN", "KAFKA_BROKERS", "KAFKA_TOPIC", "CLICKHOUSE_DSN", "COUNTRY_HEADER", "RATE_LIMIT_ANONYMOUS", "RATE_LIMIT_USER", "RATE_LIMIT_SYNC_INTERVAL"} {
				if v, ok := tt.env[k]; ok {
					t.Setenv(k, v)
				} else {
					t.Setenv(k, "") // restored after the test
					os.Unsetenv(k)
				}
			}
			cfg, err := loadConfig()
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadConfig err = %v, want error: %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestOpenStores(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config
		open    func(context.Context, config) (any, func(), error)
		wantErr bool
	}{
		{name: "memory links", cfg: config{storage: "memory"}, open: openLinks},
		{name: "unknown storage", cfg: config{storage: "mongo"}, open: openLinks, wantErr: true},
		{name: "memory accounts without dsn", cfg: config{}, open: openAccountsAny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, closeFn, err := tt.open(context.Background(), tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, want error: %v", err, tt.wantErr)
			}
			if err == nil {
				if store == nil || closeFn == nil {
					t.Fatalf("store = %v, has close func = %v", store, closeFn != nil)
				}
				closeFn()
			}
		})
	}
}

func openLinks(ctx context.Context, cfg config) (any, func(), error) { return openStore(ctx, cfg) }

func openAccountsAny(ctx context.Context, cfg config) (any, func(), error) {
	return openAccounts(ctx, cfg)
}
