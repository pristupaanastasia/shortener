package main

import (
	"context"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, cfg config)
	}{
		{name: "defaults", check: func(t *testing.T, cfg config) {
			if cfg.addr != ":8080" || cfg.baseURL != "http://localhost:8080" || cfg.storage != "memory" {
				t.Errorf("cfg = %+v", cfg)
			}
			if cfg.cassandra.Keyspace != "shortener" || cfg.cassandra.ReplicationFactor != 1 || len(cfg.cassandra.Hosts) != 1 {
				t.Errorf("cassandra = %+v", cfg.cassandra)
			}
			if cfg.redisAddr != "" || cfg.postgresDSN != "" {
				t.Errorf("optional dependencies enabled by default: %+v", cfg)
			}
		}},
		{
			name: "all set",
			env: map[string]string{
				"ADDR": ":9000", "BASE_URL": "https://sho.rt", "STORAGE": "cassandra",
				"CASSANDRA_HOSTS": "c1,c2,c3", "CASSANDRA_KEYSPACE": "links", "CASSANDRA_REPLICATION_FACTOR": "3",
				"REDIS_ADDR": "redis:6379", "POSTGRES_DSN": "postgres://db",
			},
			check: func(t *testing.T, cfg config) {
				if cfg.addr != ":9000" || cfg.baseURL != "https://sho.rt" || cfg.storage != "cassandra" || cfg.redisAddr != "redis:6379" || cfg.postgresDSN != "postgres://db" {
					t.Errorf("cfg = %+v", cfg)
				}
				if len(cfg.cassandra.Hosts) != 3 || cfg.cassandra.Keyspace != "links" || cfg.cassandra.ReplicationFactor != 3 {
					t.Errorf("cassandra = %+v", cfg.cassandra)
				}
			},
		},
		{name: "relative base url", env: map[string]string{"BASE_URL": "/short"}, wantErr: true},
		{name: "base url without scheme", env: map[string]string{"BASE_URL": "sho.rt"}, wantErr: true},
		{name: "replication factor not a number", env: map[string]string{"CASSANDRA_REPLICATION_FACTOR": "three"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"ADDR", "BASE_URL", "STORAGE", "CASSANDRA_HOSTS", "CASSANDRA_KEYSPACE", "CASSANDRA_REPLICATION_FACTOR", "REDIS_ADDR", "POSTGRES_DSN"} {
				t.Setenv(k, tt.env[k])
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
