package main

import (
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
			if len(cfg.brokers) != 1 || cfg.group != "analytics" || cfg.batchSize != 10000 || cfg.flushInterval != time.Second {
				t.Errorf("cfg = %+v", cfg)
			}
		}},
		{name: "all set", env: map[string]string{"KAFKA_BROKERS": "k1:9092,k2:9092", "KAFKA_GROUP": "g", "BATCH_SIZE": "500", "FLUSH_INTERVAL": "250ms", "CLICKHOUSE_DSN": "clickhouse://ch"},
			check: func(t *testing.T, cfg config) {
				if len(cfg.brokers) != 2 || cfg.group != "g" || cfg.batchSize != 500 || cfg.flushInterval != 250*time.Millisecond || cfg.clickhouseDSN != "clickhouse://ch" {
					t.Errorf("cfg = %+v", cfg)
				}
			}},
		{name: "batch size not a number", env: map[string]string{"BATCH_SIZE": "many"}, wantErr: true},
		{name: "batch size zero", env: map[string]string{"BATCH_SIZE": "0"}, wantErr: true},
		{name: "flush interval not a duration", env: map[string]string{"FLUSH_INTERVAL": "1"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"KAFKA_BROKERS", "KAFKA_GROUP", "BATCH_SIZE", "FLUSH_INTERVAL", "CLICKHOUSE_DSN"} {
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
