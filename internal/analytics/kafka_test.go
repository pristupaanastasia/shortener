//go:build integration

package analytics

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func brokers() []string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return strings.Split(b, ",")
	}
	return []string{"127.0.0.1:9092"}
}

// memorySink keeps inserted rows; the first failures inserts fail.
type memorySink struct {
	mu       sync.Mutex
	rows     []Row
	failures int
	inserts  int
}

func (s *memorySink) Insert(_ context.Context, rows []Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inserts++
	if s.inserts <= s.failures {
		return errors.New("clickhouse unavailable")
	}
	s.rows = append(s.rows, rows...)
	return nil
}

// count returns how many stored rows have the given key.
func (s *memorySink) count(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.rows {
		if r.Key == key {
			n++
		}
	}
	return n
}

// consume runs a consumer in group until stop is called; stop returns Run's
// error.
func consume(t *testing.T, group string, sink Sink) (stop func() error) {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{Brokers: brokers(), Group: group, BatchSize: 100, FlushInterval: 200 * time.Millisecond},
		sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			t.Error("consumer did not stop within 30s")
			return nil
		}
	}
}

func publish(t *testing.T, key string, n int) {
	t.Helper()
	pub, err := NewKafkaPublisher(brokers(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		pub.Publish(Click{Key: key, Time: time.Now(), Country: "DE", UserAgent: "curl/8.7.1"})
	}
	pub.Close() // flushes
	if pub.Dropped() != 0 {
		t.Fatalf("%d events dropped with Kafka up", pub.Dropped())
	}
}

// Regression: stopping while an insert was being retried committed the
// offsets of the batch that was never stored, losing it.
func TestStopDuringRetryKeepsEvents(t *testing.T) {
	tests := []struct {
		name   string
		events int
	}{
		{name: "events survive a shutdown during insert retries", events: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "it" + strings.ToLower(rand.Text())[:10]
			group := "it-" + key
			publish(t, key, tt.events)

			down := &memorySink{failures: 1 << 30} // ClickHouse is down
			stop := consume(t, group, down)
			waitFor(t, "the consumer to start retrying", func() bool {
				down.mu.Lock()
				defer down.mu.Unlock()
				return down.inserts >= 2
			})
			if err := stop(); err == nil {
				t.Error("Run returned nil after failing to store a batch, want an error")
			}

			up := &memorySink{}
			stop = consume(t, group, up)
			waitFor(t, "events after restart", func() bool { return up.count(key) >= tt.events })
			if err := stop(); err != nil {
				t.Errorf("Run: %v", err)
			}
			if got := up.count(key); got != tt.events {
				t.Errorf("after restart the sink has %d of %d events", got, tt.events)
			}
		})
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestKafkaPipeline(t *testing.T) {
	tests := []struct {
		name         string
		events       int
		sinkFailures int // inserts that fail before the sink recovers
	}{
		{name: "events reach the sink", events: 25},
		{name: "a failing sink is retried without losing events", events: 10, sinkFailures: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A unique link key and group per run: the topic keeps old events.
			key := "it" + strings.ToLower(rand.Text())[:10]
			group := "it-" + key

			publish(t, key, tt.events)
			// A malformed record must be skipped, not block the partition.
			raw, err := kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.DefaultProduceTopic(Topic))
			if err != nil {
				t.Fatal(err)
			}
			if err := raw.ProduceSync(context.Background(), &kgo.Record{Key: []byte(key), Value: []byte("not json")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			raw.Close()

			sink := &memorySink{failures: tt.sinkFailures}
			stop := consume(t, group, sink)
			waitFor(t, "all events in the sink", func() bool { return sink.count(key) >= tt.events })
			if err := stop(); err != nil {
				t.Errorf("Run: %v", err)
			}
			if got := sink.count(key); got != tt.events {
				t.Errorf("sink has %d events, want %d", got, tt.events)
			}
			// The new group reads the topic from the start, including events of
			// other runs: check a row of this run.
			for _, row := range sink.rows {
				if row.Key == key && (row.Country != "DE" || row.Device != "bot" || row.Referrer != "direct") {
					t.Errorf("row = %+v, want enriched DE/bot/direct", row)
					break
				}
			}

			// Offsets were committed: the same group does not see them again.
			again := &memorySink{}
			stop = consume(t, group, again)
			time.Sleep(3 * time.Second)
			if err := stop(); err != nil {
				t.Errorf("Run: %v", err)
			}
			if got := again.count(key); got != 0 {
				t.Errorf("restarted consumer re-read %d committed events", got)
			}
		})
	}
}
