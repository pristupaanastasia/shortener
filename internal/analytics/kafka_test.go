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

	"github.com/twmb/franz-go/pkg/kadm"
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

// newTopic creates a topic of its own for a test and waits until it accepts
// writes. Tests must not depend on the history of the shared topic, which may
// hold millions of events (e.g. after a load test) that a new consumer group
// would read from the start.
//
// Creation is explicit: if the publisher under test created the topic on its
// first write, leader election under a loaded machine could outlast its
// 5-second close timeout and drop events. The record written here is not
// valid JSON, so every consumer test also checks that a malformed event is
// skipped instead of blocking the partition.
func newTopic(t *testing.T) string {
	t.Helper()
	topic := "it-" + strings.ToLower(rand.Text())[:12]
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.DefaultProduceTopic(topic), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, &kgo.Record{Key: []byte("malformed"), Value: []byte("not json")}).FirstErr(); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	// Remove the topic afterwards, so per-test topics do not pile up on the
	// shared development broker.
	t.Cleanup(func() {
		admin, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
		if err != nil {
			return
		}
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := kadm.NewClient(admin).DeleteTopics(ctx, topic); err != nil {
			t.Logf("delete topic %s: %v", topic, err)
		}
	})
	return topic
}

// consume runs a consumer of topic in group until stop is called; stop
// returns Run's error.
func consume(t *testing.T, topic, group string, sink Sink) (stop func() error) {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{Brokers: brokers(), Topic: topic, Group: group, BatchSize: 100, FlushInterval: 200 * time.Millisecond},
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

func publish(t *testing.T, topic, key string, n int) {
	t.Helper()
	pub, err := NewKafkaPublisher(brokers(), slog.New(slog.NewTextHandler(io.Discard, nil)), WithTopic(topic))
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
	t.Parallel() // its own key and consumer group
	tests := []struct {
		name   string
		events int
	}{
		{name: "events survive a shutdown during insert retries", events: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "it" + strings.ToLower(rand.Text())[:10]
			topic, group := newTopic(t), "it-"+key
			publish(t, topic, key, tt.events)

			down := &memorySink{failures: 1 << 30} // ClickHouse is down
			stop := consume(t, topic, group, down)
			waitFor(t, "the consumer to start retrying", func() bool {
				down.mu.Lock()
				defer down.mu.Unlock()
				return down.inserts >= 2
			})
			if err := stop(); err == nil {
				t.Error("Run returned nil after failing to store a batch, want an error")
			}

			up := &memorySink{}
			stop = consume(t, topic, group, up)
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
	t.Parallel()
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
			t.Parallel() // each case has its own topic, key and consumer group
			key := "it" + strings.ToLower(rand.Text())[:10]
			topic, group := newTopic(t), "it-"+key

			publish(t, topic, key, tt.events) // after newTopic's malformed record

			sink := &memorySink{failures: tt.sinkFailures}
			stop := consume(t, topic, group, sink)
			waitFor(t, "all events in the sink", func() bool { return sink.count(key) >= tt.events })
			if err := stop(); err != nil {
				t.Errorf("Run: %v", err)
			}
			if got := sink.count(key); got != tt.events {
				t.Errorf("sink has %d events, want %d", got, tt.events)
			}
			// Only this run's events are in the topic.
			for _, row := range sink.rows {
				if row.Key == key && (row.Country != "DE" || row.Device != "bot" || row.Referrer != "direct") {
					t.Errorf("row = %+v, want enriched DE/bot/direct", row)
					break
				}
			}

			// Offsets were committed: the same group does not see them again.
			again := &memorySink{}
			stop = consume(t, topic, group, again)
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
