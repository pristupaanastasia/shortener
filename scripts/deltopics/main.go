// Command deltopics deletes Kafka topics: the per-run topics that tests, the
// smoke test and the load test create, so they do not pile up on a shared
// development broker.
//
//	go run ./scripts/deltopics TOPIC...           delete these topics
//	go run ./scripts/deltopics -prefix it- ...    delete topics with a prefix
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	var prefixes []string
	flag.Func("prefix", "delete every topic with this prefix (repeatable)", func(p string) error {
		prefixes = append(prefixes, p)
		return nil
	})
	flag.Parse()
	if err := run(strings.Split(*brokers, ","), prefixes, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "deltopics:", err)
		os.Exit(1)
	}
}

func run(brokers, prefixes, topics []string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	adm := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if len(prefixes) > 0 {
		all, err := adm.ListTopics(ctx)
		if err != nil {
			return fmt.Errorf("list topics: %w", err)
		}
		for _, name := range all.Names() {
			for _, p := range prefixes {
				if strings.HasPrefix(name, p) {
					topics = append(topics, name)
					break
				}
			}
		}
	}
	if len(topics) == 0 {
		return nil
	}
	resp, err := adm.DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("delete topics: %w", err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			return fmt.Errorf("delete %s: %w", r.Topic, r.Err)
		}
	}
	fmt.Printf("deleted %d topics\n", len(topics))
	return nil
}
