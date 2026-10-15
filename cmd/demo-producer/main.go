// Command demo-producer fills a Kafka cluster with realistic traffic so
// kafka-mcp has something interesting to diagnose:
//
//   - orders (6 partitions): JSON order events, with one very active customer
//     whose key lands on a single partition.
//   - payments (3 partitions): JSON payment events.
//   - shipping-service: a healthy consumer group that keeps up with orders.
//   - billing-service: a slow consumer group that falls behind on payments.
//   - fraud-detector: consumes orders briefly and then stops, leaving an
//     Empty group whose lag keeps growing.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated seed brokers")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	seeds := strings.Split(*brokers, ",")

	if err := createTopics(ctx, seeds); err != nil {
		log.Fatal(err)
	}
	go produce(ctx, seeds)
	go consume(ctx, seeds, "shipping-service", "orders", 0, 0)
	go consume(ctx, seeds, "billing-service", "payments", 750*time.Millisecond, 0)
	go consume(ctx, seeds, "fraud-detector", "orders", 0, 20*time.Second)
	log.Printf("demo traffic running against %s; press Ctrl+C to stop", *brokers)
	<-ctx.Done()
}

func createTopics(ctx context.Context, seeds []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	// Brokers may still be starting; retry for a while.
	deadline := time.Now().Add(60 * time.Second)
	for {
		err = create(ctx, admin)
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		log.Printf("waiting for Kafka: %v", err)
		time.Sleep(2 * time.Second)
	}
}

func create(ctx context.Context, admin *kadm.Client) error {
	for topic, partitions := range map[string]int32{"orders": 6, "payments": 3} {
		resp, err := admin.CreateTopic(ctx, partitions, 1, nil, topic)
		if err != nil {
			return err
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("creating %s: %w", topic, resp.Err)
		}
	}
	return nil
}

func produce(ctx context.Context, seeds []string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	statuses := []string{"created", "paid", "shipped"}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		customer := fmt.Sprintf("cus-%d", rand.IntN(50))
		if rand.IntN(3) == 0 {
			customer = "cus-42" // a very active customer: all on one partition
		}
		order := map[string]any{
			"order_id":    fmt.Sprintf("ord-%d", 1000+n),
			"customer_id": customer,
			"amount":      float64(rand.IntN(50000)) / 100,
			"currency":    "USD",
			"status":      statuses[rand.IntN(len(statuses))],
		}
		value, _ := json.Marshal(order)
		cl.Produce(ctx, &kgo.Record{
			Topic:   "orders",
			Key:     []byte(customer),
			Value:   value,
			Headers: []kgo.RecordHeader{{Key: "source", Value: []byte("web")}},
		}, nil)

		if n%2 == 0 {
			payment, _ := json.Marshal(map[string]any{
				"payment_id": fmt.Sprintf("pay-%d", n),
				"order_id":   order["order_id"],
				"amount":     order["amount"],
				"method":     []string{"card", "pse", "nequi"}[rand.IntN(3)],
			})
			cl.Produce(ctx, &kgo.Record{Topic: "payments", Key: []byte(customer), Value: payment}, nil)
		}
	}
}

// consume runs a consumer group member. perRecord slows it down; stopAfter,
// when non-zero, makes it leave the group after that long.
func consume(ctx context.Context, seeds []string, group, topic string, perRecord, stopAfter time.Duration) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ClientID(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	if stopAfter > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stopAfter)
		defer cancel()
	}
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, 50)
		fetches.EachRecord(func(*kgo.Record) {
			if perRecord > 0 {
				time.Sleep(perRecord)
			}
		})
	}
	if stopAfter > 0 {
		// Commit what was consumed, then leave: the group becomes Empty.
		_ = cl.CommitUncommittedOffsets(context.Background())
		log.Printf("%s stopped consuming (simulated crash)", group)
	}
}
