// Package bridge relays records from one Kafka cluster to another across a configured bridge.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lolocompany/bifrost/pkg/metrics"
)

// Run consumes from the from-side cluster, produces to the to-side cluster, and commits
// from-side offsets after each successful write on the to side.
func Run(ctx context.Context, id metrics.BridgeIdentity, consumer, producer *kgo.Client, m *metrics.Metrics) error {
	if consumer == nil || producer == nil {
		return errors.New("kafka clients must not be nil")
	}
	if m == nil {
		return errors.New("metrics must not be nil")
	}
	log := slog.With("bridge", id.BridgeName, "from_topic", id.FromTopic, "to_topic", id.ToTopic)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			m.AddForwardError(id, "poll")
			return fmt.Errorf("poll fetches: %w", err)
		}
		if fetches.NumRecords() == 0 {
			continue
		}

		for _, r := range fetches.Records() {
			if r.Topic != id.FromTopic {
				log.Warn("unexpected topic on fetch", "topic", r.Topic)
				m.AddForwardError(id, "route")
				return fmt.Errorf("unexpected topic %q (want %q)", r.Topic, id.FromTopic)
			}

			out := &kgo.Record{
				Topic:     id.ToTopic,
				Key:       r.Key,
				Value:     r.Value,
				Headers:   append([]kgo.RecordHeader(nil), r.Headers...),
				Timestamp: r.Timestamp,
			}

			start := time.Now()
			res := producer.ProduceSync(ctx, out)
			if err := res.FirstErr(); err != nil {
				m.AddForwardError(id, "produce")
				return fmt.Errorf("produce to %q: %w", id.ToTopic, err)
			}
			m.ObserveForwardLatency(id, time.Since(start).Seconds())

			if err := consumer.CommitRecords(ctx, r); err != nil {
				m.AddForwardError(id, "commit")
				return fmt.Errorf("commit from-side offsets: %w", err)
			}
			m.AddForwarded(id)
		}
	}
}
