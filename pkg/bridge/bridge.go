// Package bridge relays records from one Kafka cluster to another across a configured bridge.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MetricsReporter is the callback surface used by Run to record bridge metrics (implemented by metrics.BridgeMetrics).
type MetricsReporter interface {
	IncMessages(id Identity)
	IncErrors(id Identity, stage string)
	ObserveProduceDuration(id Identity, seconds float64)
}

// Run consumes from the from-side cluster, produces to the to-side cluster, and commits
// from-side offsets after each successful write on the to side. Each produced record includes
// bifrost.source.* headers (see AppendSourceHeaders) before any copied source headers.
func Run(ctx context.Context, id Identity, consumer, producer *kgo.Client, m MetricsReporter) error {
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
			m.IncErrors(id, "poll")
			return fmt.Errorf("poll fetches: %w", err)
		}
		if fetches.NumRecords() == 0 {
			continue
		}

		for _, r := range fetches.Records() {
			if r.Topic != id.FromTopic {
				log.Warn("unexpected topic on fetch", "topic", r.Topic)
				m.IncErrors(id, "route")
				return fmt.Errorf("unexpected topic %q (want %q)", r.Topic, id.FromTopic)
			}

			headers := make([]kgo.RecordHeader, 0, len(r.Headers)+4)
			headers = AppendSourceHeaders(headers, id, r)
			headers = append(headers, r.Headers...)

			out := &kgo.Record{
				Topic:     id.ToTopic,
				Key:       r.Key,
				Value:     r.Value,
				Headers:   headers,
				Timestamp: r.Timestamp,
			}

			start := time.Now()
			res := producer.ProduceSync(ctx, out)
			if err := res.FirstErr(); err != nil {
				m.IncErrors(id, "produce")
				return fmt.Errorf("produce to %q: %w", id.ToTopic, err)
			}
			m.ObserveProduceDuration(id, time.Since(start).Seconds())

			if err := consumer.CommitRecords(ctx, r); err != nil {
				m.IncErrors(id, "commit")
				return fmt.Errorf("commit from-side offsets: %w", err)
			}
			m.IncMessages(id)
		}
	}
}
