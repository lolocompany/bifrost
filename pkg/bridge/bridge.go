// Package bridge relays records from one Kafka cluster to another across a configured bridge.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
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
//
// When periodicStatsInterval is greater than zero, Run logs info-level "bridge periodic stats"
// on that interval with messages_delta and errors_delta since the previous log.
func Run(ctx context.Context, id Identity, consumer, producer *kgo.Client, m MetricsReporter, periodicStatsInterval time.Duration) error {
	if consumer == nil || producer == nil {
		return errors.New("kafka clients must not be nil")
	}
	if m == nil {
		return errors.New("metrics must not be nil")
	}
	log := slog.With("bridge", id.BridgeName, "from_topic", id.FromTopic, "to_topic", id.ToTopic)

	var msgsRelayed, errorsSeen atomic.Uint64
	stopStats := func() {}
	if periodicStatsInterval > 0 {
		statsCtx, cancel := context.WithCancel(ctx)
		stopStats = cancel
		go func() {
			ticker := time.NewTicker(periodicStatsInterval)
			defer ticker.Stop()
			lastTick := time.Now()
			for {
				select {
				case <-statsCtx.Done():
					return
				case <-ticker.C:
					now := time.Now()
					log.Info("bridge periodic stats",
						"messages_delta", msgsRelayed.Swap(0),
						"errors_delta", errorsSeen.Swap(0),
						"period_seconds", now.Sub(lastTick).Seconds(),
					)
					lastTick = now
				}
			}
		}()
	}
	defer stopStats()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			m.IncErrors(id, "poll")
			errorsSeen.Add(1)
			return fmt.Errorf("poll fetches: %w", err)
		}
		if fetches.NumRecords() == 0 {
			continue
		}

		for _, r := range fetches.Records() {
			if r.Topic != id.FromTopic {
				log.Warn("unexpected topic on fetch", "topic", r.Topic)
				m.IncErrors(id, "route")
				errorsSeen.Add(1)
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
				errorsSeen.Add(1)
				return fmt.Errorf("produce to %q: %w", id.ToTopic, err)
			}
			m.ObserveProduceDuration(id, time.Since(start).Seconds())

			if err := consumer.CommitRecords(ctx, r); err != nil {
				m.IncErrors(id, "commit")
				errorsSeen.Add(1)
				return fmt.Errorf("commit from-side offsets: %w", err)
			}
			m.IncMessages(id)
			msgsRelayed.Add(1)
		}
	}
}
