// Package bridge relays records from one Kafka cluster to another across a configured bridge.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MetricsReporter is the callback surface used by Run to record bridge metrics (implemented by metrics.BridgeMetrics).
type MetricsReporter interface {
	IncMessages(id Identity)
	IncErrors(id Identity, stage string)
	ObserveProduceDuration(id Identity, seconds float64)
	AddConsumerSeconds(id Identity, state string, seconds float64)
	AddProducerSeconds(id Identity, state string, seconds float64)
}

// FetchResult is the consumed batch surface used by RunWithClients.
type FetchResult interface {
	Err() error
	NumRecords() int
	Records() []*kgo.Record
}

// ProduceResult is the synchronous produce result surface used by RunWithClients.
type ProduceResult interface {
	FirstErr() error
}

// ConsumerClient is the minimal consumer surface required by the relay loop.
type ConsumerClient interface {
	PollFetches(context.Context) FetchResult
	CommitRecords(context.Context, ...*kgo.Record) error
}

// ProducerClient is the minimal producer surface required by the relay loop.
type ProducerClient interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

// RetryConfig configures unbounded retry with exponential backoff and additive jitter.
type RetryConfig struct {
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Jitter     time.Duration
}

// RetryPolicy defines retry behavior for transient relay stages.
type RetryPolicy struct {
	Produce RetryConfig
	Commit  RetryConfig
}

// RunOptions configures the relay loop.
type RunOptions struct {
	PeriodicStatsInterval time.Duration
	Retry                 RetryPolicy
	BatchSize             int
	MaxInFlightBatches    int
	CommitInterval        time.Duration
	CommitMaxRecords      int
	OverridePartition     *int32
	OverrideKey           []byte
	Sleep                 func(context.Context, time.Duration) error
	Jitter                func(time.Duration) time.Duration
	// ExtraHeaders are appended to each produced record after bifrost.source.* headers and before
	// headers copied from the source record.
	ExtraHeaders []kgo.RecordHeader
}

// Run consumes from the from-side cluster, produces to the to-side cluster, and commits
// from-side offsets after each successful write on the to side. Each produced record includes
// bifrost.source.* headers (see AppendSourceHeaders), then optional ExtraHeaders from config, then
// any headers copied from the source record.
//
// When PeriodicStatsInterval is greater than zero, Run logs info-level "bridge periodic stats"
// on that interval with messages_delta and errors_delta since the previous log.
func Run(ctx context.Context, id Identity, consumer, producer *kgo.Client, metrics MetricsReporter, opts RunOptions) error {
	if consumer == nil || producer == nil {
		return errors.New("kafka clients must not be nil")
	}
	return RunWithClients(ctx, id, kgoConsumer{client: consumer}, kgoProducer{client: producer}, metrics, opts)
}

// RunWithClients executes the relay loop against abstract consumer and producer clients.
func RunWithClients(ctx context.Context, id Identity, consumer ConsumerClient, producer ProducerClient, metrics MetricsReporter, opts RunOptions) error {
	if metrics == nil {
		return errors.New("metrics must not be nil")
	}
	opts = opts.withDefaults()
	log := slog.With("bridge", id.BridgeName, "from_topic", id.FromTopic, "to_topic", id.ToTopic)

	var msgsRelayed, errorsSeen atomic.Uint64
	stopStats := func() {}
	if opts.PeriodicStatsInterval > 0 {
		statsCtx, cancel := context.WithCancel(ctx)
		stopStats = cancel
		go func() {
			ticker := time.NewTicker(opts.PeriodicStatsInterval)
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

	lastPollFailed := false
	dispatchErrs := make(chan error, 1)
	commitQ := make(chan commitCandidate, opts.MaxInFlightBatches*2)
	flushQ := make(chan struct{}, 1)
	dispatchWG := &sync.WaitGroup{}
	globalSem := make(chan struct{}, opts.MaxInFlightBatches)
	committer := newCommitAggregator(opts.CommitMaxRecords)
	defer dispatchWG.Wait()

	var (
		commitTicker  *time.Ticker
		commitTickerC <-chan time.Time
	)
	if opts.CommitInterval > 0 {
		commitTicker = time.NewTicker(opts.CommitInterval)
		commitTickerC = commitTicker.C
		defer commitTicker.Stop()
	}

	scheduleBatch := func(batch []*kgo.Record) {
		dispatchWG.Add(1)
		go func(batch []*kgo.Record) {
			defer dispatchWG.Done()
			partition := batch[0].Partition
			if err := acquire(ctx, globalSem); err != nil {
				return
			}
			defer release(globalSem)

			out := mapBatchToOutput(id, batch, opts)
			var produceStart time.Time
			if err := retryStage(ctx, log, "produce", opts.Retry.Produce, &errorsSeen, metrics, id, opts, func() error {
				produceStart = time.Now()
				return produceBatchAsync(ctx, producer, out)
			}); err != nil {
				select {
				case dispatchErrs <- fmt.Errorf("produce to %q: %w", id.ToTopic, err):
				default:
				}
				return
			}
			metrics.ObserveProduceDuration(id, time.Since(produceStart).Seconds())
			select {
			case commitQ <- commitCandidate{partition: partition, records: batch}:
			case <-ctx.Done():
				return
			}
			select {
			case flushQ <- struct{}{}:
			default:
			}
		}(batch)
	}

	for {
		select {
		case err := <-dispatchErrs:
			return err
		case c := <-commitQ:
			committer.add(c.partition, c.records...)
			if committer.pendingRecords() >= opts.CommitMaxRecords {
				if err := flushCommits(ctx, log, consumer, &errorsSeen, &msgsRelayed, metrics, id, opts, committer); err != nil {
					return err
				}
			}
		case <-commitTickerC:
			if err := flushCommits(ctx, log, consumer, &errorsSeen, &msgsRelayed, metrics, id, opts, committer); err != nil {
				return err
			}
		case <-flushQ:
			if committer.pendingRecords() >= opts.CommitMaxRecords {
				if err := flushCommits(ctx, log, consumer, &errorsSeen, &msgsRelayed, metrics, id, opts, committer); err != nil {
					return err
				}
			}
		default:
			if err := ctx.Err(); err != nil {
				_ = flushCommits(context.Background(), log, consumer, &errorsSeen, &msgsRelayed, metrics, id, opts, committer)
				return err
			}
			pollStart := time.Now()
			fetches := consumer.PollFetches(ctx)
			pollSeconds := time.Since(pollStart).Seconds()
			if err := fetches.Err(); err != nil {
				metrics.AddConsumerSeconds(id, "idle", pollSeconds)
				metrics.AddProducerSeconds(id, "idle", pollSeconds)
				metrics.IncErrors(id, "poll")
				errorsSeen.Add(1)
				if !lastPollFailed {
					log.Info("connection lost; reconnecting in background", "stage", "poll", "error_message", err.Error())
					lastPollFailed = true
				}
				continue
			}
			if lastPollFailed {
				log.Info("connection restored; resuming relay", "stage", "poll")
				lastPollFailed = false
			}
			if fetches.NumRecords() == 0 {
				metrics.AddConsumerSeconds(id, "idle", pollSeconds)
				metrics.AddProducerSeconds(id, "idle", pollSeconds)
				continue
			}
			metrics.AddConsumerSeconds(id, "busy", pollSeconds)
			batches, err := partitionBatches(id, fetches.Records(), opts.BatchSize)
			if err != nil {
				log.Warn("unexpected topic on fetch", "topic", errTopic(err))
				metrics.IncErrors(id, "route")
				errorsSeen.Add(1)
				return err
			}
			for _, batch := range batches {
				scheduleBatch(batch)
			}
		}
	}
}
