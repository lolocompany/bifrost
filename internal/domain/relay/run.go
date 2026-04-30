// Package relay moves records from one Kafka cluster topic to another.
package relay

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

// Run consumes from the from-side cluster, produces to the to-side cluster, and commits
// from-side offsets after each successful write on the to side. Each produced record includes
// bifrost.source.* headers (see AppendSourceHeaders), then optional ExtraHeaders from config, then
// any headers copied from the source record.
//
// When PeriodicStatsInterval is greater than zero, Run logs info-level "bridge periodic stats"
// on that interval with messages_delta and errors_delta since the previous log.
func Run(ctx context.Context, id Identity, consumer, producer *kgo.Client, metrics Metrics, opts Options) error {
	if consumer == nil || producer == nil {
		return errors.New("kafka clients must not be nil")
	}
	return RunWithClients(ctx, id, kgoConsumer{client: consumer}, kgoProducer{client: producer}, metrics, opts)
}

// RunWithClients executes the relay loop against abstract consumer and producer clients.
func RunWithClients(ctx context.Context, id Identity, consumer ConsumerClient, producer ProducerClient, metrics Metrics, opts Options) error {
	if metrics == nil {
		return errors.New("metrics must not be nil")
	}
	opts = opts.withDefaults()
	log := slog.With("bridge", id.BridgeName, "from_topic", id.FromTopic, "to_topic", id.ToTopic)

	var msgsRelayed, errorsSeen atomic.Uint64
	stopStats := func() {}
	if opts.PeriodicStatsInterval > 0 {
		statsCtx, cancel := context.WithCancel(ctx)
		statsDone := make(chan struct{})
		stopStats = func() {
			cancel()
			<-statsDone
		}
		go func() {
			defer close(statsDone)
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
	// commitQ must absorb completions while the loop is polling/committing; size 2x limits
	// producer blocking without unbounded buffering.
	commitQ := make(chan commitCandidate, opts.MaxInFlightBatches*2)
	flushQ := make(chan struct{}, 1)
	dispatchWG := &sync.WaitGroup{}
	// globalSem caps concurrently produced batches across partitions.
	globalSem := make(chan struct{}, opts.MaxInFlightBatches)
	committer := newCommitAggregator(opts.CommitMaxRecords)
	defer dispatchWG.Wait()
	baseRetry := retryContext{
		log:        log,
		errorsSeen: &errorsSeen,
		metrics:    metrics,
		id:         id,
		opts:       opts,
	}

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
			retry := baseRetry
			retry.ctx = ctx
			retry.stage = StageProduce
			retry.cfg = opts.Retry.Produce
			if err := retryStage(retry, func() error {
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
				commitRetry := baseRetry
				commitRetry.ctx = ctx
				if err := flushCommits(commitContext{
					consumer:    consumer,
					msgsRelayed: &msgsRelayed,
					committer:   committer,
					retry:       commitRetry,
				}); err != nil {
					return err
				}
			}
		case <-commitTickerC:
			commitRetry := baseRetry
			commitRetry.ctx = ctx
			if err := flushCommits(commitContext{consumer: consumer, msgsRelayed: &msgsRelayed, committer: committer, retry: commitRetry}); err != nil {
				return err
			}
		case <-flushQ:
			if committer.pendingRecords() >= opts.CommitMaxRecords {
				commitRetry := baseRetry
				commitRetry.ctx = ctx
				if err := flushCommits(commitContext{consumer: consumer, msgsRelayed: &msgsRelayed, committer: committer, retry: commitRetry}); err != nil {
					return err
				}
			}
		default:
			if err := ctx.Err(); err != nil {
				drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
				commitRetry := baseRetry
				commitRetry.ctx = drainCtx
				_ = flushCommits(commitContext{consumer: consumer, msgsRelayed: &msgsRelayed, committer: committer, retry: commitRetry})
				cancelDrain()
				return err
			}
			pollStart := time.Now()
			fetches := consumer.PollFetches(ctx)
			pollSeconds := time.Since(pollStart).Seconds()
			if err := fetches.Err(); err != nil {
				metrics.AddConsumerSeconds(id, RelayStateIdle, pollSeconds)
				metrics.AddProducerSeconds(id, RelayStateIdle, pollSeconds)
				metrics.IncErrors(id, StagePoll)
				errorsSeen.Add(1)
				if !lastPollFailed {
					log.Info("connection lost; reconnecting in background", "stage", StagePoll, "error_message", err.Error())
					lastPollFailed = true
				}
				continue
			}
			if lastPollFailed {
				log.Info("connection restored; resuming relay", "stage", StagePoll)
				lastPollFailed = false
			}
			if fetches.NumRecords() == 0 {
				metrics.AddConsumerSeconds(id, RelayStateIdle, pollSeconds)
				metrics.AddProducerSeconds(id, RelayStateIdle, pollSeconds)
				continue
			}
			metrics.AddConsumerSeconds(id, RelayStateBusy, pollSeconds)
			batches, err := partitionBatches(id, fetches.Records(), opts.BatchSize)
			if err != nil {
				log.Warn("unexpected topic on fetch", "topic", errTopic(err))
				metrics.IncErrors(id, StageRoute)
				errorsSeen.Add(1)
				return err
			}
			for _, batch := range batches {
				scheduleBatch(batch)
			}
		}
	}
}
