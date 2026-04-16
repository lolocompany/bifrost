// Package bridge relays records from one Kafka cluster to another across a configured bridge.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	ProduceSync(context.Context, ...*kgo.Record) ProduceResult
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
	OverridePartition     *int32
	OverrideKey           []byte
	Sleep                 func(context.Context, time.Duration) error
	Jitter                func(time.Duration) time.Duration
	// ExtraHeaders are appended to each produced record after bifrost.source.* headers and before
	// headers copied from the source record.
	ExtraHeaders []kgo.RecordHeader
}

var defaultRetryConfig = RetryConfig{
	MinBackoff: time.Second,
	MaxBackoff: 30 * time.Second,
	Jitter:     250 * time.Millisecond,
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

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
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
			continue
		}

		batches, err := partitionBatches(id, fetches.Records(), opts.BatchSize)
		if err != nil {
			log.Warn("unexpected topic on fetch", "topic", errTopic(err))
			metrics.IncErrors(id, "route")
			errorsSeen.Add(1)
			return err
		}

		for _, batch := range batches {
			out := make([]*kgo.Record, 0, len(batch))
			for _, r := range batch {
				headers := make([]kgo.RecordHeader, 0, len(r.Headers)+4+len(opts.ExtraHeaders))
				headers = AppendSourceHeaders(headers, id, r)
				headers = append(headers, opts.ExtraHeaders...)
				headers = append(headers, r.Headers...)

				record := &kgo.Record{
					Topic:     id.ToTopic,
					Key:       r.Key,
					Value:     r.Value,
					Headers:   headers,
					Timestamp: r.Timestamp,
				}
				if opts.OverridePartition != nil {
					record.Partition = *opts.OverridePartition
				} else {
					record.Partition = r.Partition
				}
				if opts.OverrideKey != nil {
					record.Key = append([]byte(nil), opts.OverrideKey...)
				}
				out = append(out, record)
			}

			var produceStart time.Time
			if err := retryStage(ctx, log, "produce", opts.Retry.Produce, &errorsSeen, metrics, id, opts, func() error {
				produceStart = time.Now()
				return producer.ProduceSync(ctx, out...).FirstErr()
			}); err != nil {
				return fmt.Errorf("produce to %q: %w", id.ToTopic, err)
			}
			metrics.ObserveProduceDuration(id, time.Since(produceStart).Seconds())

			if err := retryStage(ctx, log, "commit", opts.Retry.Commit, &errorsSeen, metrics, id, opts, func() error {
				return consumer.CommitRecords(ctx, batch...)
			}); err != nil {
				return fmt.Errorf("commit from-side offsets: %w", err)
			}
			for range batch {
				metrics.IncMessages(id)
				msgsRelayed.Add(1)
			}
		}
	}
}

type kgoConsumer struct {
	client *kgo.Client
}

func (c kgoConsumer) PollFetches(ctx context.Context) FetchResult {
	return kgoFetches{fetches: c.client.PollFetches(ctx)}
}

func (c kgoConsumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

type kgoProducer struct {
	client *kgo.Client
}

func (p kgoProducer) ProduceSync(ctx context.Context, records ...*kgo.Record) ProduceResult {
	return kgoProduceResults{results: p.client.ProduceSync(ctx, records...)}
}

type kgoFetches struct {
	fetches kgo.Fetches
}

func (f kgoFetches) Err() error             { return f.fetches.Err() }
func (f kgoFetches) NumRecords() int        { return f.fetches.NumRecords() }
func (f kgoFetches) Records() []*kgo.Record { return f.fetches.Records() }

type kgoProduceResults struct {
	results kgo.ProduceResults
}

func (r kgoProduceResults) FirstErr() error { return r.results.FirstErr() }

func (o RunOptions) withDefaults() RunOptions {
	if o.Sleep == nil {
		o.Sleep = sleepContext
	}
	if o.Jitter == nil {
		o.Jitter = randomJitter
	}
	if o.BatchSize == 0 {
		o.BatchSize = 1
	}
	o.Retry.Produce = withRetryDefaults(o.Retry.Produce)
	o.Retry.Commit = withRetryDefaults(o.Retry.Commit)
	return o
}

func withRetryDefaults(cfg RetryConfig) RetryConfig {
	if cfg == (RetryConfig{}) {
		return defaultRetryConfig
	}
	return cfg
}

func retryStage(
	ctx context.Context,
	log *slog.Logger,
	stage string,
	cfg RetryConfig,
	errorsSeen *atomic.Uint64,
	m MetricsReporter,
	id Identity,
	opts RunOptions,
	op func() error,
) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := op(); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			m.IncErrors(id, stage)
			errorsSeen.Add(1)
			attempt++
			base, jitter, sleepFor := retryDelay(attempt, cfg, opts.Jitter)
			log.Warn("bridge stage failed; retrying",
				"stage", stage,
				"attempt", attempt,
				"error_message", err.Error(),
				"base_backoff", base.String(),
				"jitter", jitter.String(),
				"sleep", sleepFor.String(),
			)
			if err := opts.Sleep(ctx, sleepFor); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return err
			}
			continue
		}
		return nil
	}
}

func retryDelay(attempt int, cfg RetryConfig, jitterFn func(time.Duration) time.Duration) (time.Duration, time.Duration, time.Duration) {
	base := cfg.MinBackoff
	for i := 1; i < attempt; i++ {
		if base >= cfg.MaxBackoff {
			base = cfg.MaxBackoff
			break
		}
		if base > cfg.MaxBackoff/2 {
			base = cfg.MaxBackoff
			break
		}
		base *= 2
	}
	jitter := time.Duration(0)
	if cfg.Jitter > 0 && jitterFn != nil {
		jitter = jitterFn(cfg.Jitter)
		if jitter < 0 {
			jitter = 0
		}
		if jitter > cfg.Jitter {
			jitter = cfg.Jitter
		}
	}
	return base, jitter, base + jitter
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// #nosec G404 -- retry jitter only needs statistical spread, not cryptographic randomness.
	return time.Duration(rand.Int64N(int64(max) + 1))
}

type topicMismatchError struct {
	got  string
	want string
}

func (e topicMismatchError) Error() string {
	return fmt.Sprintf("unexpected topic %q (want %q)", e.got, e.want)
}

func errTopic(err error) string {
	var topicErr topicMismatchError
	if errors.As(err, &topicErr) {
		return topicErr.got
	}
	return ""
}

type topicPartition struct {
	topic     string
	partition int32
}

func partitionBatches(id Identity, records []*kgo.Record, batchSize int) ([][]*kgo.Record, error) {
	if batchSize <= 0 {
		batchSize = 1
	}
	grouped := make(map[topicPartition][]*kgo.Record)
	order := make([]topicPartition, 0)
	for _, r := range records {
		if r.Topic != id.FromTopic {
			return nil, topicMismatchError{got: r.Topic, want: id.FromTopic}
		}
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], r)
	}

	batches := make([][]*kgo.Record, 0, len(records))
	for _, key := range order {
		partitionRecords := grouped[key]
		for start := 0; start < len(partitionRecords); start += batchSize {
			end := start + batchSize
			if end > len(partitionRecords) {
				end = len(partitionRecords)
			}
			batches = append(batches, partitionRecords[start:end])
		}
	}
	return batches, nil
}
