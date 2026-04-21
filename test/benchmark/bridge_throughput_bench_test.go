package benchmark_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bifrost"
	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// benchDrainJoinTimeout bounds how long we wait for verify PollFetches to return enough records.
// The verify client uses direct topic consumption (no consumer group), so this is mostly fetch
// latency and metadata—not multi-minute group join/sync stalls.
const benchDrainJoinTimeout = 3 * time.Minute

// benchSmokeDrainTimeout caps post-timer verification: draining every relayed record would take
// O(b.N) PollFetches and can dominate wall time; integration tests cover full E2E correctness.
const benchSmokeDrainTimeout = 15 * time.Second

// newBenchVerifyClient builds the read-side client used only to count records on the destination
// topic. Integration tests use a consumer group; benchmarks use a direct consumer (ConsumeTopics
// without ConsumerGroup) so franz-go assigns all topic partitions from metadata without group
// join—consumer group join on Docker/macOS + Redpanda was observed to block warmup for many minutes.
// The pump/producer/bridge path still uses benchCluster + kafka.*Opts for large batches; for
// multi-megabyte records, fetch limits are raised here only (not the full bench cluster opts).
func newBenchVerifyClient(brokers []string, topic, group string, payloadSize int) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ClientID("bench-verify-" + group),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		// Cap broker long-poll so ctx cancellation is observed between PollFetches calls.
		kgo.FetchMaxWait(2 * time.Second),
	}
	if payloadSize > 1024*1024 {
		partBytes := int32(int64(payloadSize) + 256*1024)
		fetchTotal := int32(50 << 20)
		opts = append(opts, kgo.FetchMaxBytes(fetchTotal), kgo.FetchMaxPartitionBytes(partBytes))
	}
	return kgo.NewClient(opts...)
}

// benchCluster mirrors production YAML: producer.batch_max_bytes, consumer fetch limits, and
// client broker socket caps—same fields that avoid MESSAGE_TOO_LARGE on clients (broker-side
// limits are set in cluster_bench_test.go on Redpanda).
func benchCluster(brokers []string, payloadSize int) bifrostconfig.Cluster {
	n := int32(int64(payloadSize) + 256*1024) // record + overhead
	fetchTotal := int32(50 << 20)             // total fetch window (matches prior bench default)
	// franz-go requires BrokerMaxReadBytes >= FetchMaxBytes and BrokerMaxWriteBytes >= ProducerBatchMaxBytes.
	bw := int32(benchBrokerMaxMessageBytes)
	if bw < n {
		bw = n
	}
	br := fetchTotal
	return bifrostconfig.Cluster{
		Brokers: brokers,
		TLS:     bifrostconfig.TLS{Enabled: false},
		SASL:    bifrostconfig.SASL{Mechanism: "none"},
		Client: bifrostconfig.ClientSettings{
			BrokerMaxWriteBytes: &bw,
			BrokerMaxReadBytes:  &br,
		},
		Consumer: bifrostconfig.ConsumerSettings{
			FetchMaxBytes:          &fetchTotal,
			FetchMaxPartitionBytes: &n,
		},
		Producer: bifrostconfig.ProducerSettings{
			BatchMaxBytes: &n,
		},
	}
}

// BenchmarkBridgeRelay256B measures end-to-end relay time per message (256 B payload) through
// pkg/bridge against a live Redpanda broker (see cluster_bench_test.go).
func BenchmarkBridgeRelay256B(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 256, 1)
}

// BenchmarkBridgeRelay16KiB uses a 16 KiB payload to stress larger records.
func BenchmarkBridgeRelay16KiB(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 16*1024, 1)
}

func BenchmarkBridgeRelay64KiB(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 64*1024, 1)
}

func BenchmarkBridgeRelay256KiB(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 256*1024, 1)
}

func BenchmarkBridgeRelay1MiB(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 1024*1024, 1)
}

// 10MiB payload to stress very large records.
func BenchmarkBridgeRelay10MiB(b *testing.B) {
	benchmarkBridgeRelayWithBurst(b, 10*1024*1024, 1)
}

// BenchmarkKafkaRoundTrip256B measures produce + single-partition consume on one topic with no
// bridge. Compare to BenchmarkBridgeRelay256B: the gap is mostly bridge work (relay produce,
// commit, headers copy) versus baseline Kafka client + broker path.
func BenchmarkKafkaRoundTrip256B(b *testing.B) {
	benchmarkKafkaRoundTrip(b, 256)
}

func BenchmarkKafkaRoundTrip16KiB(b *testing.B) {
	benchmarkKafkaRoundTrip(b, 16*1024)
}

func BenchmarkKafkaRoundTrip64KiB(b *testing.B) {
	benchmarkKafkaRoundTrip(b, 64*1024)
}

func BenchmarkKafkaRoundTrip1MiB(b *testing.B) {
	benchmarkKafkaRoundTrip(b, 1024*1024)
}

// BenchmarkBridgeRelayBurst256B sends burstSize messages per benchmark iteration while the bridge
// runs. Throughput (MB/s) reflects batching of produces; ns/op is per burst. Use with
// BenchmarkBridgeRelay256B to see whether the ceiling is per-message overhead or broker/bridge
// serialization.
func BenchmarkBridgeRelayBurst256B(b *testing.B) {
	const burstSize = 128
	benchmarkBridgeRelayWithBurst(b, 256, burstSize)
}

func BenchmarkBridgeRelayBurst64KiB(b *testing.B) {
	const burstSize = 32
	benchmarkBridgeRelayWithBurst(b, 64*1024, burstSize)
}

func BenchmarkBifrostRunBatching256B(b *testing.B) {
	const burstSize = 128
	benchmarkBifrostRunWithBurst(b, 256, burstSize, 8, 1, 1)
}

func BenchmarkBifrostRunReplicas256B(b *testing.B) {
	const burstSize = 128
	benchmarkBifrostRunWithBurst(b, 256, burstSize, 1, 4, 4)
}

func BenchmarkBifrostRunBatchingReplicas256B(b *testing.B) {
	const burstSize = 128
	benchmarkBifrostRunWithBurst(b, 256, burstSize, 8, 4, 4)
}

func BenchmarkBifrostRunAcksLeader256B(b *testing.B) {
	const burstSize = 128
	tuned := benchCluster(append([]string(nil), setupBenchRedpanda(b)...), 256)
	tuned.Producer.RequiredAcks = "leader"
	benchmarkBifrostRunWithBurstAndCluster(b, tuned, 256, burstSize, 8, 4, 4)
}

func BenchmarkBifrostRunBatchSweep256B(b *testing.B) {
	for _, batchSize := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkBifrostRunWithBurst(b, 256, 128, batchSize, 4, 4)
		})
	}
}

func BenchmarkBifrostRunReplicaSweep256B(b *testing.B) {
	for _, replicas := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("replicas_%d", replicas), func(b *testing.B) {
			benchmarkBifrostRunWithBurst(b, 256, 128, 8, replicas, 8)
		})
	}
}

func BenchmarkBifrostRunPartitionSweep256B(b *testing.B) {
	for _, partitions := range []int32{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("partitions_%d", partitions), func(b *testing.B) {
			benchmarkBifrostRunWithBurst(b, 256, 128, 8, 4, partitions)
		})
	}
}

func benchmarkKafkaRoundTrip(b *testing.B, payloadSize int) {
	brokers := setupBenchRedpanda(b)
	if len(brokers) == 0 {
		b.Fatal("brokers: empty")
	}
	if payloadSize <= 0 {
		b.Fatal("payload size must be positive")
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "bifrost.bench.rt." + suffix

	env := benchCluster(brokers, payloadSize)
	full, err := kafka.FullClusterOpts(&env)
	if err != nil {
		b.Fatalf("cluster opts: %v", err)
	}
	pump, err := kgo.NewClient(append(full, kgo.AllowAutoTopicCreation(), kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		b.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte{0}}); res.FirstErr() != nil {
		b.Fatalf("bootstrap topic: %v", res.FirstErr())
	}

	verify, err := newBenchVerifyClient(brokers, topic, "bench-rt-verify-"+suffix, payloadSize)
	if err != nil {
		b.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	prewarmCtx, prewarmCancel := context.WithTimeout(ctx, benchDrainJoinTimeout)
	defer prewarmCancel()
	if err := drainTopicRecords(prewarmCtx, verify, topic, 1, nil); err != nil {
		b.Fatalf("drain bootstrap: %v", err)
	}

	runDeadline := 30 * time.Minute
	runCtx, runCancel := context.WithTimeout(ctx, runDeadline)
	defer runCancel()

	b.ReportAllocs()
	// Use b.Loop() (Go 1.24+): the benchmark function runs once per measurement. A classic
	// for i := 0; i < b.N; i++ loop re-invokes this entire function for each b.N calibration step,
	// repeating Docker setup, warmup drain, bridge, and smoke — wall time explodes.
	for b.Loop() {
		if res := pump.ProduceSync(runCtx, &kgo.Record{Topic: topic, Value: payload}); res.FirstErr() != nil {
			b.Fatalf("produce: %v", res.FirstErr())
		}
	}

	// Verify at least one produced record is visible to the consumer (full b.N is not worth draining here).
	if b.N > 0 {
		smokeCtx, smokeCancel := context.WithTimeout(ctx, benchSmokeDrainTimeout)
		defer smokeCancel()
		benchProgressf("bifrost benchmark: smoke drain 1 record from %q (≤%s)...\n", topic, benchSmokeDrainTimeout)
		if err := drainTopicRecords(smokeCtx, verify, topic, 1, nil); err != nil {
			b.Fatalf("smoke drain: %v", err)
		}
	}

	b.SetBytes(int64(payloadSize))
}

// benchmarkBridgeRelayWithBurst runs the end-to-end bridge benchmark. burst is the number of
// produces per benchmark iteration (use 1 for per-message latency).
func benchmarkBridgeRelayWithBurst(b *testing.B, payloadSize int, burst int) {
	b.Helper()
	brokers := setupBenchRedpanda(b)
	if len(brokers) == 0 {
		b.Fatal("brokers: empty")
	}
	if payloadSize <= 0 {
		b.Fatal("payload size must be positive")
	}
	if burst <= 0 {
		b.Fatal("burst must be positive")
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "bifrost.bench.from." + suffix
	toTopic := "bifrost.bench.to." + suffix

	env := benchCluster(brokers, payloadSize)
	full, err := kafka.FullClusterOpts(&env)
	if err != nil {
		b.Fatalf("cluster opts: %v", err)
	}
	pump, err := kgo.NewClient(append(full, kgo.AllowAutoTopicCreation(), kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		b.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	// Create to-topic before the bridge runs so the first relay produce does not hit
	// UNKNOWN_TOPIC_OR_PARTITION while metadata/partitions settle (integration also bootstraps to-topic).
	// Pin partition 0 so the seed record and bridge consumer group assignment line up on one partition.
	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: toTopic, Partition: 0, Value: nil}); res.FirstErr() != nil {
		b.Fatalf("bootstrap to-topic: %v", res.FirstErr())
	}
	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: fromTopic, Partition: 0, Value: []byte{0}}); res.FirstErr() != nil {
		b.Fatalf("bootstrap from-topic: %v", res.FirstErr())
	}

	metricsOff := false
	metricsRegistry, err := metrics.NewFromConfig(bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{Enable: &metricsOff},
		Bridges: []bifrostconfig.Bridge{
			{
				Name: "bench",
				From: bifrostconfig.BridgeTarget{Cluster: "bench", Topic: fromTopic},
				To:   bifrostconfig.BridgeTarget{Cluster: "bench", Topic: toTopic},
			},
		},
	})
	if err != nil {
		b.Fatalf("metrics: %v", err)
	}
	defer metricsRegistry.StopServer()

	// Consumer group is required for CommitRecords in pkg/bridge. Cap fetch long-poll and disable
	// fetch sessions so PollFetches returns regularly and ctx cancellation is observable.
	consumer, err := kafka.NewConsumerForBridge(&env, "bench-cg-"+suffix, fromTopic, nil, nil,
		kgo.FetchMaxWait(2*time.Second),
		kgo.DisableFetchSessions(),
	)
	if err != nil {
		b.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&env, nil, nil)
	if err != nil {
		b.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	id := bridge.IdentityFrom(bifrostconfig.Bridge{
		Name: "bench",
		From: bifrostconfig.BridgeTarget{Cluster: "bench", Topic: fromTopic},
		To:   bifrostconfig.BridgeTarget{Cluster: "bench", Topic: toTopic},
	})

	verify, err := newBenchVerifyClient(brokers, toTopic, "bench-verify-"+suffix, payloadSize)
	if err != nil {
		b.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Run(bridgeCtx, id, consumer, producer, metricsRegistry.BridgeMetrics, bridge.RunOptions{
			BatchSize:          bifrostconfig.DefaultBridgeBatchSize,
			MaxInFlightBatches: bifrostconfig.DefaultMaxInFlightBatches,
			CommitInterval:     bifrostconfig.DefaultCommitInterval,
			CommitMaxRecords:   bifrostconfig.DefaultCommitMaxRecords,
		})
	}()

	// Wait until the bridge has relayed the from-topic bootstrap (record has bifrost source headers).
	// Do not count "two records on to-topic": the nil bootstrap is not a relay, and counting
	// records blocked for minutes when the bridge consumer group was slow to assign while verify
	// had already consumed the nil produce.
	warmupCtx, warmupCancel := context.WithTimeout(ctx, benchDrainJoinTimeout)
	defer warmupCancel()
	benchProgressf("bifrost benchmark: warmup wait for first relay on %q (up to %s)...\n", toTopic, benchDrainJoinTimeout)
	if err := waitForBenchRelayOnToTopic(warmupCtx, verify, toTopic, errCh); err != nil {
		b.Fatalf("warmup: %v", err)
	}

	runDeadline := 30 * time.Minute
	runCtx, runCancel := context.WithTimeout(ctx, runDeadline)
	defer runCancel()

	b.ReportAllocs()
	for b.Loop() {
		for j := 0; j < burst; j++ {
			if res := pump.ProduceSync(runCtx, &kgo.Record{Topic: fromTopic, Partition: 0, Value: payload}); res.FirstErr() != nil {
				b.Fatalf("produce: %v", res.FirstErr())
			}
		}
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			b.Fatalf("bridge: %v", err)
		}
	default:
	}
	if b.N*burst > 0 {
		smokeCtx, smokeCancel := context.WithTimeout(ctx, benchSmokeDrainTimeout)
		defer smokeCancel()
		benchProgressf("bifrost benchmark: smoke drain 1 relay from %q (≤%s)...\n", toTopic, benchSmokeDrainTimeout)
		if err := drainTopicRecords(smokeCtx, verify, toTopic, 1, errCh); err != nil {
			b.Fatalf("smoke drain: %v", err)
		}
	}

	// SetBytes records bytes processed per benchmark iteration (one pass of the loop above). The
	// testing package multiplies by b.N when reporting throughput; do not multiply by b.N here or
	// MB/s will scale incorrectly with benchtime (larger b.N → inflated MB/s).
	b.SetBytes(int64(payloadSize * burst))
	if burst > 1 {
		b.ReportMetric(float64(burst), "msgs/iter")
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("bridge: %v", err)
	}
}

func benchmarkBifrostRunWithBurst(b *testing.B, payloadSize int, burst, batchSize, replicas int, fromPartitions int32) {
	brokers := setupBenchRedpanda(b)
	env := benchCluster(brokers, payloadSize)
	benchmarkBifrostRunWithBurstAndCluster(b, env, payloadSize, burst, batchSize, replicas, fromPartitions)
}

func benchmarkBifrostRunWithBurstAndCluster(
	b *testing.B,
	env bifrostconfig.Cluster,
	payloadSize int,
	burst, batchSize, replicas int,
	fromPartitions int32,
) {
	b.Helper()
	brokers := env.Brokers
	if len(brokers) == 0 {
		b.Fatal("brokers: empty")
	}
	if payloadSize <= 0 {
		b.Fatal("payload size must be positive")
	}
	if burst <= 0 {
		b.Fatal("burst must be positive")
	}
	if batchSize <= 0 {
		b.Fatal("batch size must be positive")
	}
	if replicas <= 0 {
		b.Fatal("replicas must be positive")
	}
	if fromPartitions <= 0 {
		b.Fatal("fromPartitions must be positive")
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "bifrost.bench.proc.from." + suffix
	toTopic := "bifrost.bench.proc.to." + suffix

	full, err := kafka.FullClusterOpts(&env)
	if err != nil {
		b.Fatalf("cluster opts: %v", err)
	}
	pump, err := kgo.NewClient(append(full, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		b.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	mustCreateBenchTopic(b, ctx, pump, fromTopic, fromPartitions)
	mustCreateBenchTopic(b, ctx, pump, toTopic, fromPartitions)

	metricsOff := false
	cfg := bifrostconfig.Config{
		Clusters: map[string]bifrostconfig.Cluster{"bench": env},
		Bridges: []bifrostconfig.Bridge{{
			Name:      "bench",
			Replicas:  replicas,
			BatchSize: batchSize,
			From:      bifrostconfig.BridgeTarget{Cluster: "bench", Topic: fromTopic},
			To:        bifrostconfig.BridgeTarget{Cluster: "bench", Topic: toTopic},
		}},
		Metrics: bifrostconfig.Metrics{Enable: &metricsOff},
		Logging: bifrostconfig.Logging{
			Level:                 "info",
			Format:                "json",
			Stream:                "stdout",
			PeriodicStatsInterval: "0",
		},
	}

	verify, err := newBenchVerifyClient(brokers, toTopic, "bench-bifrost-proc-verify-"+suffix, payloadSize)
	if err != nil {
		b.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	runCtx, runCancel := context.WithTimeout(ctx, 30*time.Minute)
	defer runCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bifrost.Run(runCtx, cfg)
	}()

	for partition := int32(0); partition < fromPartitions; partition++ {
		seed := append([]byte(nil), payload...)
		seed = append(seed, byte(partition))
		if res := pump.ProduceSync(runCtx, &kgo.Record{
			Topic:     fromTopic,
			Partition: partition,
			Value:     seed,
		}); res.FirstErr() != nil {
			b.Fatalf("seed from-topic partition %d: %v", partition, res.FirstErr())
		}
	}

	prewarmCtx, prewarmCancel := context.WithTimeout(ctx, benchDrainJoinTimeout)
	defer prewarmCancel()
	benchProgressf("bifrost process benchmark: warmup drain %d records from %q (≤%s)...\n", fromPartitions, toTopic, benchDrainJoinTimeout)
	if err := drainTopicRecords(prewarmCtx, verify, toTopic, int(fromPartitions), errCh); err != nil {
		b.Fatalf("warmup drain: %v", err)
	}

	b.ReportAllocs()
	var produced uint64
	for b.Loop() {
		for j := 0; j < burst; j++ {
			partition := int32(produced % uint64(fromPartitions))
			produced++
			if res := pump.ProduceSync(runCtx, &kgo.Record{
				Topic:     fromTopic,
				Partition: partition,
				Value:     payload,
			}); res.FirstErr() != nil {
				b.Fatalf("produce: %v", res.FirstErr())
			}
		}
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			b.Fatalf("bifrost.Run: %v", err)
		}
	default:
	}
	if b.N*burst > 0 {
		smokeCtx, smokeCancel := context.WithTimeout(ctx, benchSmokeDrainTimeout)
		defer smokeCancel()
		benchProgressf("bifrost process benchmark: smoke drain 1 relay from %q (≤%s)...\n", toTopic, benchSmokeDrainTimeout)
		if err := drainTopicRecords(smokeCtx, verify, toTopic, 1, errCh); err != nil {
			b.Fatalf("smoke drain: %v", err)
		}
	}

	b.SetBytes(int64(payloadSize * burst))
	if burst > 1 {
		b.ReportMetric(float64(burst), "msgs/iter")
	}

	runCancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("bifrost.Run: %v", err)
	}
}

func mustCreateBenchTopic(b *testing.B, ctx context.Context, cl *kgo.Client, topic string, partitions int32) {
	b.Helper()
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		b.Fatalf("create topic %q: %v", topic, err)
	}
	for _, result := range resp.Sorted() {
		if result.Err != nil && !errors.Is(result.Err, kerr.TopicAlreadyExists) {
			b.Fatalf("create topic %q: %v", topic, result.Err)
		}
	}
}

// waitForBenchRelayOnToTopic blocks until a record produced by pkg/bridge appears on toTopic
// (identified by bifrost source headers). This avoids counting raw topic records: the pump's
// nil bootstrap to to-topic is not a relay and must not be confused with the relayed seed.
func waitForBenchRelayOnToTopic(ctx context.Context, cl *kgo.Client, toTopic string, errCh <-chan error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if errCh != nil {
			select {
			case err := <-errCh:
				if err != nil && !errors.Is(err, context.Canceled) {
					return fmt.Errorf("bridge exited: %w", err)
				}
			default:
			}
		}
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		for _, r := range fetches.Records() {
			if r.Topic != toTopic {
				continue
			}
			for _, h := range r.Headers {
				if h.Key == bridge.HeaderSourceCluster {
					return nil
				}
			}
		}
	}
}

func drainTopicRecords(ctx context.Context, cl *kgo.Client, topic string, want int, errCh <-chan error) error {
	received := 0
	for received < want {
		if err := ctx.Err(); err != nil {
			return err
		}
		if errCh != nil {
			select {
			case err := <-errCh:
				if err != nil && !errors.Is(err, context.Canceled) {
					return fmt.Errorf("bridge exited: %w", err)
				}
			default:
			}
		}
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		for _, r := range fetches.Records() {
			if r.Topic == topic {
				received++
			}
		}
	}
	return nil
}
