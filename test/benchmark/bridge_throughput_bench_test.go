package benchmark_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	pump, err := kgo.NewClient(append(full, kgo.AllowAutoTopicCreation())...)
	if err != nil {
		b.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte{0}}); res.FirstErr() != nil {
		b.Fatalf("bootstrap topic: %v", res.FirstErr())
	}

	base, err := kafka.ClientOpts(&env)
	if err != nil {
		b.Fatalf("verify base opts: %v", err)
	}
	cons, err := kafka.ConsumerClusterOpts(&env)
	if err != nil {
		b.Fatalf("verify consumer opts: %v", err)
	}
	verifyOpts := append(append(base, cons...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup("bench-rt-verify-"+suffix))
	verify, err := kgo.NewClient(verifyOpts...)
	if err != nil {
		b.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	if err := drainTopicRecords(ctx, verify, topic, 1); err != nil {
		b.Fatalf("drain bootstrap: %v", err)
	}

	runDeadline := 30 * time.Minute
	runCtx, runCancel := context.WithTimeout(ctx, runDeadline)
	defer runCancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res := pump.ProduceSync(runCtx, &kgo.Record{Topic: topic, Value: payload}); res.FirstErr() != nil {
			b.Fatalf("produce: %v", res.FirstErr())
		}
	}
	if err := drainTopicRecords(runCtx, verify, topic, b.N); err != nil {
		b.Fatalf("drain: %v", err)
	}
	b.StopTimer()

	b.SetBytes(int64(payloadSize))
}

// benchmarkBridgeRelayWithBurst runs the end-to-end bridge benchmark. burst is the number of
// produces per benchmark iteration (use 1 for per-message latency). drainTopicRecords receives
// b.N*burst total records on the destination topic.
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
	pump, err := kgo.NewClient(append(full, kgo.AllowAutoTopicCreation())...)
	if err != nil {
		b.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: toTopic, Value: nil}); res.FirstErr() != nil {
		b.Fatalf("bootstrap to-topic: %v", res.FirstErr())
	}
	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: fromTopic, Value: []byte{0}}); res.FirstErr() != nil {
		b.Fatalf("bootstrap from-topic: %v", res.FirstErr())
	}

	metricsOff := false
	reg := prometheus.NewRegistry()
	m, _, err := metrics.New(reg, bifrostconfig.Metrics{Enable: &metricsOff}, []bifrostconfig.Bridge{
		{
			Name: "bench",
			From: bifrostconfig.BridgeTarget{Cluster: "bench", Topic: fromTopic},
			To:   bifrostconfig.BridgeTarget{Cluster: "bench", Topic: toTopic},
		},
	})
	if err != nil {
		b.Fatalf("metrics: %v", err)
	}

	consumer, err := kafka.NewConsumerForBridge(&env, "bench-cg-"+suffix, fromTopic, nil)
	if err != nil {
		b.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&env, nil)
	if err != nil {
		b.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	id := bridge.IdentityFrom(bifrostconfig.Bridge{
		Name: "bench",
		From: bifrostconfig.BridgeTarget{Cluster: "bench", Topic: fromTopic},
		To:   bifrostconfig.BridgeTarget{Cluster: "bench", Topic: toTopic},
	})

	base, err := kafka.ClientOpts(&env)
	if err != nil {
		b.Fatalf("verify base opts: %v", err)
	}
	cons, err := kafka.ConsumerClusterOpts(&env)
	if err != nil {
		b.Fatalf("verify consumer opts: %v", err)
	}
	verifyOpts := append(append(base, cons...), kgo.ConsumeTopics(toTopic), kgo.ConsumerGroup("bench-verify-"+suffix))
	verify, err := kgo.NewClient(verifyOpts...)
	if err != nil {
		b.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	// Drain bootstrap record so the timed section only counts relayed messages.
	if err := drainTopicRecords(ctx, verify, toTopic, 1); err != nil {
		b.Fatalf("drain bootstrap: %v", err)
	}

	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Run(bridgeCtx, id, consumer, producer, m, 0)
	}()

	runDeadline := 30 * time.Minute
	runCtx, runCancel := context.WithTimeout(ctx, runDeadline)
	defer runCancel()

	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < burst; j++ {
			if res := pump.ProduceSync(runCtx, &kgo.Record{Topic: fromTopic, Value: payload}); res.FirstErr() != nil {
				b.Fatalf("produce: %v", res.FirstErr())
			}
		}
	}
	if err := drainTopicRecords(runCtx, verify, toTopic, b.N*burst); err != nil {
		b.Fatalf("drain: %v", err)
	}
	b.StopTimer()

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

func drainTopicRecords(ctx context.Context, cl *kgo.Client, topic string, want int) error {
	received := 0
	for received < want {
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
