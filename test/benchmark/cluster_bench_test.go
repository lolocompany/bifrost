// Package benchmark_test contains micro-benchmarks and optional Docker-backed bridge benchmarks.
//
// Heavy benchmarks require Docker (Testcontainers) and opt-in:
//
//	BIFROST_BENCHMARK=1 go test -bench=. -benchmem ./test/benchmark/...
//
// or `make bench`.
//
// Interpreting results:
//   - BenchmarkKafkaRoundTrip* — produce + consume one topic (no bridge). Compare to
//     BenchmarkBridgeRelay* at the same size: the gap isolates relay work (second produce,
//     commit, record copy).
//   - BenchmarkBridgeRelayBurst* — several messages per iteration; higher MB/s can indicate
//     amortized fixed costs vs single-message BenchmarkBridgeRelay*.
//
// Profiling (CPU, heap, trace, block): make bench-profile-cpu and related targets; override the
// benchmark with BENCH=BenchmarkKafkaRoundTrip256B. Use benchstat to compare runs:
//
//	go test -bench=BenchmarkBridgeRelay256B -benchtime=5s -count=5 ./test/benchmark/... > /tmp/a.txt
//	benchstat /tmp/a.txt /tmp/b.txt
package benchmark_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// skipIfDockerUnhealthy mirrors testcontainers.SkipIfProviderIsNotHealthy but for *testing.B
// (Testcontainers only ships the *testing.T variant).
func skipIfDockerUnhealthy(b *testing.B) {
	b.Helper()
	ctx := context.Background()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		b.Skipf("docker unavailable for benchmarks: %v", err)
	}
	if err := provider.Health(ctx); err != nil {
		b.Skipf("docker unhealthy for benchmarks: %v", err)
	}
}

const benchRedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.3.11"

// benchBrokerMaxMessageBytes is the upper bound for benchmark payloads (see BenchmarkBridgeRelay10MiB) plus margin.
// Redpanda defaults are too small for multi-megabyte records; these cluster properties raise broker limits.
const benchBrokerMaxMessageBytes = 32 << 20

var (
	benchClusterOnce sync.Once
	benchBrokers     []string
	benchTerminate   func(context.Context, ...testcontainers.TerminateOption) error
)

func requireBenchmark(b *testing.B) {
	b.Helper()
	if os.Getenv("BIFROST_BENCHMARK") != "1" {
		b.Skip("set BIFROST_BENCHMARK=1 to run Docker-backed bridge benchmarks (see makefile target bench)")
	}
}

// setupBenchRedpanda starts a single Redpanda node once per test process (shared by all bridge benchmarks).
func setupBenchRedpanda(b *testing.B) []string {
	b.Helper()
	requireBenchmark(b)
	skipIfDockerUnhealthy(b)

	benchClusterOnce.Do(func() {
		ctx := context.Background()
		ctr, err := redpanda.Run(ctx, benchRedpandaImage,
			redpanda.WithAutoCreateTopics(),
			// Default cluster batch cap is 1 MiB; large-record benchmarks need a higher ceiling (topic max.message.bytes inherits from this when unset).
			redpanda.WithBootstrapConfig("kafka_batch_max_bytes", benchBrokerMaxMessageBytes),
		)
		if err != nil {
			b.Fatalf("redpanda: %v", err)
		}
		benchTerminate = ctr.Terminate
		seed, err := ctr.KafkaSeedBroker(ctx)
		if err != nil {
			b.Fatalf("KafkaSeedBroker: %v", err)
		}
		benchBrokers = []string{seed}
	})
	return benchBrokers
}

func TestMain(m *testing.M) {
	code := m.Run()
	if benchTerminate != nil {
		_ = benchTerminate(context.Background())
	}
	os.Exit(code)
}
