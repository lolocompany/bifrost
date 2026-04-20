// Package benchmark_test contains micro-benchmarks and optional Docker-backed bridge benchmarks.
//
// Heavy benchmarks require Docker (Testcontainers) and opt-in:
//
//	BIFROST_BENCHMARK=1 go test -bench=. -benchmem ./test/benchmark/...
//
// or `make bench` (default: small benchmark subset; `make bench-full` for the full suite).
//
// Each benchmark that calls setupBenchRedpanda gets its own Redpanda container, started before
// the benchmark body and terminated in b.Cleanup when the benchmark returns—no shared broker
// across benchmarks, so results are not order-dependent. The first image pull can still take
// several minutes on a cold machine; progress lines go to stderr only when BIFROST_BENCHMARK_VERBOSE=1.
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
	"fmt"
	"log/slog"
	"os"
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

func requireBenchmark(b *testing.B) {
	b.Helper()
	if os.Getenv("BIFROST_BENCHMARK") != "1" {
		b.Skip("set BIFROST_BENCHMARK=1 to run Docker-backed bridge benchmarks (see makefile target bench)")
	}
}

// benchProgressf writes progress to stderr only when BIFROST_BENCHMARK_VERBOSE=1.
func benchProgressf(format string, args ...any) {
	if os.Getenv("BIFROST_BENCHMARK_VERBOSE") != "1" {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}

// setupBenchRedpanda starts a dedicated Redpanda node for this benchmark and registers b.Cleanup
// to terminate it when the benchmark finishes. Benchmarks do not share a broker, so results are
// not sensitive to execution order relative to other benchmarks.
func setupBenchRedpanda(b *testing.B) []string {
	b.Helper()
	requireBenchmark(b)
	skipIfDockerUnhealthy(b)

	ctx := context.Background()
	benchProgressf("bifrost benchmark: starting Redpanda (Docker); first run may take several minutes while the image is pulled...\n")
	ctr, err := redpanda.Run(ctx, benchRedpandaImage,
		redpanda.WithAutoCreateTopics(),
		// Default cluster batch cap is 1 MiB; large-record benchmarks need a higher ceiling (topic max.message.bytes inherits from this when unset).
		redpanda.WithBootstrapConfig("kafka_batch_max_bytes", benchBrokerMaxMessageBytes),
	)
	if err != nil {
		b.Fatalf("redpanda: %v", err)
	}
	b.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			slog.Error("benchmark redpanda teardown", "error_message", err)
		}
	})

	seed, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		b.Fatalf("KafkaSeedBroker: %v", err)
	}
	benchProgressf("bifrost benchmark: Redpanda ready at %s\n", seed)
	return []string{seed}
}
