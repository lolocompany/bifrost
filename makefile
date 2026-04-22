# All targets are phony (no file named "build", "test", etc. should shadow these).
.PHONY: bench bench-full bench-profile-block bench-profile-cpu bench-profile-mem bench-profile-trace \
	build build-release build-docker codequality-baseline codequality-gate codequality-review codequality-scorecard format help lint \
	test test-coverage test-integration

# Isolated Redpanda per benchmark: wall time grows with container churn; allow a generous cap.
BENCH_PATTERN ?= BenchmarkBridgeRelay256B|BenchmarkKafkaRoundTrip256B|BenchmarkBridgeRelayBurst256B
BENCH_TIME ?= 2s
BENCH_TIMEOUT ?= 30m
# Set to empty or a higher value if you want more OS threads during a benchmark (default 1 = sequential CPU).
BENCH_GOMAXPROCS ?= 1

# Optional local revision/time for cmd/bifrost/version (matches CI-style ldflags).
REV := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@printf '  %-30s %s\n' 'bench' 'Benchmarks (Docker; default subset BENCH_PATTERN; BENCH_TIME=$(BENCH_TIME); timeout $(BENCH_TIMEOUT))'
	@printf '  %-30s %s\n' 'bench-full' 'All benchmarks, -count=6 (slow); bench.txt=stdout only (for benchstat); bench.err=logs'
	@printf '  %-30s %s\n' 'bench-profile-block' 'Block profile + pprof -http (override BENCH=...)'
	@printf '  %-30s %s\n' 'bench-profile-cpu' 'CPU profile + pprof -http (override BENCH=...)'
	@printf '  %-30s %s\n' 'bench-profile-mem' 'Heap profile + pprof -http (override BENCH=...)'
	@printf '  %-30s %s\n' 'bench-profile-trace' 'Execution trace + go tool trace (override BENCH=...)'
	@printf '  %-30s %s\n' 'build' 'Build bifrost (./cmd/bifrost)'
	@printf '  %-30s %s\n' 'build-release' 'GoReleaser snapshot to dist/ (needs goreleaser + syft; no publish)'
	@printf '  %-30s %s\n' 'build-docker' 'Build Docker image (bifrost:latest)'
	@printf '  %-30s %s\n' 'format' 'go fmt + gofmt -w'
	@printf '  %-30s %s\n' 'help' 'Show this message'
	@printf '  %-30s %s\n' 'lint' 'go vet, go mod verify, govulncheck/gosec (go tool), golangci-lint'
	@printf '  %-30s %s\n' 'codequality-scorecard' 'Build codequality scorecard JSON/Markdown'
	@printf '  %-30s %s\n' 'codequality-baseline' 'Capture baseline codequality snapshot for regression gating'
	@printf '  %-30s %s\n' 'codequality-gate' 'Run codequality gate checks (absolute + regression)'
	@printf '  %-30s %s\n' 'codequality-review' 'Generate scorecard and print top hotspots for weekly triage'
	@printf '  %-30s %s\n' 'test' 'Unit tests (./test/unit/...)'
	@printf '  %-30s %s\n' 'test-coverage' 'Integration + unit tests; coverage.out + HTML (-coverpkg ./pkg/...; ./test/...)'
	@printf '  %-30s %s\n' 'test-integration' 'Integration tests (BIFROST_INTEGRATION=1; ./test/integration/...)'

lint:
	go vet ./...
	go mod verify
	go tool govulncheck ./...
	go tool gosec -fmt text -stdout -quiet ./...
	golangci-lint run ./...

codequality-scorecard:
	python3 scripts/codequality_pipeline.py --output-prefix scorecard

codequality-baseline:
	python3 scripts/codequality_pipeline.py --output-prefix baseline-scorecard --write-baseline

codequality-gate:
	python3 scripts/codequality_pipeline.py --output-prefix gate-scorecard --enforce

codequality-review:
	python3 scripts/codequality_pipeline.py --output-prefix weekly-scorecard
	@echo "Top hotspots (churn x complexity):"
	@python3 -c 'import json; from pathlib import Path; report=Path("reports/codequality/weekly-scorecard.json"); data=json.loads(report.read_text(encoding="utf-8")); [print(f"- {r[\"risk\"]:>5} {r[\"file\"]} (churn={r[\"churn_180d\"]}, cyclo={r[\"cyclomatic_sum\"]}, cogn={r[\"cognitive_sum\"]})") for r in data.get("hotspots", [])[:10]]'

format:
	go fmt ./...
	gofmt -w .

build:
	go build -trimpath -ldflags "-s -w -X github.com/lolocompany/bifrost/cmd/bifrost/version.Revision=$(REV) -X github.com/lolocompany/bifrost/cmd/bifrost/version.BuildTime=$(BUILD_TIME)" -o bifrost ./cmd/bifrost

# Env vars match goreleaser/.goreleaser.yaml (CI sets these in the release workflow).
# Requires https://github.com/anchore/syft on PATH for SBOM generation.
build-release:
	@command -v syft >/dev/null 2>&1 || { echo >&2 "syft not on PATH (install: https://github.com/anchore/syft#installation)"; exit 1; }
	@command -v goreleaser >/dev/null 2>&1 || { echo >&2 "goreleaser not on PATH (install: https://goreleaser.com/install/)"; exit 1; }
	BIFROST_BUILD_TIME=$(BUILD_TIME) RELEASE_NAME=local-snapshot RELEASE_BODY='Local snapshot (not a production release).' goreleaser release --snapshot --clean --skip=publish,validate --config goreleaser/.goreleaser.yaml

test:
	go test -shuffle=on -timeout 120s ./test/unit/...

test-integration:
	BIFROST_INTEGRATION=1 go test -shuffle=on -timeout 120s ./test/integration/...

# Tests live under test/..., so default -cover only sees external test packages (no pkg statements) → 0%.
# -coverpkg instruments pkg/ and cmd/ when those packages are exercised from test/ packages.
test-coverage:
	BIFROST_INTEGRATION=1 go test -coverprofile=coverage.out -coverpkg=./pkg/... ./test/...
	go tool cover -html=coverage.out

# GOMAXPROCS=$(BENCH_GOMAXPROCS) and -p 1: one package worker; default one OS thread for stable CPU.
# Benchmark lines go to stdout; slog and other diagnostics go to stderr. Keep them separate so
# go tool benchstat can parse bench.txt (merged stderr breaks benchmark line continuations).
bench:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench='$(BENCH_PATTERN)' -benchmem -benchtime=$(BENCH_TIME) -timeout=$(BENCH_TIMEOUT) ./test/benchmark/... 2>bench.err | tee bench.txt
	go tool benchstat bench.txt

# Run all benchmarks 6 times.
bench-full:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -count=6 -p 1 -bench=. -benchmem -benchtime=$(BENCH_TIME) -timeout=$(BENCH_TIMEOUT) ./test/benchmark/... 2>bench.err | tee bench.txt
	go tool benchstat bench.txt

bench-profile-cpu:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench=$(BENCH_PATTERN) -benchmem -benchtime=10s -timeout=$(BENCH_TIMEOUT) -cpuprofile=test/benchmark/.prof/cpu.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/cpu.prof

bench-profile-block:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench=$(BENCH_PATTERN) -benchmem -benchtime=10s -timeout=$(BENCH_TIMEOUT) -blockprofile=test/benchmark/.prof/block.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/block.prof

bench-profile-trace:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench=$(BENCH_PATTERN) -benchmem -benchtime=10s -timeout=$(BENCH_TIMEOUT) -trace=test/benchmark/.prof/trace.out 	./test/benchmark/...
	go tool trace test/benchmark/.prof/trace.out

bench-profile-mem:
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench=$(BENCH_PATTERN) -benchmem -benchtime=10s -timeout=$(BENCH_TIMEOUT) -memprofile=test/benchmark/.prof/mem.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/mem.prof
