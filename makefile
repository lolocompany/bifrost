# All targets are phony (no file named "build", "test", etc. should shadow these).
.PHONY: bench bench-full \
	build build-docker codequality format help lint \
	test test-coverage test-integration test-race test-regression test-unit
	
# Source code paths, for use in linting and codequality targets.
SOURCE_CODE ?= ./cmd/... ./pkg/...

# Isolated Redpanda per benchmark: wall time grows with container churn; allow a generous cap.
BENCH_PATTERN ?= BenchmarkBridgeRelay256B|BenchmarkKafkaRoundTrip256B|BenchmarkBridgeRelayBurst256B
BENCH_TIME ?= 2s
BENCH_TIMEOUT ?= 30m
# Set to empty or a higher value if you want more OS threads during a benchmark (default 1 = sequential CPU).
BENCH_GOMAXPROCS ?= 1

# Optional local revision/time for cmd/bifrost/version (matches CI-style ldflags).
REV := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

help: ## Show available make targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "%-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build bifrost binary from ./cmd/bifrost
	go build -trimpath -ldflags "-s -w -X github.com/lolocompany/bifrost/cmd/bifrost/version.Revision=$(REV) -X github.com/lolocompany/bifrost/cmd/bifrost/version.BuildTime=$(BUILD_TIME)" -o bifrost ./cmd/bifrost

format: ## Run go fmt and gofmt
	go fmt ./...
	gofmt -w .

lint: ## Run go vet, module verify, static/security/style checks
	go vet ./...
	go mod verify
	go tool staticcheck $(SOURCE_CODE)
	go tool govulncheck $(SOURCE_CODE)
	go tool gosec -fmt text -stdout -quiet $(SOURCE_CODE)
	golangci-lint run $(SOURCE_CODE)
	go tool errcheck $(SOURCE_CODE)
	go tool revive -config revive.toml -formatter stylish $(SOURCE_CODE)

complexity-explorer: ## Run complexity explorer
	go tool complexity-explorer

codequality: ## Run codequality checks and print color text report
	@bash scripts/codequality_report.sh


test: test-unit ## Alias for test-unit

test-unit: ## Run unit tests (./test/unit/...)
	go test -shuffle=on -timeout 120s ./test/unit/...

test-race: ## Run unit tests with race detector
	go test -race -shuffle=on -timeout 180s ./test/unit/...

test-integration: ## Run integration tests (BIFROST_INTEGRATION=1)
	BIFROST_INTEGRATION=1 go test -shuffle=on -timeout 120s ./test/integration/...

test-regression: ## Run config regression tests (BIFROST_INTEGRATION=1)
	BIFROST_INTEGRATION=1 go test -shuffle=on -timeout 300s ./test/regression/...

# Tests live under test/..., so default -cover only sees external test packages (no pkg statements) → 0%.
# -coverpkg instruments pkg/ and cmd/ when those packages are exercised from test/ packages.
test-coverage: ## Run tests with coverage output and HTML report
	BIFROST_INTEGRATION=1 go test -coverprofile=coverage.out -coverpkg='$(SOURCE_CODE)' ./test/...
	go tool cover -html=coverage.out

# GOMAXPROCS=$(BENCH_GOMAXPROCS) and -p 1: one package worker; default one OS thread for stable CPU.
# Benchmark lines go to stdout; slog and other diagnostics go to stderr. Keep them separate so
# go tool benchstat can parse bench.txt (merged stderr breaks benchmark line continuations).
bench: ## Run benchmark subset (Docker-backed)
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -p 1 -bench='$(BENCH_PATTERN)' -benchmem -benchtime=$(BENCH_TIME) -timeout=$(BENCH_TIMEOUT) ./test/benchmark/... 2>bench.err | tee bench.txt
	go tool benchstat bench.txt

# Run all benchmarks 6 times.
bench-full: ## Run all benchmarks with count=6
	BIFROST_BENCHMARK=1 GOMAXPROCS=$(BENCH_GOMAXPROCS) go test -count=6 -p 1 -bench=. -benchmem -benchtime=$(BENCH_TIME) -timeout=$(BENCH_TIMEOUT) ./test/benchmark/... 2>bench.err | tee bench.txt
	go tool benchstat bench.txt

