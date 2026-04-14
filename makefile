# All targets are phony (no file named "build", "test", etc. should shadow these).
.PHONY: bench bench-profile-block bench-profile-cpu bench-profile-mem bench-profile-trace \
	build build-docker format help lint test test-coverage test-integration

help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@printf '  %-30s %s\n' 'build' 'Build bifrost (./cmd/bifrost)'
	@printf '  %-30s %s\n' 'build-docker' 'Build Docker image (bifrost:latest)'
	@printf '  %-30s %s\n' 'lint' 'go vet, go mod verify, govulncheck/gosec (go tool), golangci-lint'
	@printf '  %-30s %s\n' 'format' 'go fmt + gofmt -w'
	@printf '  %-30s %s\n' 'test' 'Unit tests (./test/unit/...)'
	@printf '  %-30s %s\n' 'test-integration' 'Integration tests (BIFROST_INTEGRATION=1; ./test/integration/...)'
	@printf '  %-30s %s\n' 'test-coverage' 'All tests, coverage.out + HTML (-coverpkg for pkg/ and cmd/)'
	@printf '  %-30s %s\n' 'bench' 'Benchmarks (BIFROST_BENCHMARK=1 for Docker bridge; ./test/benchmark/...)'
	@printf '  %-30s %s\n' 'bench-profile-cpu' 'Record CPU profile for one benchmark (override BENCH=...)'
	@printf '  %-30s %s\n' 'bench-profile-cpu-inspect' 'Open CPU profile in browser (pprof -http=:5432)'
	@printf '  %-30s %s\n' 'bench-profile-mem' 'Record heap profile for one benchmark'
	@printf '  %-30s %s\n' 'bench-profile-mem-inspect' 'Open heap profile in browser (pprof -http=:5432)'
	@printf '  %-30s %s\n' 'bench-profile-trace' 'Record execution trace for one benchmark'
	@printf '  %-30s %s\n' 'bench-profile-trace-inspect' 'Open trace (go tool trace)'
	@printf '  %-30s %s\n' 'bench-profile-block' 'Record block profile (sched/sync contention)'
	@printf '  %-30s %s\n' 'bench-profile-block-inspect' 'Open block profile in browser (pprof -http=:5432)'
	@printf '  %-30s %s\n' 'help' 'Show this message'

lint:
	go vet ./...
	go mod verify
	go tool govulncheck ./...
	go tool gosec -fmt text -stdout -quiet ./...
	golangci-lint run ./...

format:
	go fmt ./...
	gofmt -w .

# Optional local revision/time for cmd/bifrost/version (matches CI-style ldflags).
REV := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

build:
	go build -trimpath -ldflags "-s -w -X github.com/lolocompany/bifrost/cmd/bifrost/version.Revision=$(REV) -X github.com/lolocompany/bifrost/cmd/bifrost/version.BuildTime=$(BUILD_TIME)" -o bifrost ./cmd/bifrost

build-docker:
	docker build -t bifrost:local . --build-arg VERSION=local-dev-$(REV) --build-arg REVISION=$(REV) --build-arg BUILD_TIME=$(BUILD_TIME)

test:
	go test -v ./test/unit/...

test-integration:
	BIFROST_INTEGRATION=1 go test -v ./test/integration/...

# Tests live under test/..., so default -cover only sees external test packages (no pkg statements) → 0%.
# -coverpkg instruments pkg/ and cmd/ when those packages are exercised from test/ packages.
test-coverage:
	BIFROST_INTEGRATION=1 go test -coverprofile=coverage.out -coverpkg=./pkg/... ./test/...
	go tool cover -html=coverage.out

bench:
	BIFROST_BENCHMARK=1 go test -bench=. -benchmem -benchtime=5s -timeout=30m ./test/benchmark/...

# Default benchmark for profiling (override: make bench-profile-cpu BENCH=BenchmarkKafkaRoundTrip256B).
BENCH ?= BenchmarkBridgeRelay256B

bench-profile-cpu:
	BIFROST_BENCHMARK=1 go test -bench=$(BENCH) -benchmem -benchtime=5s -timeout=30m -cpuprofile=test/benchmark/.prof/cpu.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/cpu.prof

bench-profile-block:
	BIFROST_BENCHMARK=1 go test -bench=$(BENCH) -benchmem -benchtime=5s -timeout=30m -blockprofile=test/benchmark/.prof/block.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/block.prof

bench-profile-trace:
	BIFROST_BENCHMARK=1 go test -bench=$(BENCH) -benchmem -benchtime=3s -timeout=30m -trace=test/benchmark/.prof/trace.out 	./test/benchmark/...
	go tool trace test/benchmark/.prof/trace.out

bench-profile-mem:
	BIFROST_BENCHMARK=1 go test -bench=$(BENCH) -benchmem -benchtime=5s -timeout=30m -memprofile=test/benchmark/.prof/mem.prof 	./test/benchmark/...
	go tool pprof -http=:5432 test/benchmark/.prof/mem.prof
