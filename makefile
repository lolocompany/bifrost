.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@echo "  build              Build bifrost (./cmd/bifrost)"
	@echo "  test               Unit tests (./test/unit/...)"
	@echo "  bench              Benchmarks (BIFROST_BENCHMARK=1 for Docker bridge throughput; ./test/benchmark/...)"
	@echo "  test-integration   Integration tests (BIFROST_INTEGRATION=1; ./test/integration/...)"
	@echo "  lint               go vet + golangci-lint"
	@echo "  format             go fmt + gofmt -w"
	@echo "  help               Show this message"

.PHONY: build
build:
	go build -o bifrost ./cmd/bifrost

.PHONY: test
test:
	go test -v ./test/unit/...

.PHONY: bench
bench:
	BIFROST_BENCHMARK=1 go test -bench=. -benchmem -benchtime=5s -timeout=30m ./test/benchmark/...

.PHONY: test-integration
test-integration:
	BIFROST_INTEGRATION=1 go test -v ./test/integration/...

.PHONY: lint
lint:
	go vet ./...
	golangci-lint run ./...

.PHONY: format
format:
	go fmt ./...
	gofmt -w .
