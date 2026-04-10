# bifrost

Banner logo that says "Bifrost: Kafka Cluster Bridge" with a rainbow bridging the gap between two clusters

**bifrost** is a configurable Kafka replication service for moving records between topics and clusters using declarative YAML configurations. It is designed for teams that need a lightweight, operationally simple way to build reliable cross-cluster relay pipelines without writing custom consumer/producer code for every path.

**Observability**:

- **Logs:** Structured log lines (JSON or logfmt), with configurable properties and target streams.
- **Metrics:** Prometheus endpoint (default `**http://0.0.0.0:9090/metrics`\*\*, configurable with `metrics.listen_addr`) with `bifrost_` application series plus standard `go_*` / `process_*` collector series.

**Runtime requirement:** at least one **Kafka-compatible** broker (Apache Kafka, Redpanda, etc.) reachable from the host or container running bifrost.

---

## Using the CLI

### Install or download

**Download prebuilt binaries:** [GitHub Releases](https://github.com/lolocompany/bifrost/releases) — the release workflow publishes cross-compiled `**bifrost`**artifacts for Linux, macOS, and Windows (**amd64** and**arm64\*\*) plus checksums.

**Install with the Go toolchain:**

```bash
go install github.com/lolocompany/bifrost/cmd/bifrost@latest
```

**Build from source:**

```bash
make build   # writes ./bifrost
# or
go build -o bifrost ./cmd/bifrost
```

### Running the CLI

```bash
./bifrost --config /path/to/bifrost.yaml
# or
./bifrost -c /path/to/bifrost.yaml
```

If you omit `**--config**` / `**-c**`, the default path is `**bifrost.yaml**` in the **current working directory**. If that file is missing or invalid, bifrost exits with an error.

A full multi-cluster example lives in `[example.config.yaml](./example.config.yaml)`. Minimal shape:

```yaml
clusters:
  dev:
    brokers:
      - '127.0.0.1:9092'
  prod:
    brokers:
      - '127.0.0.1:9094'

bridges:
  - name: a-to-b
    from:
      cluster: dev
      topic: incoming
    to:
      cluster: prod
      topic: outgoing
```

### Development

| Command                 | Purpose                                                                            |
| ----------------------- | ---------------------------------------------------------------------------------- |
| `make build`            | Build `./bifrost` from source                                                      |
| `make test`             | Run unit tests (`./test/unit/...`)                                                 |
| `make test-integration` | Run Docker-backed integration tests (`BIFROST_INTEGRATION=1`)                      |
| `make bench`            | Run benchmarks (`./test/benchmark/...`, Docker-backed throughput benches included) |
| `make lint`             | Run `go vet` and `golangci-lint`                                                   |
| `make format`           | Run `go fmt` and `gofmt`                                                           |

---

## Metrics

When `metrics.enabled` is true (default), bifrost serves Prometheus metrics on `/metrics` at `metrics.listen_addr` (default `:9090`).

Metric groups are controlled by `metrics.groups` and default to enabled when omitted:

- `forward`, `errors`, `latency` — together control the `bifrost_relay_*` success, error, and duration metrics for each bridge
- `kafka` — broker hook metrics per cluster (connect, E2E bytes/errors/latency, throttling)
- `tls` — TLS handshake and peer certificate metrics per cluster
- `golang`, `process`, `tcp` — runtime/platform collectors

Application metric names are prefixed with `bifrost_`; runtime collector metrics keep standard names (`go_*`, `process_*`). You can also set `metrics.extra_labels` to attach constant labels to all exported series.

For full metric-by-metric tables (name, labels, explanation), see [Metrics Deep Dive](./docs/metrics.md).
