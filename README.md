# bifrost

![Banner logo that says "Bifrost: Kafka Cluster Bridge" with a rainbow bridging the gap between two clusters](./assets/banner_logo.jpg)

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
# or
BIFROST_CONFIG=/path/to/bifrost.yaml ./bifrost
```

If you omit `**--config**` / `**-c**` and do not set **`BIFROST_CONFIG`**, the default path is `**bifrost.yaml**` in the **current working directory**. If that file is missing or invalid, bifrost exits with an error.

### Docker

The [Dockerfile](./Dockerfile) image runs `bifrost` with working directory `/app`. **When using this image, prefer setting `BIFROST_CONFIG`** to the path of your YAML inside the container (for example after bind-mounting or copying the file). That keeps the config location explicit and avoids relying on default filenames or extra container command arguments.

```bash
docker run --rm \
  -e BIFROST_CONFIG=/app/bifrost.yaml \
  -v /path/on/host/bifrost.yaml:/app/bifrost.yaml:ro \
  ghcr.io/lolocompany/bifrost:latest
```

Replace the image tag with the version you pull from [GitHub Releases](https://github.com/lolocompany/bifrost/releases) (the release workflow publishes the container to GHCR alongside the binaries).

### Config example

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

## Downstream deduplication (source headers)

Every relayed record includes **source coordinate** headers so consumers can treat deliveries as **at-least-once** and still process each logical message once. The bridge always sets these; they appear **before** any headers copied from the source record.

| Header                     | Value                                                    |
| -------------------------- | -------------------------------------------------------- |
| `bifrost.source.cluster`   | Source cluster name (UTF-8 string, from bridge identity) |
| `bifrost.source.topic`     | Source topic name (UTF-8 string)                         |
| `bifrost.source.partition` | Source partition index, **4 bytes big-endian unsigned**  |
| `bifrost.source.offset`    | Source offset, **8 bytes big-endian unsigned**           |

**Idempotency key:** the tuple `(cluster, topic, partition, offset)` uniquely identifies the source record. Use it as a deduplication key in your consumer: store seen keys (or a short hash of the concatenation) in a database, cache, or compacted topic, and skip processing when the key was already handled. That works across redeliveries, consumer restarts, and multiple bridge instances writing the same destination, as long as they all relay the same source coordinates.

---

## Metrics

When `metrics.enabled` is true (default), bifrost serves Prometheus metrics on `/metrics` at `metrics.listen_addr` (default `:9090`).

Core `bifrost_relay_*` bridge metrics are always exported when metrics are enabled. Optional families are controlled by `metrics.groups` (default enabled when omitted):

- `kafka` — broker hook metrics per cluster (connect, E2E bytes/errors/latency, throttling)
- `tls` — TLS handshake and peer certificate metrics per cluster
- `golang`, `process`, `tcp` — runtime/platform collectors

Application metric names are prefixed with `bifrost_`; runtime collector metrics keep standard names (`go_*`, `process_*`). You can also set `metrics.extra_labels` to attach constant labels to all exported series.

For full metric-by-metric tables (name, labels, explanation), see [Metrics Deep Dive](./docs/metrics.md).
