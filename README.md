# bifrost

![Banner logo that says "Bifrost: Kafka Cluster Bridge" with a rainbow bridging the gap between two clusters](./assets/banner_logo.jpg)

**bifrost** is a configurable Kafka replication service for moving records between topics and clusters using declarative YAML configurations. It is designed for teams that need a lightweight, operationally simple way to build reliable cross-cluster relay pipelines without writing custom consumer/producer code for every path.

**Observability**:

- **Logs:** Structured log lines (JSON or logfmt), written to stdout or stderr with configurable extra fields.
- **Metrics:** Prometheus scrape path `/metrics`. By default `metrics.listen_addr` is `:9090` (all interfaces) when metrics are enabled; override it in YAML. Scrape, for example, `http://127.0.0.1:9090/metrics`. Series include `bifrost_` application metrics plus standard `go_*` / `process_*` collector series.

**Runtime requirement:** at least one **Kafka-compatible** broker (Apache Kafka, Redpanda, etc.) reachable from the host or container running bifrost.

---

## Using the CLI

### Install or download

**Download prebuilt binaries:** [GitHub Releases](https://github.com/lolocompany/bifrost/releases) — the release workflow publishes cross-compiled `**bifrost`**binaries for Linux, macOS, and Windows (amd64 and arm64)** plus checksums.

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

If you omit `**--config**` / `**-c**` and do not set `**BIFROST_CONFIG**`, the default path is `**bifrost.yaml**` in the **current working directory**. If that file is missing or invalid, bifrost exits with an error.

### Docker

Release images are built by [GoReleaser](https://goreleaser.com/) (`dockers_v2`) from [`goreleaser/Dockerfile`](./goreleaser/Dockerfile). To **build locally from source** without GoReleaser, use [Dockerfile.build](./Dockerfile.build) (for example `make build-docker`).

The image runs `bifrost` with working directory `/home/bifrost`. **When using this image, prefer setting `BIFROST_CONFIG`** to the path of your YAML inside the container (for example after bind-mounting or copying the file). That keeps the config location explicit and avoids relying on default filenames or extra container command arguments.

```bash
docker run --rm \
  -e BIFROST_CONFIG=/home/bifrost/bifrost.yaml \
  -v bifrost.yaml:/home/bifrost/bifrost.yaml:ro \
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
    # Optional bridge-local relay batching. Defaults to 1 (disabled).
    # batch_size: 1
    # Optional concurrent produce depth per relay replica. Defaults to 64.
    # max_in_flight_batches: 64
    # Optional interval-based commit coalescing. Defaults to 1s.
    # commit_interval: "1s"
    # Optional size-based commit coalescing. Defaults to 1024.
    # commit_max_records: 1024
    # Optional fixed destination partition override. When omitted, bifrost preserves
    # the source partition number on each record. If set, the destination topic
    # must contain that partition index.
    # override_partition: 0
    # Optional fixed key override applied to every produced record for this bridge.
    # override_key: "static-key"
```

### Throughput-friendly defaults

Bifrost defaults are intentionally elastic: low overhead at idle, but ready to scale under load.

- Bridge defaults:
  - `max_in_flight_batches: 64`
  - `commit_interval: 1s`
  - `commit_max_records: 1024`
- Producer defaults:
  - `producer.required_acks: leader`
  - `producer.disable_idempotent_write: true`
- For stronger durability guarantees, prefer `producer.required_acks: all` and keep idempotence enabled (`disable_idempotent_write: false`).
- For higher throughput tuning, prefer:
  - `producer.linger: "5ms"`
  - `producer.batch_compression: "snappy"` (or `zstd` for better compression ratio)
  - `producer.batch_max_bytes` sized for your largest expected records
  - `consumer.fetch_max_bytes` and `consumer.fetch_max_partition_bytes` sized for your record profile.

### Relay failure handling

Bridge stages do not all fail the process the same way:

- `poll fetches` errors are counted, logged, and retried immediately with no backoff.
- Destination `produce` failures retry the same record or source-partition batch in-place with exponential backoff plus additive jitter.
- Source offset `commit` failures retry the same commit in-place with exponential backoff plus additive jitter.
- Unexpected source-topic mismatches are treated as fatal and stop the bridge.

Produce and commit retries are configured per cluster:

```yaml
clusters:
  source:
    consumer:
      commit_retry:
        min_backoff: '1s'
        max_backoff: '30s'
        jitter: '250ms'
  destination:
    producer:
      retry:
        min_backoff: '1s'
        max_backoff: '30s'
        jitter: '250ms'
```

If you omit these blocks, bifrost uses the same defaults shown above. Commit retries happen after a successful produce and retry the commit itself rather than re-producing the record, which reduces duplicate writes when Kafka acknowledges the produce but the offset commit fails.

### Development

| Command | Purpose |
| --- | --- |
| `make build` | Build `./bifrost` from source |
| `make test` | Run unit tests (`./test/unit/...`) |
| `make test-integration` | Run Docker-backed integration tests (`BIFROST_INTEGRATION=1`) |
| `make bench` | Default subset; **one Redpanda container per benchmark** (isolated; slower than shared broker) |
| `make lint` | Run `go vet`, `go mod verify`, `govulncheck`, `gosec`, and `golangci-lint` |
| `make lint-ci` | Run CI lint gate (`lint` + `staticcheck` + `errcheck` + `revive`) |
| `make codequality-scorecard` | Generate codequality scorecard (`reports/codequality/scorecard.{json,md}`) |
| `make codequality-baseline` | Capture/refresh codequality baseline for regression gates |
| `make codequality-gate` | Enforce codequality regression + severe outlier gates |
| `make test-unit` | Run unit tests (`./test/unit/...`) |
| `make test-race` | Run unit tests with race detector (`./test/unit/...`) |
| `make format` | Run `go fmt` and `gofmt` |

Contributor and agent-oriented notes on layout and naming: `[docs/AGENTS.md](./docs/AGENTS.md)`.
Configuration profiles and tradeoffs: `[docs/config-profiles.md](./docs/config-profiles.md)`.
Codequality policy and governance: `[docs/codequality.md](./docs/codequality.md)` and `[docs/maintainability-governance.md](./docs/maintainability-governance.md)`.

---

## Downstream deduplication (source headers)

Every relayed record includes **source coordinate** headers so consumers can treat deliveries as **at-least-once** and still process each logical message once. The bridge always sets these; optional per-bridge `**extra_headers`** in YAML are added next, then any headers copied from the source record. Extra header keys must not use the `**bifrost.\*\*\*` prefix (reserved for the relay). By default, bifrost writes each destination record to the same partition number as the source record, so the destination topic must have at least as many partitions as the source topic. `override_partition` can force all records on a bridge to one destination partition instead, and `override_key` can replace every produced record key with a fixed string. `batch_size` groups records by source topic-partition, but offsets are still committed only after the matching produce succeeds.

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

The metrics endpoint is unauthenticated. Bind it to a trusted interface or restrict network access before exposing bifrost outside local or private infrastructure.

Core `bifrost_relay_`\* bridge metrics are always exported when metrics are enabled. Optional families are controlled by `metrics.groups` (default enabled when omitted):

- `kafka` — broker hook metrics per cluster (connect, E2E bytes/errors/latency, throttling)
- `tls` — TLS handshake and peer certificate metrics per cluster
- `golang`, `process`, `tcp` — runtime/platform collectors

Application metric names are prefixed with `bifrost_`; runtime collector metrics keep standard names (`go_*`, `process_*`). You can also set `metrics.extra_labels` to attach constant labels to all exported series.

For full metric-by-metric tables (name, labels, explanation), see [Metrics Deep Dive](./docs/metrics.md).
