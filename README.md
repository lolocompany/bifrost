# bifrost

![Banner logo that says "Bifrost: Kafka Cluster Bridge" with a rainbow bridging the gap between two clusters](./assets/banner_logo.jpg)

**bifrost** is a small **Kafka cluster bridge**: you declare **named clusters** (broker URLs, TLS, SASL) under `clusters` and one or more **bridges** in **YAML**. Each bridge **consumes** from a **from** topic on one cluster and **produces** to a **to** topic on another, committing the from-side offset only after the to-side write succeeds.

**Why it matters:** teams that need **reliable topic-to-topic replication** across Kafka clusters get a **single binary** and **one config file**—no custom consumers per route, and **at-least-once** behavior with explicit failure stages you can alert on.

**Observability** (when enabled in config):

- **Logs:** structured **slog** output (**JSON** or **logfmt**) to **stdout**, **stderr**, or a **file** (`logging` in config).
- **Metrics:** when `metrics.enabled` is true (default), a Prometheus scrape endpoint (default **`http://0.0.0.0:9090/metrics`**; override with `metrics.listen_addr`). Series are grouped and prefixed with `bifrost_` (see [Metrics](#metrics) below).

**Runtime requirement:** at least one **Kafka-compatible** broker (Apache Kafka, Redpanda, etc.) reachable from the host or container running bifrost.

---

## Using the CLI

### Install or download

**Download prebuilt binaries:** [GitHub Releases](https://github.com/lolocompany/bifrost/releases) — the release workflow publishes cross-compiled **`bifrost`** artifacts for Linux, macOS, and Windows (**amd64** and **arm64**) plus checksums.

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

If you omit **`--config`** / **`-c`**, the default path is **`bifrost.yaml`** in the **current working directory**. If that file is missing or invalid, bifrost exits with an error.

A full multi-cluster example lives in [`example.config.yaml`](./example.config.yaml). Minimal shape:

```yaml
clusters:
  dev:
    brokers:
      - '127.0.0.1:9092'
    tls:
      enabled: false
    sasl:
      mechanism: none
  prod:
    brokers:
      - '127.0.0.1:9094'
    tls:
      enabled: false
    sasl:
      mechanism: none

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

| Command                 | Purpose                                                  |
| :---------------------- | :------------------------------------------------------- |
| `make test`             | Unit tests (`./test/unit/...`)                           |
| `make test-integration` | Integration tests (Docker; sets `BIFROST_INTEGRATION=1`) |
| `make test-all`         | All tests under `./test/...`                             |
| `make bench`            | Benchmarks (`./test/benchmark/...`; Docker + `BIFROST_BENCHMARK=1` for bridge throughput) |
| `make lint`             | `go vet` + `golangci-lint`                               |

`make bench` runs **config parse** micro-benchmarks and **Docker-backed** `BenchmarkBridgeRelay*` tests (Redpanda via Testcontainers) that measure end-to-end relay throughput through `pkg/bridge`. Without `BIFROST_BENCHMARK=1`, only the in-process benchmarks run.

---

## Metrics

When `metrics.enabled` is true (default), bifrost serves Prometheus text format on `metrics.listen_addr` (for example `:9090` at path `/metrics`). You can turn off entire families under `metrics.groups` in the config; omitted or null group keys default to **enabled**.

All series use the **`bifrost_` namespace** followed by the group prefix (for example `bifrost_forward_…`, `bifrost_kafka_…`). Histograms expose the usual Prometheus suffixes: `_bucket`, `_sum`, and `_count`.

### `forward`

Relay success: messages successfully produced on the **to** cluster after a successful offset commit on the **from** side. Each series is scoped to one configured bridge (YAML `bridges[].name` and its `from` / `to` cluster keys and topics).

| Metric                           | Type    | Labels                                                           | What it shows                                                   |
| :------------------------------- | :------ | :--------------------------------------------------------------- | :-------------------------------------------------------------- |
| `bifrost_forward_messages_total` | Counter | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic` | Total relayed messages produced on the to side for that bridge. |

### `errors`

Bridge relay failures (distinct from Kafka client wire errors in the `kafka` group).

| Metric                        | Type    | Labels                                                                    | What it shows                                                                 |
| :---------------------------- | :------ | :------------------------------------------------------------------------ | :---------------------------------------------------------------------------- |
| `bifrost_errors_bridge_total` | Counter | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic`, `stage` | Total failures by bridge. `stage` is `poll`, `produce`, `commit`, or `route`. |

### `latency`

End-to-end produce time for one relayed record on the to-side client.

| Metric                                           | Type      | Labels                                                           | What it shows                                                                        |
| :----------------------------------------------- | :-------- | :--------------------------------------------------------------- | :----------------------------------------------------------------------------------- |
| `bifrost_latency_relay_produce_duration_seconds` | Histogram | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic` | Wall-clock seconds to produce one relayed record on the to side (buckets/sum/count). |

### `golang`

Standard [client_golang](https://github.com/prometheus/client_golang) collectors: Go runtime and build metadata. Series are exposed as `bifrost_go_*` (for example `bifrost_go_memstats_*`, `bifrost_go_gc_*`) and `bifrost_go_build_info`. These collectors do not use bifrost-specific labels.

### `process`

Standard process collector: CPU, memory, file descriptors, start time, etc. Series are exposed as `bifrost_process_*` (for example `bifrost_process_cpu_seconds_total`, `bifrost_process_resident_memory_bytes`). No bifrost-specific labels.

### `kafka`

Observability from [franz-go](https://github.com/twmb/franz-go) broker hooks (client-side Kafka protocol). Hooks are registered **per cluster** (shared when several bridges use the same cluster entry), not per bridge.

| Metric                                              | Type      | Labels    | What it shows                                                           |
| :-------------------------------------------------- | :-------- | :-------- | :---------------------------------------------------------------------- |
| `bifrost_kafka_broker_connect_attempts_total`       | Counter   | `cluster` | Broker dial attempts (success or failure).                              |
| `bifrost_kafka_broker_connect_failures_total`       | Counter   | `cluster` | Dial or client init failures (including TLS/SASL).                      |
| `bifrost_kafka_broker_connect_duration_seconds`     | Histogram | `cluster` | Seconds to dial and finish connection setup (API versions, SASL, etc.). |
| `bifrost_kafka_broker_e2e_requests_total`           | Counter   | `cluster` | Completed request/response pairs (E2E hook).                            |
| `bifrost_kafka_broker_e2e_errors_total`             | Counter   | `cluster` | E2E write or read errors.                                               |
| `bifrost_kafka_broker_e2e_write_bytes_total`        | Counter   | `cluster` | Bytes written for full broker requests.                                 |
| `bifrost_kafka_broker_e2e_read_bytes_total`         | Counter   | `cluster` | Bytes read for full broker responses.                                   |
| `bifrost_kafka_broker_e2e_request_duration_seconds` | Histogram | `cluster` | E2E duration in seconds (write through response read).                  |
| `bifrost_kafka_broker_throttle_seconds_total`       | Counter   | `cluster` | Cumulative broker throttle time applied (seconds).                      |
| `bifrost_kafka_broker_throttle_events_total`        | Counter   | `cluster` | Responses that indicated throttling.                                    |

### `tls`

Derived from TLS connections to brokers when TLS is in use. Like `kafka`, these are keyed by **cluster** (not per bridge).

| Metric                                                     | Type    | Labels                                              | What it shows                                                          |
| :--------------------------------------------------------- | :------ | :-------------------------------------------------- | :--------------------------------------------------------------------- |
| `bifrost_tls_broker_handshakes_total`                      | Counter | `cluster`, `tls_version` (for example `1.2`, `1.3`) | Completed TLS handshakes.                                              |
| `bifrost_tls_broker_handshake_errors_total`                | Counter | `cluster`                                           | Connections that were not a completed TLS handshake.                   |
| `bifrost_tls_broker_peer_leaf_not_after_timestamp_seconds` | Gauge   | `cluster`                                           | Leaf certificate **NotAfter** as Unix seconds (last seen per cluster). |

### `tcp`

**Linux only.** Gauges from `/proc/<pid>/net/snmp` (Tcp) and `/proc/<pid>/net/netstat` (TcpExt) for this process’s **network namespace** (container-wide if not isolated). Not per-application socket; values are kernel counters as exposed by the kernel. **No labels** (process-wide gauges).

**Snmp (Tcp)**

| Metric                            | Labels | What it shows                          |
| :-------------------------------- | :----- | :------------------------------------- |
| `bifrost_tcp_retrans_segments`    | —      | TCP segments retransmitted.            |
| `bifrost_tcp_active_opens`        | —      | Connections opened actively.           |
| `bifrost_tcp_passive_opens`       | —      | Connections opened passively (listen). |
| `bifrost_tcp_attempt_fails`       | —      | Failed connection attempts.            |
| `bifrost_tcp_estab_resets`        | —      | Resets on established connections.     |
| `bifrost_tcp_current_established` | —      | Currently established connections.     |
| `bifrost_tcp_in_segments`         | —      | Segments received.                     |
| `bifrost_tcp_out_segments`        | —      | Segments sent.                         |
| `bifrost_tcp_in_errors`           | —      | Receive errors.                        |
| `bifrost_tcp_out_resets`          | —      | Segments sent with RST.                |

**Netstat (TcpExt)**

| Metric                                   | Labels | What it shows                   |
| :--------------------------------------- | :----- | :------------------------------ |
| `bifrost_tcp_netstat_lost_retransmit`    | —      | Packets lost after retransmit.  |
| `bifrost_tcp_netstat_fast_retrans`       | —      | Fast retransmit recoveries.     |
| `bifrost_tcp_netstat_slow_start_retrans` | —      | Slow-start retransmits.         |
| `bifrost_tcp_netstat_retrans_fail`       | —      | Retransmit failures.            |
| `bifrost_tcp_netstat_syn_retrans`        | —      | SYN retransmits.                |
| `bifrost_tcp_netstat_timeouts`           | —      | TCP timeouts.                   |
| `bifrost_tcp_netstat_spurious_rtos`      | —      | Spurious RTOs.                  |
| `bifrost_tcp_netstat_backlog_drop`       | —      | Accept backlog drops.           |
| `bifrost_tcp_netstat_abort_on_timeout`   | —      | Connections aborted on timeout. |

On non-Linux platforms the `tcp` group registers no extra series.
