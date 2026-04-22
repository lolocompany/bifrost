# Metrics Deep Dive

This document is the detailed reference for all metrics emitted by bifrost.

- Namespace: application metrics are prefixed with `bifrost_`; Go/process collector metrics keep standard `go_*` and `process_*` names.
- Constant labels: if `metrics.extra_labels` is configured, those labels are attached to all metrics.
- Relay metrics (`bifrost_relay_*`) are always registered when the metrics endpoint is enabled; they are not gated by `metrics.groups`.
- Group switches: `metrics.groups.<group>` controls optional families (`kafka`, `tls`, `tcp`, `golang`, `process`).

## Naming Schema

Application metrics follow this schema:

- `bifrost_<subsystem>_<thing>_<unit>[_total]`
- `snake_case` only.
- Base units in names (`_seconds`, `_bytes`, `_timestamp_seconds`).
- Counters use `_total`.
- Keep the `<thing>` segment concise and avoid repeating subsystem context in the name.
- Labels carry dimensions (do not encode dimensions into metric names).

## Core Label Keys

These are the stable, low-cardinality label keys used by bifrost application metrics.

| Label key | Groups | Meaning |
| :-- | :-- | :-- |
| `bridge` | `relay` | Logical bridge route name from config. |
| `from_kafka_cluster` | `relay` | Source cluster key from `bridges[].from.cluster`. |
| `from_topic` | `relay` | Source topic from `bridges[].from.topic`. |
| `to_kafka_cluster` | `relay` | Destination cluster key from `bridges[].to.cluster`. |
| `to_topic` | `relay` | Destination topic from `bridges[].to.topic`. |
| `stage` | `relay` | Relay error stage (`poll`, `produce`, `commit`, `route`). |
| `kafka_cluster` | `kafka`, `tls`, `tcp` | Cluster key associated with broker hook events. |
| `tls_version` | `tls` | Negotiated TLS protocol version (`1.2`, `1.3`, `unknown`). |

Additional labels may appear when `metrics.extra_labels` is configured; those are user-defined constant labels attached to all metric families.
`metrics.extra_labels` must not reuse built-in variable label keys (`bridge`, `from_kafka_cluster`,
`from_topic`, `to_kafka_cluster`, `to_topic`, `stage`, `state`, `kafka_cluster`, `tls_version`,
`le`, `quantile`), otherwise config validation fails.

## relay

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `bifrost_relay_messages_total` | `Counter` | `bridge`, `from_kafka_cluster`, `from_topic`, `to_kafka_cluster`, `to_topic` | Count of records relayed successfully (produce + commit completed). |
| `bifrost_relay_errors_total` | `Counter` | `bridge`, `from_kafka_cluster`, `from_topic`, `to_kafka_cluster`, `to_topic`, `stage` | Count of relay errors by stage (`poll`, `produce`, `commit`, `route`). |
| `bifrost_relay_produce_duration_seconds` | `Histogram` | `bridge`, `from_kafka_cluster`, `from_topic`, `to_kafka_cluster`, `to_topic` | Histogram of to-side produce time per synchronous relay produce call (one record when `batch_size=1`, otherwise one source-partition batch). |
| `bifrost_relay_consumer_seconds_total` | `Counter` | `bridge`, `from_kafka_cluster`, `from_topic`, `to_kafka_cluster`, `to_topic`, `state` | Wall-clock seconds attributed to consumer state. `state` is `busy` when poll returns records, otherwise `idle`. |
| `bifrost_relay_producer_seconds_total` | `Counter` | `bridge`, `from_kafka_cluster`, `from_topic`, `to_kafka_cluster`, `to_topic`, `state` | Wall-clock seconds attributed to producer state. `state` is `busy` during produce attempts, `idle` during no-work poll intervals and retry backoff sleeps. |

### Idle Percentage Queries

Use `rate(...)` over a window (for example, `5m`) and divide idle by total (`idle + busy`).

Consumer idle %:

```promql
100 *
sum by (bridge, from_kafka_cluster, from_topic, to_kafka_cluster, to_topic) (
  rate(bifrost_relay_consumer_seconds_total{state="idle"}[5m])
)
/
sum by (bridge, from_kafka_cluster, from_topic, to_kafka_cluster, to_topic) (
  rate(bifrost_relay_consumer_seconds_total[5m])
)
```

Producer idle %:

```promql
100 *
sum by (bridge, from_kafka_cluster, from_topic, to_kafka_cluster, to_topic) (
  rate(bifrost_relay_producer_seconds_total{state="idle"}[5m])
)
/
sum by (bridge, from_kafka_cluster, from_topic, to_kafka_cluster, to_topic) (
  rate(bifrost_relay_producer_seconds_total[5m])
)
```

## Labels Added By Prometheus/Alloy

Prometheus and Alloy scrapes typically attach target identity labels such as `job` and `instance`, and may also attach discovery/relabel-derived infrastructure labels (for example `cluster`, `namespace`, `pod`, `service`) depending on your pipeline configuration.

Prometheus/Alloy pipelines can also be configured to add or rewrite labels via relabeling and service-discovery metadata promotion. Since those labels may vary by environment, avoid using the following keys in `metrics.extra_labels` to prevent exact or semantic collisions:

- `job`
- `instance`
- `cluster`
- `namespace`
- `pod`
- `service`
- any label matching `^__.*__$`

At startup, bifrost logs a warning if any configured `metrics.extra_labels` keys match the reserved
scrape list above.

## kafka

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `bifrost_kafka_connect_attempts_total` | `Counter` | `cluster` | Number of broker connect attempts. |
| `bifrost_kafka_connect_errors_total` | `Counter` | `cluster` | Number of broker connect errors. |
| `bifrost_kafka_connect_duration_seconds` | `Histogram` | `cluster` | Histogram of successful broker connect/init durations. |
| `bifrost_kafka_requests_total` | `Counter` | `cluster` | Number of completed request/response pairs seen by E2E hooks. |
| `bifrost_kafka_request_errors_total` | `Counter` | `cluster` | Number of E2E broker requests that ended with error. |
| `bifrost_kafka_write_bytes_total` | `Counter` | `cluster` | Total bytes written to brokers (E2E hook). |
| `bifrost_kafka_read_bytes_total` | `Counter` | `cluster` | Total bytes read from brokers (E2E hook). |
| `bifrost_kafka_request_duration_seconds` | `Histogram` | `cluster` | Histogram of end-to-end request duration (write through response read). |
| `bifrost_kafka_throttle_seconds_total` | `Counter` | `cluster` | Cumulative broker throttle duration in seconds. |
| `bifrost_kafka_throttle_events_total` | `Counter` | `cluster` | Number of throttled broker responses. |

## tls

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `bifrost_tls_handshakes_total` | `Counter` | `cluster`, `tls_version` | Number of completed TLS handshakes by negotiated TLS version. |
| `bifrost_tls_handshake_errors_total` | `Counter` | `cluster` | Number of broker connections without completed TLS handshake. |
| `bifrost_tls_peer_leaf_not_after_timestamp_seconds` | `Gauge` | `cluster` | Last observed peer leaf certificate expiration as Unix timestamp seconds. |

## tcp

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `bifrost_tcp_connect_attempts_total` | `Counter` | `cluster` | Number of TCP dial attempts to brokers. |
| `bifrost_tcp_connect_errors_total` | `Counter` | `cluster` | Number of TCP dial errors to brokers. |
| `bifrost_tcp_connect_duration_seconds` | `Histogram` | `cluster` | Histogram of TCP dial duration only (until the TCP socket is established). TLS handshake and SASL happen after this and are not included. |
| `bifrost_tcp_disconnects_total` | `Counter` | `cluster` | Number of broker TCP disconnect events. |
| `bifrost_tcp_active_connections` | `Gauge` | `cluster` | Current number of active broker TCP connections. |

## golang

These metrics come from the Prometheus Go collector and build info collector.

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `go_*` | `Mixed` | none | Standard Go runtime metrics (`go_goroutines`, `go_gc_*`, `go_memstats_*`, etc.). |
| `go_build_info` | `Gauge` | exporter/build labels | Build metadata metric from the Go build info collector. |

## process

These metrics come from the Prometheus process collector.

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `process_*` | `Mixed` | none | Standard process metrics (CPU time, memory, open FDs, start time, virtual/resident memory, etc.). |

## Codequality policy

Repository codequality policy (maintainability/complexity targets, thresholds, and governance workflow) lives in `docs/codequality.md`.
