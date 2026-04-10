# Metrics Deep Dive

This document is the detailed reference for all metrics emitted by bifrost.

- Namespace: application metrics are prefixed with `bifrost_`; Go/process collector metrics keep standard `go_*` and `process_*` names.
- Constant labels: if `metrics.extra_labels` is configured, those labels are attached to all metrics.
- Group switches: `metrics.groups.<group>` controls each family.

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
| `from_cluster` | `relay` | Source cluster key from `bridges[].from.cluster`. |
| `from_topic` | `relay` | Source topic from `bridges[].from.topic`. |
| `to_cluster` | `relay` | Destination cluster key from `bridges[].to.cluster`. |
| `to_topic` | `relay` | Destination topic from `bridges[].to.topic`. |
| `stage` | `relay` | Relay error stage (`poll`, `produce`, `commit`, `route`). |
| `cluster` | `kafka`, `tls`, `tcp` | Cluster key associated with broker hook events. |
| `tls_version` | `tls` | Negotiated TLS protocol version (`1.2`, `1.3`, `unknown`). |

Additional labels may appear when `metrics.extra_labels` is configured; those are user-defined constant labels attached to all metric families.

## relay

| Metric | Type | Labels | Description |
| :-- | :-- | :-- | :-- |
| `bifrost_relay_messages_total` | `Counter` | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic` | Count of records relayed successfully (produce + commit completed). |
| `bifrost_relay_errors_total` | `Counter` | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic`, `stage` | Count of relay errors by stage (`poll`, `produce`, `commit`, `route`). |
| `bifrost_relay_produce_duration_seconds` | `Histogram` | `bridge`, `from_cluster`, `from_topic`, `to_cluster`, `to_topic` | Histogram of to-side produce time per relayed record. |

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
| `bifrost_tcp_connect_duration_seconds` | `Histogram` | `cluster` | Histogram of successful TCP connect durations. |
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
