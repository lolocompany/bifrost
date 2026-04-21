# Config Profiles

This document gives three practical Kafka relay profiles for bifrost and compares their tradeoffs.

The **throughput-oriented** profile matches bifrost defaults.

## Profiles At A Glance

| Profile | Primary goal | Key settings |
| :-- | :-- | :-- |
| Throughput-oriented (default) | Max message rate with acceptable duplicate risk | `producer.required_acks=leader`, `max_in_flight_batches=64`, `commit_interval=1s`, `commit_max_records=1024` |
| Durability-oriented | Minimize data-loss risk on broker failure | `producer.required_acks=all`, lower in-flight, faster commit flush |
| Resource-constrained | Keep CPU/memory/network pressure low | lower in-flight, lower batching pressure, slower commit cadence |

## Throughput-Oriented (Default)

```yaml
bridges:
  - name: prod-to-dev
    batch_size: 1
    max_in_flight_batches: 64
    commit_interval: "1s"
    commit_max_records: 1024

clusters:
  prod:
    producer:
      required_acks: leader
```

Pros:

- Highest throughput potential under sustained load.
- Best utilization of destination producer parallelism.
- Fewer commit RPCs per message than low-interval profiles.

Cons:

- Lower durability than `required_acks: all` during replica failure windows.
- Higher in-flight memory footprint during bursts.
- More potential duplicates after crashes than tighter commit cadence.

Best for:

- High-volume replication where latency and throughput are prioritized.

## Durability-Oriented

```yaml
bridges:
  - name: prod-to-dev
    batch_size: 1
    max_in_flight_batches: 8
    commit_interval: "200ms"
    commit_max_records: 256

clusters:
  prod:
    producer:
      required_acks: all
```

Pros:

- Stronger durability at the destination.
- Faster offset commit visibility, reducing replay window after restart.
- Lower duplicate volume in failure/restart scenarios.

Cons:

- Lower throughput than throughput profile (often significantly lower).
- Higher broker/commit overhead due to more frequent commits.
- Higher end-to-end latency under load.

Best for:

- Pipelines where correctness/durability is more important than raw throughput.

## Resource-Constrained

```yaml
bridges:
  - name: prod-to-dev
    batch_size: 1
    max_in_flight_batches: 4
    commit_interval: "2s"
    commit_max_records: 512

clusters:
  prod:
    producer:
      required_acks: leader
      linger: "0s"
```

Pros:

- Lower memory and CPU pressure.
- Lower background commit churn.
- More stable behavior on small nodes.

Cons:

- Lower peak throughput than throughput profile.
- Larger replay window than durability profile due to slower commits.
- More sensitive to transient destination slowdowns.

Best for:

- Dev clusters, small workloads, and constrained environments.

## Direct Tradeoff Comparison

| Dimension | Throughput-oriented | Durability-oriented | Resource-constrained |
| :-- | :-- | :-- | :-- |
| Throughput ceiling | Highest | Lowest | Medium-low |
| Destination durability | Medium (`leader`) | Highest (`all`) | Medium (`leader`) |
| Duplicate replay window | Medium | Lowest | Highest |
| CPU/memory use | Highest | Medium | Lowest |
| Commit RPC pressure | Medium | Highest | Lowest |

## Notes

- All profiles still keep bifrost at-least-once semantics (produce success before commit progression).
- If you need both high throughput and strong durability, start from durability profile and increase `max_in_flight_batches` gradually while monitoring broker saturation and relay lag.
