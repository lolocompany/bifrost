# Guide for AI agents working on bifrost

This document summarizes what **bifrost** is, how the repository is organized, and naming conventions so changes stay aligned with the product and existing code.

## What bifrost is

**bifrost** is a **Kafka replication / relay service**: a single long-running process reads records from a **source** topic on one **cluster** and writes them to a **destination** topic on another cluster, driven entirely by **declarative YAML** (`clusters`, `bridges`). Users avoid hand-writing consumer/producer glue for each path.

Important product facts (from the README and code):

- **Bridge (config):** One YAML `bridges[]` entry is a **directional** path: `from` (cluster + topic) → `to` (cluster + topic). The type is `config.Bridge`. Optional `extra_headers` adds string Kafka headers on the to-side record (after `bifrost.source.*`); keys must not use the `bifrost.*` prefix. This is the user-facing word in docs and config.
- **Relay (behavior):** The **pkg/bridge** package implements the **relay loop** (`bridge.Run`): consume → produce → commit, with retries and `bifrost.source.*` headers for downstream deduplication. README and metrics often say **relay** (`bifrost_relay_*`).
- **Clusters:** Named broker profiles under `clusters:`; consumer settings apply on the **from** side, producer settings on the **to** side.
- **Process:** One OS process can run **many bridges in parallel** (one goroutine per configured bridge, coordinated with `errgroup`).

Do **not** conflate:

| Term                          | Meaning                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| `config.Bridge`               | One configured route (YAML / struct).                                                              |
| `pkg/bridge`                  | The relay implementation package (import path often aliased if `bridge` would shadow).             |
| **Relay**                     | The act of moving records along a bridge; used in docs and metric names.                           |
| **Runtime** (generic English) | Easy to overuse in names; prefer **process**, **relay**, or **run** when describing this codebase. |

## Layout

| Path                            | Role                                                                                                                                                                                          |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cmd/bifrost`                   | CLI: flags, load YAML, logging setup, signal context, **`bifrost.Run(ctx, cfg)`**. **`cmd/bifrost/version`** holds release/build metadata and `-X` ldflags targets.                                                                                           |
| `pkg/bifrost`                   | Process orchestration: **`Run`**, Kafka client construction, topic ensure, `errgroup` over configured bridges.                                                                                |
| `pkg/bridge`                    | Core relay loop and interfaces (`MetricsReporter`, `RunOptions`, etc.).                                                                                                                       |
| `pkg/config`                    | Parsed config, validation, defaults.                                                                                                                                                          |
| `pkg/kafka`                     | Franz-go helpers (producers, consumers, ping, ensure topics).                                                                                                                                 |
| `pkg/metrics`                   | Prometheus collectors, **`NewFromConfig`** (registry + `/metrics` HTTP server when enabled), `Handler`.                                                                                        |
| `test/unit`, `test/integration` | Tests; integration tests are Docker-backed when enabled. **Unit tests for `pkg/*` live under `test/unit/<package>/`** (e.g. `test/unit/bifrost` for `pkg/bifrost`), not beside `pkg` sources. |

## Naming and abstractions

### Preferred vocabulary

- **from / to** — Match YAML and `BridgeTarget` (`From`, `To`). Say “destination cluster” when discussing producers keyed by `To.Cluster`.
- **relay** — Use for the streaming behavior and cross-cutting concerns (headers, metrics prefix `bifrost_relay_*`), not for the config struct name (`Bridge` stays `Bridge`).
- **process** — Use when referring to the single `bifrost` OS process hosting all bridges.
- **run** — **`pkg/bifrost.Run`** is the entrypoint for **startup wiring** (metrics, Kafka clients, bridges). **`cmd/bifrost`** only loads config and calls it. Avoid names like `bridgeRuntime` (ambiguous next to `config.Bridge` and the `bridge` package import).

### Avoid import shadowing

The relay package is commonly imported as:

```go
import (
    "github.com/lolocompany/bifrost/pkg/bridge"
    ...
)
```

Do **not** use `bridge` as a local variable name in the same file if you need to call `bridge.Run` or `bridge.IdentityFrom`. Prefer `bridgeCfg` for values of type `config.Bridge`.

### Functions in `pkg/bifrost`

Keep names **action-oriented** and **scope-explicit**:

- Building Franz clients → **build…**, **new…** (e.g. producers **by destination cluster**).
- Topic creation → **ensure…** with a qualifier (configured bridges vs one cluster).
- Goroutine entry → **`Run`** / **`runOneBridge`** (process vs single bridge).

Extract helpers only when the same logic runs multiple times with different parameters, or when tests target a focused unit (e.g. **`BridgeRunOptions`** in `test/unit/bifrost`).

### Errors and metrics

- Wrap errors with `fmt.Errorf("...: %w", err)`; use `errors.Is` for `context.Canceled` where appropriate.
- Metric naming follows README: application metrics prefixed `bifrost_`; relay metrics `bifrost_relay_*`. See `docs/metrics.md` for detail.

## What to read before large changes

1. `README.md` — product behavior, failure handling, headers, metrics overview.
2. `example.config.yaml` — full config shape.
3. `pkg/config/bridge.go` — `Bridge` validation and consumer group naming.
4. `pkg/bridge` — relay loop contract and retries.
5. `docs/metrics.md` — if touching observability.

## Anti-patterns for this repo

- Introducing a generic **`runtime`** or **`bridgeRuntime`** type name without tying it to **relay** or **process** vocabulary.
- Abbreviating **`bridge` the package** away with locals named `bridge` in the same file.
- New **`util` / `helpers`** packages (repo convention: avoid catch-all helper packages).
- Skipping tests for behavior changes in `pkg/` or relay wiring (`pkg/bifrost`); add or update tests under **`test/unit/...`** as appropriate.

---

_Maintainers: update this file when public behavior or major package boundaries change._
