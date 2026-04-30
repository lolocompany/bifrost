# Guide for AI agents working on bifrost

This file says what bifrost is, how repo organized, what names to use.
Goal: code changes stay aligned with product + existing code.

## What bifrost is

`bifrost` is Kafka replication / relay service.
One long-running process reads source topic on one cluster, writes destination topic on another cluster.
Behavior comes from declarative YAML: `clusters`, `bridges`.
User not hand-write consumer/producer glue per path.

Key product facts:

- **Bridge (config):** each YAML `bridges[]` item is one directional path: `from` (cluster+topic) -> `to` (cluster+topic). Type is `config.Bridge`.
  - `batch_size`: batch per source partition (`1` means no batching).
  - `override_partition`: force destination partition.
  - `override_key`: replace produced key with fixed string.
  - `extra_headers`: add string headers after `bifrost.source.*`; keys must not start `bifrost.*`.
  - In docs/config, user-facing word is **bridge**.
- **Relay (behavior):** `pkg/bridge` runs relay loop (`bridge.Run`): consume -> produce -> commit, with retries + `bifrost.source.*` headers for downstream dedupe.
  - Batching stays partition-local.
  - At-least-once preserved: commit only after matching produce succeeds.
  - README + metrics often use **relay** (`bifrost_relay_*`).
- **Clusters:** named broker profiles under `clusters:`. Consumer settings apply on from side; producer settings on to side.
- **Process:** one OS process runs `pkg/bifrost.Run` with `errgroup`.
  - Each bridge config can spawn multiple `bridge.Run` goroutines.
  - `replicas` omitted or `0` => auto-size from source partition count (after topic ensure), with CPU/memory caps + global fair-sharing across bridges (see `pkg/bifrost` budgeting + `SystemSnapshot`).
  - Positive `replicas` => fixed relay goroutine count (own from-consumer, shared to-producer).
  - Process shares one producer per destination cluster unless benchmark proves another topology better.

Do not mix these terms:

| Term | Meaning |
| :-- | :-- |
| `config.Bridge` | one configured route (YAML/struct) |
| `pkg/bridge` | relay implementation package |
| Relay | act of moving records on bridge |
| Runtime (generic word) | too vague here; prefer process, relay, or run |

## Layout

- `cmd/bifrost`: CLI flags, YAML load, logging, signal context, call `bifrost.Run(ctx, cfg)`. `cmd/bifrost/version` holds release/build metadata + `-X` ldflags targets.
- `pkg/bifrost`: process orchestration (`Run`), Kafka client construction, topic ensure, `errgroup` over configured bridges.
- `pkg/bridge`: relay loop + interfaces (`MetricsReporter`, `RunOptions`, ...).
- `pkg/config`: parsed config, validation, defaults source-of-truth (via config methods).
- `pkg/kafka`: Franz-go helpers (producers, consumers, ping, ensure topics).
- `pkg/metrics`: Prometheus collectors, `NewFromConfig` (registry + optional `/metrics` HTTP server), `Handler`.
- `test/unit`, `test/integration`: tests (integration can be Docker-backed). Unit tests for `pkg/*` live in `test/unit/<package>/`, not beside `pkg` sources. Do not add new `*_test.go` under `pkg/` product packages.

## Naming and abstractions

### Preferred vocabulary

- `from` / `to`: match YAML and `BridgeTarget` fields (`From`, `To`).
- `destination cluster`: use when discussing producers keyed by `To.Cluster`.
- `relay`: use for streaming behavior + cross-cutting concerns (headers, `bifrost_relay_*` metrics), not as rename for `config.Bridge`.
- `process`: single `bifrost` OS process hosting all bridges.
- `run`: `pkg/bifrost.Run` is startup wiring entrypoint (metrics, Kafka clients, bridges). `cmd/bifrost` loads config + calls it. Avoid vague name like `bridgeRuntime`.

### Avoid import shadowing

If file imports `pkg/bridge`, do not create local variable named `bridge` in same file.
Use `bridgeCfg` for `config.Bridge` values.

### Functions in `pkg/bifrost`

Names should be action-oriented + scope-explicit:

- Build clients: `build...`, `new...` (ex: producers by destination cluster).
- Topic creation: `ensure...` with qualifier (configured bridges vs one cluster).
- Goroutine entry: `Run` / `runOneBridge` (process-level vs single bridge).

Extract helper only if logic repeats with different params, or tests target focused unit (example: `BridgeRunOptions` tests in `test/unit/bifrost`).

### Errors and metrics

- Wrap errors with `fmt.Errorf("...: %w", err)`.
- Use `errors.Is` for `context.Canceled` when relevant.
- In `pkg/*`, prefer explicit error returns; avoid panic-centric helpers in library code.
- Metrics: app metrics prefix `bifrost_`; relay metrics prefix `bifrost_relay_*`. See `docs/metrics.md`.

### Quality gates

- Keep local checks aligned with CI: `make lint`, `make test-unit`, `make test-race`.
- If lint policy changes, update `.golangci.yml`, CI workflow(s), and `.cursor/docs/codequality.md` together.

## Read before large changes

1. `README.md` for behavior, failure handling, headers, metrics overview.
2. `example.config.yaml` for full config shape.
3. `pkg/config/bridge.go` for `Bridge` validation + consumer group naming.
4. `pkg/bridge` for relay contract + retries.
5. `docs/metrics.md` when changing observability.

## Repo anti-patterns

- Adding vague `runtime` / `bridgeRuntime` names not tied to relay/process vocabulary.
- Naming local variable `bridge` when `pkg/bridge` import needed.
- Creating generic `util` / `helpers` package.
- Changing behavior in `pkg/` or relay wiring (`pkg/bifrost`) without matching tests under `test/unit/...` (or integration where appropriate).

---

_Maintainers: update this doc when public behavior or major package boundaries change._
