# Test Architecture

This repository uses a config-first testing style inspired by lolo-engine while staying fully in Go.

## Suite layout

- `test/unit`:
  - Pure logic and package-level unit tests.
  - Prefer deterministic table-driven tests.
- `test/integration`:
  - Process-driven end-to-end tests.
  - Each test composes a config artifact, provisions its own Kafka cluster, starts a child `bifrost` CLI process, and asserts Kafka output plus `/metrics`.
- `test/regression`:
  - Config-driven deterministic and randomized regression matrices.
  - Stores per-test artifacts for replay and post-failure diagnosis.
- `test/.artifacts`:
  - Runtime test artifacts (`config.yaml`, `stdout.log`, `stderr.log`, `metrics.txt`).
  - Preserved on failures; regression tests may preserve even on pass.

## Running tests

- Unit:
  - `make test`
- Process integration:
  - `BIFROST_INTEGRATION=1 make test-integration`
- Regression:
  - `BIFROST_INTEGRATION=1 make test-regression`
- Full integration:
  - `BIFROST_INTEGRATION=1 make test-integration`

## Replay a failed process test

1. Open the matching directory under `test/.artifacts/...`.
2. Inspect `config.yaml`, `stdout.log`, `stderr.log`, and `metrics.txt`.
3. Re-run manually from repo root:
   - `go run ./cmd/bifrost --config <path-to-config.yaml>`
