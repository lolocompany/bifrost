# Codequality Policy

This doc sets maintainability + complexity targets.
This doc also sets Go style + static-analysis baseline in CI.

## Why these checks

- ISO/IEC 25010 maintainability traits (modularity, analysability, modifiability, testability) give base model.
- SIG-style dimensions (duplication, unit size, unit complexity, architecture violations) turn model into checks.
- Sonar-style complexity/debt thinking informs thresholds and risk scoring.

## Canonical targets

| Dimension | Metric | Green | Yellow | Red |
| :-- | :-- | :-- | :-- | :-- |
| Testability | Unit coverage total (`go test -coverpkg`) | `>= 70%` | `50-69.99%` | `< 50%` |
| Complexity | Functions over cyclomatic red threshold (`> 15`) | `0` | `1-10` | `> 10` |
| Complexity | Functions over cognitive red threshold (`> 20`) | `0` | `1-10` | `> 10` |
| Analysability | Duplicate clone groups (`dupl -t 60`) | `0-1` | `2-5` | `> 5` |
| Modularity | Package import cycles | `0` | n/a | `> 0` |

Notes:

- Legacy complexity may already exceed steady-state target. CI gates regressions; new change must not worsen baseline debt.
- Hotspot priority uses `churn x complexity` to target high-risk/high-change refactor areas.

## Exception process

Any justified exception should include:

1. Why threshold not practical now for that code path.
2. Follow-up issue/task with owner + target milestone.
3. Bounded scope (file/function/package), never blanket waiver.
4. PR note explaining why current risk still acceptable.

## Measurement and reporting workflow

- Run local report: `make codequality`
- Artifacts:
  - `reports/codequality/coverage.txt`
  - `reports/codequality/cyclomatic-red.txt`
  - `reports/codequality/cognitive-red.txt`
  - `reports/codequality/duplication.txt`

## Go style and static-analysis baseline

Repo enforces lint baseline aligned with Uber Go style guidance:

- `golangci-lint` with repository config (`.golangci.yml`)
- `go vet`
- `staticcheck`
- `errcheck`
- `revive`

Local command:

- `make lint`

CI command path:

- `.github/workflows/go-quality.yml` runs `make lint`, `make test-unit`, `make test-race`.

## Alignment decision rules

- **Aligned**: core checks green, trend non-regressing.
- **Partially aligned**: historical debt exists, but CI blocks regression and trend improves.
- **Not aligned**: repeated red with no remediation path, or baseline keeps regressing.
