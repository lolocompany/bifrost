# Codequality Policy

This document defines maintainability and complexity targets for repository code quality.
It also defines the Go style and static-analysis baseline used in CI.

## Why these checks

- ISO/IEC 25010 maintainability characteristics (modularity, analysability, modifiability, testability) are the conceptual basis.
- SIG-style dimensions (duplication, unit size, unit complexity, architecture violations) provide actionable code-level checks.
- Sonar-style complexity/debt practices inform threshold bands and risk scoring.

## Canonical targets

| Dimension | Metric | Green | Yellow | Red |
| :-- | :-- | :-- | :-- | :-- |
| Testability | Unit coverage total (`go test -coverpkg`) | `>= 70%` | `50-69.99%` | `< 50%` |
| Complexity | Functions over cyclomatic red threshold (`> 15`) | `0` | `1-10` | `> 10` |
| Complexity | Functions over cognitive red threshold (`> 20`) | `0` | `1-10` | `> 10` |
| Analysability | Duplicate clone groups (`dupl -t 60`) | `0-1` | `2-5` | `> 5` |
| Modularity | Package import cycles | `0` | n/a | `> 0` |

Notes:

- Existing legacy complexity can exceed desired steady-state limits; CI enforcement is regression-based so new changes do not worsen baseline debt.
- Hotspot prioritization uses `churn x complexity` to focus refactors where risk and change frequency overlap.

## Exception process

Any justified exception should include:

1. Why the threshold is currently impractical for that code path.
2. A follow-up issue/task with owner and target milestone.
3. A bounded scope (file/function/package), not a blanket waiver.
4. A PR note explaining why current risk remains acceptable.

## Measurement and reporting workflow

- Run codequality report locally: `make codequality`
- Artifacts:
  - `reports/codequality/coverage.txt`
  - `reports/codequality/cyclomatic-red.txt`
  - `reports/codequality/cognitive-red.txt`
  - `reports/codequality/duplication.txt`

## Go style and static-analysis baseline

The repository enforces a lint baseline aligned with the Uber Go style guidance:

- `golangci-lint` with repository config (`.golangci.yml`)
- `go vet`
- `staticcheck`
- `errcheck`
- `revive`

Local command:

- `make lint`

CI command path:

- `.github/workflows/go-quality.yml` runs `make lint`, `make test-unit`, and `make test-race`.

## Alignment decision rules

- **Aligned**: core checks are green and trend does not regress.
- **Partially aligned**: historical debt remains, but CI gates prevent regression and trend improves.
- **Not aligned**: repeated red status without remediation trajectory, or baseline repeatedly regresses.
