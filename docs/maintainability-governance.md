# Maintainability Governance

This playbook defines how to operate maintainability metrics after the pipeline is in place.

## Cadence

- **Per PR**
  - Run `make codequality-gate`.
  - Do not merge if regression gates fail.
  - For justified exceptions, add a bounded follow-up issue.
- **Per merge to `main`**
  - Run `make codequality-scorecard`.
  - Keep generated snapshot under `reports/codequality/snapshots/`.
- **Weekly**
  - Review top hotspots from latest scorecard (`churn x complexity`).
  - Select at least one high-risk hotspot for incremental refactor/test hardening.
- **Monthly**
  - Compare current scorecard to baseline and prior month.
  - Decide whether thresholds can be tightened.

## Triage Model

Prioritize refactors in this order:

1. Files with highest hotspot risk and low/medium test coverage.
2. Functions over cognitive/cyclomatic red threshold.
3. Any newly introduced duplicate clone groups.
4. Packages with high internal fan-out that correlate with recent defects/incidents.

## Backlog Template

Use this template for each maintainability item:

- **Scope**: file/function/package
- **Metric trigger**: which threshold is violated
- **Observed value**: current metric value
- **Target value**: intended value after remediation
- **Risk**: impact if not addressed
- **Owner**: accountable engineer
- **Due milestone**: sprint/release target
- **Validation**: command/output proving improvement

## Threshold Ratcheting Policy

- Ratchet only after two consecutive periods with no regressions.
- Tighten one threshold category at a time (for example complexity first, coverage next).
- Never ratchet without a passing CI gate and a rollback plan.

## Definition of Done for maintainability work

- Related metric moves in expected direction.
- `make codequality-gate` passes.
- Tests covering modified logic are present and deterministic.
- Any exception records are closed or explicitly re-approved.
