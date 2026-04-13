---
name: go-quality-reviewer-subagent
description: >
  Delegates read-only Go quality and pattern reviews to the project subagent defined in
  .cursor/agents/go-quality-reviewer.md. Use for verification during or after implementation:
  audit diffs and touched packages against repo conventions and idiomatic Go. Also use when
  the user asks for a standalone standards or pattern review. Triggers: "go quality review",
  "use go-quality-reviewer", "subagent review", "verify quality", "standards review",
  "pattern review", idiomatic Go audit, or after substantive pkg/cmd changes.
---

# Go quality reviewer subagent

## What this is

This repository defines a **Cursor subagent** at [`.cursor/agents/go-quality-reviewer.md`](../../agents/go-quality-reviewer.md). That file is the subagent’s **system prompt**: read-only Go audits against **this repo’s conventions** (errors, interfaces, concurrency, `cmd`/`pkg` layout) plus **common Go patterns** (boundaries, options, testing, concurrency hygiene).

The subagent **never edits** code—it **only** analyzes and returns a structured report.

## When to apply this skill

Use this skill when **any** of these apply:

- **During or after Go implementation** (features, refactors, fixes): run the subagent on the **changed diff or touched paths** as a **quality verification** step—the same way you would run tests or linters. The main agent (or the user) still **applies** fixes; the subagent only **reports**.
- The user asks for a **standalone** audit, **standards check**, or **pattern** review (no concurrent implementation in the same request is required).
- The user names **`go-quality-reviewer`** or points at **`.cursor/agents/go-quality-reviewer.md`**.
- After **large or risky Go changes** (new packages, concurrency, error paths)—propose or run this review **before merge**.
- The user wants an **exhaustive location list** (`file:line`) of violations plus **suggested fixes** in the report (the subagent does not apply them).

**Separation of roles:** wanting **code changes** does **not** mean skipping this skill. Use it to **verify** that new or edited code meets the bar. Only the **subagent** is read-only; **implementation** is done by the main agent or the user **after** (or in a follow-up turn) using the report.

## How to delegate

1. **Invoke the subagent** using the mechanism your Cursor environment supports for **named project agents** (subagent name: `go-quality-reviewer`). If the UI requires a path, use **`.cursor/agents/go-quality-reviewer.md`**.
2. **Pass explicit scope** in the same turn (the subagent defaults to recent changes if the user said “my changes” but did not specify):
   - **Branch / PR:** e.g. “Review diff vs `main`” or “Review commits on this branch.”
   - **Paths:** e.g. `pkg/kafka/`, `cmd/…`, or specific files.
   - **Git diff:** e.g. `git diff main...HEAD` or staged diff—state the base ref.
3. **Read-only tools only** for the subagent: file reads, search, `git diff`. No writes, formatters, or patches.

If subagent delegation is **not** available in the session, **read** [`.cursor/agents/go-quality-reviewer.md`](../../agents/go-quality-reviewer.md) and follow that prompt **yourself** (still read-only): same scope rules and same report format.

## What the user should get back

Per the subagent definition, the report **must** include:

1. **Summary** — themes and severity mix.
2. **Findings** — for each issue type: principle violated, what is wrong, **exhaustive** `path:line` list (or grouped counts + how to reproduce search if huge), **suggested fix** (text/snippet).
3. Optional **positives** and **ordered next steps** (by severity).

Findings use **Critical / High / Medium / Low**.

## Coordination with the main agent

- **Typical loop:** implement or change Go code → **delegate `go-quality-reviewer`** on the relevant scope → read the report → **main agent applies** suggested fixes (or the user does). The **subagent** never edits files.
- Repo-wide Go bar lives in **`.cursor/rules/agent-golang-and-replies.mdc`**; follow it while **writing**, and use **`go-quality-reviewer`** to **verify** that the result matches conventions and patterns (read-only check).

## Quick prompt examples (for the user or for delegation)

- “Run **go-quality-reviewer** on `git diff origin/main...HEAD`.”
- “Use the Go quality subagent on `pkg/config/` and `pkg/metrics/` only.”
- “Read-only review: discarded errors and missing `context` in the Kafka client package.”
