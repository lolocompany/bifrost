#!/usr/bin/env python3
"""Codequality pipeline for bifrost."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "reports" / "codequality" / "raw"
OUT_DIR = ROOT / "reports" / "codequality"
SNAPSHOT_DIR = OUT_DIR / "snapshots"
BASELINE_PATH = OUT_DIR / "baseline.json"

THRESHOLDS = {
    "coverage_total_target": 70.0,
    "cyclo_warn": 10,
    "cyclo_red": 15,
    "cogn_warn": 15,
    "cogn_red": 20,
    "duplication_warn_groups": 1,
    "duplication_red_groups": 5,
}


@dataclass
class ComplexityStats:
    max_value: int
    over_warn: int
    over_red: int
    top: List[Dict[str, object]]


def run(command: List[str], output_path: Path | None = None) -> str:
    proc = subprocess.run(command, cwd=ROOT, check=True, text=True, capture_output=True)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(proc.stdout, encoding="utf-8")
    return proc.stdout


def ensure_tools() -> None:
    tools = [
        "github.com/fzipp/gocyclo/cmd/gocyclo@latest",
        "github.com/uudashr/gocognit/cmd/gocognit@latest",
        "github.com/mibk/dupl@latest",
    ]
    for tool in tools:
        run(["go", "install", tool])


def collect_raw() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    run(
        [
            "go",
            "test",
            "./test/unit/...",
            "-coverprofile=reports/codequality/raw/unit-coverage.out",
            "-coverpkg=./pkg/...",
        ]
    )
    run(
        ["go", "tool", "cover", "-func=reports/codequality/raw/unit-coverage.out"],
        RAW_DIR / "unit-coverage-summary.txt",
    )

    gopath = run(["go", "env", "GOPATH"]).strip()
    run([f"{gopath}/bin/gocyclo", "-over", "0", "./pkg", "./cmd"], RAW_DIR / "gocyclo.txt")
    run([f"{gopath}/bin/gocognit", "-over", "0", "./pkg", "./cmd"], RAW_DIR / "gocognit.txt")
    run([f"{gopath}/bin/dupl", "-t", "60", "./pkg", "./cmd"], RAW_DIR / "dupl.txt")

    imports = run(["go", "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./pkg/...", "./cmd/..."])
    (RAW_DIR / "imports.txt").write_text(imports, encoding="utf-8")

    git_log = run(["git", "log", "--since=180 days ago", "--name-only", "--pretty=format:"])
    counts: Dict[str, int] = defaultdict(int)
    for line in git_log.splitlines():
        item = line.strip()
        if item.endswith(".go") and (item.startswith("pkg/") or item.startswith("cmd/")):
            counts[item] += 1
    churn_lines = [f"{count:5d} {path}" for path, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
    (RAW_DIR / "churn-180d.txt").write_text("\n".join(churn_lines) + ("\n" if churn_lines else ""), encoding="utf-8")


def parse_coverage() -> float:
    coverage_path = RAW_DIR / "unit-coverage-summary.txt"
    text = coverage_path.read_text(encoding="utf-8")
    match = re.search(r"^total:\s+\(statements\)\s+([0-9.]+)%$", text, flags=re.M)
    if not match:
        raise ValueError("Unable to parse total coverage from coverage summary")
    return float(match.group(1))


def parse_complexity(path: Path, warn: int, red: int) -> ComplexityStats:
    entries = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        value = int(parts[0])
        package = parts[1]
        func_name = parts[2]
        location = parts[3]
        entries.append(
            {
                "value": value,
                "package": package,
                "function": func_name,
                "location": location,
            }
        )

    entries.sort(key=lambda item: int(item["value"]), reverse=True)
    over_warn = sum(1 for item in entries if int(item["value"]) > warn)
    over_red = sum(1 for item in entries if int(item["value"]) > red)
    max_value = int(entries[0]["value"]) if entries else 0
    return ComplexityStats(max_value=max_value, over_warn=over_warn, over_red=over_red, top=entries[:10])


def parse_duplication() -> int:
    text = (RAW_DIR / "dupl.txt").read_text(encoding="utf-8")
    match = re.search(r"Found total\s+(\d+)\s+clone groups\.", text)
    if not match:
        return 0
    return int(match.group(1))


def parse_import_fanout() -> Dict[str, object]:
    fanouts = []
    for raw in (RAW_DIR / "imports.txt").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        import_path = parts[0]
        imports = parts[1:]
        internal_imports = [item for item in imports if item.startswith("github.com/lolocompany/bifrost/")]
        fanouts.append({"package": import_path, "fanout": len(internal_imports)})
    fanouts.sort(key=lambda item: item["fanout"], reverse=True)
    average = round(sum(item["fanout"] for item in fanouts) / len(fanouts), 2) if fanouts else 0.0
    return {
        "average": average,
        "max": fanouts[0]["fanout"] if fanouts else 0,
        "top": fanouts[:10],
    }


def complexity_by_file(path: Path) -> Dict[str, int]:
    per_file: Dict[str, int] = defaultdict(int)
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        value = int(parts[0])
        location = parts[3]
        file_path = location.split(":", 1)[0]
        per_file[file_path] += value
    return dict(per_file)


def churn_index() -> Dict[str, int]:
    churn: Dict[str, int] = {}
    for raw in (RAW_DIR / "churn-180d.txt").read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        count, path = raw.strip().split(maxsplit=1)
        churn[path] = int(count)
    return churn


def build_hotspots(cyclo_file: Dict[str, int], cogn_file: Dict[str, int], churn: Dict[str, int]) -> List[Dict[str, object]]:
    rows = []
    keys = set(churn.keys()) | set(cyclo_file.keys()) | set(cogn_file.keys())
    for path in keys:
        churn_score = churn.get(path, 0)
        cyclo_score = cyclo_file.get(path, 0)
        cogn_score = cogn_file.get(path, 0)
        risk = churn_score * (cyclo_score + cogn_score)
        rows.append(
            {
                "file": path,
                "churn_180d": churn_score,
                "cyclomatic_sum": cyclo_score,
                "cognitive_sum": cogn_score,
                "risk": risk,
            }
        )
    rows.sort(key=lambda item: item["risk"], reverse=True)
    return rows[:10]


def status_for_metric(value: float, green_limit: float, red_limit: float, lower_is_better: bool = True) -> str:
    if lower_is_better:
        if value <= green_limit:
            return "green"
        if value > red_limit:
            return "red"
        return "yellow"
    if value >= green_limit:
        return "green"
    if value < red_limit:
        return "red"
    return "yellow"


def summarize(coverage: float, cyclo: ComplexityStats, cogn: ComplexityStats, dup_groups: int) -> Dict[str, object]:
    return {
        "coverage_total": {
            "value": coverage,
            "target": THRESHOLDS["coverage_total_target"],
            "status": status_for_metric(coverage, THRESHOLDS["coverage_total_target"], 50.0, lower_is_better=False),
        },
        "cyclomatic_over_red": {
            "value": cyclo.over_red,
            "target": 0,
            "status": "green" if cyclo.over_red == 0 else ("yellow" if cyclo.over_red <= 10 else "red"),
        },
        "cognitive_over_red": {
            "value": cogn.over_red,
            "target": 0,
            "status": "green" if cogn.over_red == 0 else ("yellow" if cogn.over_red <= 10 else "red"),
        },
        "duplication_groups": {
            "value": dup_groups,
            "target": 0,
            "status": status_for_metric(
                float(dup_groups),
                THRESHOLDS["duplication_warn_groups"],
                THRESHOLDS["duplication_red_groups"],
            ),
        },
    }


def enforce(snapshot: Dict[str, object], baseline: Dict[str, object] | None) -> Tuple[bool, List[str]]:
    failures: List[str] = []
    summary = snapshot["summary"]

    # Absolute severe outlier gates.
    if snapshot["cyclomatic"]["max_value"] > 40:
        failures.append("Cyclomatic max exceeds severe outlier cap (>40).")
    if snapshot["cognitive"]["max_value"] > 80:
        failures.append("Cognitive max exceeds severe outlier cap (>80).")
    if int(snapshot["duplication"]["clone_groups"]) > THRESHOLDS["duplication_red_groups"]:
        failures.append("Duplication clone groups exceed red threshold.")

    # Baseline regression gates.
    if baseline:
        if summary["coverage_total"]["value"] < baseline["summary"]["coverage_total"]["value"]:
            failures.append("Coverage regressed below baseline.")
        if summary["cyclomatic_over_red"]["value"] > baseline["summary"]["cyclomatic_over_red"]["value"]:
            failures.append("Count of cyclomatic red functions increased above baseline.")
        if summary["cognitive_over_red"]["value"] > baseline["summary"]["cognitive_over_red"]["value"]:
            failures.append("Count of cognitive red functions increased above baseline.")
        if summary["duplication_groups"]["value"] > baseline["summary"]["duplication_groups"]["value"]:
            failures.append("Duplicate clone groups increased above baseline.")

    return (len(failures) == 0, failures)


def write_markdown(snapshot: Dict[str, object], path: Path) -> None:
    summary = snapshot["summary"]
    lines = [
        "# Codequality Scorecard",
        "",
        f"- Generated at: `{snapshot['generated_at']}`",
        "",
        "| Check | Value | Target | Status |",
        "| :-- | --: | --: | :-- |",
    ]
    for key, value in summary.items():
        lines.append(
            f"| `{key}` | `{value['value']}` | `{value['target']}` | `{value['status']}` |"
        )

    lines.extend(
        [
            "",
            "## Top Cyclomatic Functions",
        ]
    )
    for item in snapshot["cyclomatic"]["top"]:
        lines.append(f"- `{item['value']}` `{item['function']}` (`{item['location']}`)")

    lines.extend(
        [
            "",
            "## Top Cognitive Functions",
        ]
    )
    for item in snapshot["cognitive"]["top"]:
        lines.append(f"- `{item['value']}` `{item['function']}` (`{item['location']}`)")

    lines.extend(
        [
            "",
            "## Hotspots (churn x complexity)",
        ]
    )
    for item in snapshot["hotspots"]:
        lines.append(
            f"- `{item['risk']}` `{item['file']}` (churn={item['churn_180d']}, cyclo={item['cyclomatic_sum']}, cogn={item['cognitive_sum']})"
        )

    lines.extend(
        [
            "",
            "## Governance Next Step",
            "- Run `make codequality-gate` on each PR and block merges on regressions.",
            "- Review top hotspots weekly and pick at least one high-risk file for incremental refactor/test hardening.",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_baseline() -> Dict[str, object] | None:
    if not BASELINE_PATH.exists():
        return None
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run codequality pipeline")
    parser.add_argument("--skip-collect", action="store_true", help="Use existing raw files")
    parser.add_argument("--write-baseline", action="store_true", help="Write current snapshot as baseline")
    parser.add_argument("--enforce", action="store_true", help="Fail on enforced gates")
    parser.add_argument("--output-prefix", default="scorecard", help="Output file prefix in reports/codequality")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    if not args.skip_collect:
        ensure_tools()
        collect_raw()

    coverage = parse_coverage()
    cyclo = parse_complexity(RAW_DIR / "gocyclo.txt", THRESHOLDS["cyclo_warn"], THRESHOLDS["cyclo_red"])
    cogn = parse_complexity(RAW_DIR / "gocognit.txt", THRESHOLDS["cogn_warn"], THRESHOLDS["cogn_red"])
    dup_groups = parse_duplication()
    fanout = parse_import_fanout()
    hotspots = build_hotspots(
        complexity_by_file(RAW_DIR / "gocyclo.txt"),
        complexity_by_file(RAW_DIR / "gocognit.txt"),
        churn_index(),
    )

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": THRESHOLDS,
        "summary": summarize(coverage, cyclo, cogn, dup_groups),
        "coverage": {"total_percent": coverage},
        "cyclomatic": asdict(cyclo),
        "cognitive": asdict(cogn),
        "duplication": {"clone_groups": dup_groups},
        "modularity": {
            "fanout": fanout,
            "package_cycles": 0,  # Go compiler rejects package import cycles.
        },
        "hotspots": hotspots,
    }

    json_out = OUT_DIR / f"{args.output_prefix}.json"
    md_out = OUT_DIR / f"{args.output_prefix}.md"
    json_out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    write_markdown(snapshot, md_out)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (SNAPSHOT_DIR / f"{stamp}.json").write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")

    if args.write_baseline:
        BASELINE_PATH.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")

    if args.enforce:
        baseline = load_baseline()
        ok, failures = enforce(snapshot, baseline)
        if not ok:
            print("Codequality gates failed:")
            for failure in failures:
                print(f"- {failure}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
