# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Aggregate scored cases into JSON + markdown reports.

JSON for machine-readable history (commit per run to track drift),
markdown for at-a-glance human reading.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from evals.judge import Score
    from evals.runner import CaseRun


@dataclass
class ScoredCase:
    """Pair of CaseRun + Score for the report."""

    case_id: str
    prompt: str
    model: str
    tags: list[str]
    duration_s: float
    tool_sequence: list[str]
    succeeded: bool
    query_score: int
    query_notes: str
    chart_score: int
    chart_notes: str
    summary: str
    judge_error: str | None = None


def build_report(scored: list[ScoredCase], *, ran_at: str | None = None) -> dict:
    """Compute aggregate stats + return a JSON-serialisable dict."""
    if not scored:
        return {
            "ran_at": ran_at or _now(),
            "case_count": 0,
            "cases": [],
            "stats": {},
        }
    judged = [s for s in scored if s.judge_error is None]
    return {
        "ran_at": ran_at or _now(),
        "case_count": len(scored),
        "judged_count": len(judged),
        "judge_failure_count": len(scored) - len(judged),
        "stats": {
            "mean_query_score": _mean(s.query_score for s in judged),
            "mean_chart_score": _mean(s.chart_score for s in judged),
            "mean_overall": _mean(
                (s.query_score + s.chart_score) / 2.0 for s in judged
            ),
            "succeeded_count": sum(1 for s in scored if s.succeeded),
        },
        "cases": [asdict(s) for s in scored],
    }


def to_markdown(report: dict) -> str:
    """Render the JSON report as a human-readable markdown blob."""
    if report["case_count"] == 0:
        return "# Eval report\n\n_No cases run._\n"

    s = report["stats"]
    lines = [
        "# Eval report",
        f"\nRan at: {report['ran_at']}",
        f"Cases: {report['case_count']}  ·  Succeeded: {s['succeeded_count']}"
        f"  ·  Judge failures: {report['judge_failure_count']}",
        "",
        "## Aggregate scores",
        "",
        "| Axis | Mean |",
        "|---|---|",
        f"| Query | {s['mean_query_score']:.2f} / 5 |",
        f"| Chart | {s['mean_chart_score']:.2f} / 5 |",
        f"| **Overall** | **{s['mean_overall']:.2f} / 5** |",
        "",
        "## Cases",
        "",
        "| ID | Q | C | Tools | Summary |",
        "|---|---|---|---|---|",
    ]
    for c in report["cases"]:
        # Show "—" for the score when the judge errored so the column
        # doesn't show a misleading "0".
        q = c["query_score"] if c["judge_error"] is None else "—"
        ch = c["chart_score"] if c["judge_error"] is None else "—"
        tools = " → ".join(c["tool_sequence"]) or "(none)"
        summary = c["summary"].replace("|", "\\|")
        lines.append(f"| `{c['case_id']}` | {q} | {ch} | {tools} | {summary} |")

    # Add detailed notes for any case scored < 4 — that's where the
    # signal for iteration lives.
    weak = [c for c in report["cases"]
            if c["judge_error"] is None
            and (c["query_score"] < 4 or c["chart_score"] < 4)]
    if weak:
        lines.append("\n## Cases needing attention\n")
        for c in weak:
            lines.append(f"### `{c['case_id']}` — {c['prompt']}")
            lines.append(f"Query ({c['query_score']}/5): {c['query_notes']}")
            lines.append(f"Chart ({c['chart_score']}/5): {c['chart_notes']}")
            lines.append("")

    return "\n".join(lines) + "\n"


def to_scored_case(run: CaseRun, score: Score) -> ScoredCase:
    """Bridge CaseRun + Score into the flat row the report consumes."""
    return ScoredCase(
        case_id=run.case.id,
        prompt=run.case.prompt,
        model=run.case.model,
        tags=list(run.case.tags),
        duration_s=run.duration_s,
        tool_sequence=list(run.tool_sequence),
        succeeded=run.succeeded,
        query_score=score.query_score,
        query_notes=score.query_notes,
        chart_score=score.chart_score,
        chart_notes=score.chart_notes,
        summary=score.summary,
        judge_error=score.error,
    )


def write_report(report: dict, *, out_dir: Path) -> tuple[Path, Path]:
    """Write both JSON + markdown side-by-side. Returns ``(json_path, md_path)``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "report.json"
    md_path = out_dir / "report.md"
    json_path.write_text(json.dumps(report, indent=2, default=str))
    md_path.write_text(to_markdown(report))
    return json_path, md_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mean(it) -> float:
    vals = list(it)
    return sum(vals) / len(vals) if vals else 0.0


def _now() -> str:
    return datetime.now(UTC).isoformat()
