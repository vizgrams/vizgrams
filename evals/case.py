# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Eval case definition + YAML loader.

A case is a single (prompt, expected behaviour) pair. ``expectations``
are free-form prose anchors the judge uses — not regex match strings.
This keeps the dataset readable + lets the judge weigh shades of
"close" rather than binary pass/fail.

YAML shape:

    id: weekly_pr_throughput
    model: example
    prompt: "Show me weekly PR throughput"
    expectations:
      query: |
        Should root on PullRequest.
        Should bucket by week (format_time YYYY-WW or a saved week_key).
        Measure should be count of pull_request_key.
      chart: |
        Time series — line preferred; bar acceptable for weekly buckets.
        Not kpi (multi-row), not table (chartable shape).
        x_field = week column, y_field = count.
    tags: [time_series, novel]      # optional, for filtering reports
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class EvalCase:
    """One eval test case."""

    id: str
    model: str                          # which model dir to run against
    prompt: str
    # Free-form prose the judge uses to score query / chart correctness.
    # Keep these short + factual — the judge reads them verbatim.
    query_expectation: str = ""
    chart_expectation: str = ""
    tags: list[str] = field(default_factory=list)
    # The source YAML path, for traceability in reports.
    source_path: str | None = None


def load_case(path: Path) -> EvalCase:
    """Parse one YAML file into an EvalCase."""
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level must be a mapping")

    case_id = data.get("id") or path.stem
    model = data.get("model")
    prompt = data.get("prompt")
    if not model or not prompt:
        raise ValueError(f"{path}: 'model' and 'prompt' are required")

    expectations = data.get("expectations") or {}
    if not isinstance(expectations, dict):
        raise ValueError(f"{path}: 'expectations' must be a mapping")

    return EvalCase(
        id=case_id,
        model=model,
        prompt=prompt,
        query_expectation=str(expectations.get("query") or "").strip(),
        chart_expectation=str(expectations.get("chart") or "").strip(),
        tags=list(data.get("tags") or []),
        source_path=str(path),
    )


def load_cases(cases_dir: Path) -> list[EvalCase]:
    """Load every ``*.yaml`` under ``cases_dir`` (non-recursive). Sorted by id."""
    cases = [load_case(p) for p in sorted(cases_dir.glob("*.yaml"))]
    # Detect duplicate ids — the report assumes uniqueness.
    seen = set()
    for c in cases:
        if c.id in seen:
            raise ValueError(f"duplicate case id {c.id!r}")
        seen.add(c.id)
    return cases
