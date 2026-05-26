# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""LLM-as-judge scoring (Epic 23 VG-263).

A separate LLM scores the chat orchestrator's response against the
case's prose expectations. Two axes per case — query correctness and
chart correctness — each 1-5 with notes. Use a different provider /
model than the authoring one to avoid same-model bias.

The judge is invoked with a forced tool call so the output is
structured. No raw JSON parsing from prose.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from semantic.llm.provider import LLMResponse, ToolCall

if TYPE_CHECKING:
    from evals.runner import CaseRun
    from semantic.llm.provider import LLMClient


# ---------------------------------------------------------------------------
# Scoring shape
# ---------------------------------------------------------------------------


@dataclass
class Score:
    """One judged response."""

    query_score: int        # 1-5 (1 = wrong entity / measure; 5 = bullseye)
    query_notes: str
    chart_score: int        # 1-5 (1 = wrong shape entirely; 5 = right + axes)
    chart_notes: str
    summary: str            # one-sentence overall verdict
    # Set when the judge errored out (network, bad parse, etc.). Score
    # fields then carry zero; ``summary`` carries the error string.
    error: str | None = None

    @property
    def overall(self) -> float:
        """Mean of the two axes — 1.0 to 5.0."""
        return (self.query_score + self.chart_score) / 2.0


# ---------------------------------------------------------------------------
# Judge prompt
# ---------------------------------------------------------------------------


_JUDGE_SYSTEM_PROMPT = """You are a senior data analyst reviewing the output of an
LLM-powered chat agent that answers data questions by authoring SQL-like
queries and picking charts.

For each turn you will see:
  - the user's question
  - the data model the agent was working against (entity schema)
  - the agent's authored query (or saved-query reference)
  - the chart spec it produced (chart_type + axes)
  - the human author's expectations for query + chart correctness

Score each axis on a 1-5 scale by calling the ``score_response`` tool
exactly once. Use the rubrics below:

Query score (was the data shape right?):
  5  Hit every expectation. Right root entity, right measures with the
     correct field, filters present where expected, grouping correct.
  4  Right shape with one minor issue (e.g. correct entity + measure
     but wrong format granularity).
  3  Got the broad direction right but missed something material
     (wrong measure name, missing a relevant filter).
  2  Wrong measure or wrong grouping — the answer would not address
     the question.
  1  Wrong root entity, or query failed / wasn't produced at all.

Chart score (was the chart appropriate?):
  5  Right chart type for the data shape AND axes correctly mapped.
  4  Right type, axes slightly off (wrong x picked from valid choices).
  3  Different type than ideal but defensible (bar where line would
     be clearer; readable but not the obvious choice).
  2  Wrong type for the data shape (kpi for multi-row; bar without a
     category column).
  1  Chart could not be rendered (missing fields, type doesn't match
     the data at all).

In ``notes`` for each axis, name the SPECIFIC issue if the score is
below 5. Don't hedge — be concrete enough that a developer can act on it.
"""


_SCORE_TOOL_SCHEMA: dict = {
    "type": "function",
    "function": {
        "name": "score_response",
        "description": "Record the two-axis score for one chat-response review.",
        "parameters": {
            "type": "object",
            "properties": {
                "query_score": {
                    "type": "integer", "minimum": 1, "maximum": 5,
                    "description": "1-5 rubric — query correctness.",
                },
                "query_notes": {
                    "type": "string",
                    "description": "Specific issues with the query (or 'matches expectations').",
                },
                "chart_score": {
                    "type": "integer", "minimum": 1, "maximum": 5,
                    "description": "1-5 rubric — chart correctness.",
                },
                "chart_notes": {
                    "type": "string",
                    "description": "Specific issues with the chart pick (or 'matches expectations').",
                },
                "summary": {
                    "type": "string",
                    "description": "One sentence overall verdict.",
                },
            },
            "required": ["query_score", "query_notes", "chart_score",
                         "chart_notes", "summary"],
        },
    },
}


# ---------------------------------------------------------------------------
# Judge invocation
# ---------------------------------------------------------------------------


def judge(
    run: CaseRun,
    *,
    llm_client: LLMClient,
    judge_model: str | None = None,
) -> Score:
    """Score one CaseRun. Returns a Score (errors captured in .error)."""
    user_msg = _build_user_message(run)
    messages = [
        {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]
    try:
        resp: LLMResponse = llm_client.complete(
            messages=messages,
            tools=[_SCORE_TOOL_SCHEMA],
            model=judge_model,
        )
    except Exception as exc:  # noqa: BLE001
        return _error_score(f"judge LLM call failed: {type(exc).__name__}: {exc}")

    tc = _first_tool_call(resp, expected="score_response")
    if tc is None:
        return _error_score(
            f"judge did not call score_response (content={resp.content!r})"
        )

    args = tc.arguments or {}
    try:
        return Score(
            query_score=int(args["query_score"]),
            query_notes=str(args["query_notes"]),
            chart_score=int(args["chart_score"]),
            chart_notes=str(args["chart_notes"]),
            summary=str(args["summary"]),
        )
    except (KeyError, ValueError, TypeError) as exc:
        return _error_score(
            f"judge args missing/invalid: {exc} — got {json.dumps(args)}",
        )


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------


def _build_user_message(run: CaseRun) -> str:
    """Render the judge's user message — case + response in a structured
    JSON-like block. Plain text rather than JSON so the LLM has more
    latitude with quoting in the YAML fields."""
    r = run.result
    parts = [
        f"# User question\n{run.case.prompt}",
        f"# Model under test\n{run.case.model}",
        f"# Query expectation\n{run.case.query_expectation or '(none given)'}",
        f"# Chart expectation\n{run.case.chart_expectation or '(none given)'}",
        "",
        "# Agent response",
        f"  success: {r.success}",
        f"  iterations: {r.iterations}",
        f"  tool sequence: {' → '.join(run.tool_sequence) or '(none)'}",
        f"  title: {r.title or '(none)'}",
    ]
    if r.error:
        parts.append(f"  error: {r.error}")
    if r.query_yaml:
        parts.append("\n## Query YAML")
        parts.append("```yaml")
        parts.append(r.query_yaml.strip())
        parts.append("```")
    if r.view_yaml:
        parts.append("\n## View YAML (chart spec)")
        parts.append("```yaml")
        parts.append(r.view_yaml.strip())
        parts.append("```")
    if r.saved_view:
        parts.append(f"\n## Saved view reference\n  {json.dumps(r.saved_view)}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_tool_call(resp: LLMResponse, *, expected: str) -> ToolCall | None:
    for tc in resp.tool_calls or []:
        if tc.name == expected:
            return tc
    return None


def _error_score(message: str) -> Score:
    return Score(
        query_score=0, query_notes="", chart_score=0, chart_notes="",
        summary=message, error=message,
    )
