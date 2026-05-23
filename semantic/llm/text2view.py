# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""text2view — query result → chart spec + caption.

Given a query result (columns + sample rows), picks an appropriate chart
type and column-to-axis mapping, plus a one-sentence caption. Single LLM
call, no retry — the worst case is a bad chart, which is recoverable by
the user clicking "edit view" rather than the LLM looping.

The tool ``present_view`` is **forced** (``tool_choice`` pinned to it) so
the LLM emits structured output every time. Cheap and reliable — pattern
borrowed from the OpenAI function-calling cookbook.

Reusable on its own: a "suggest a view for this saved query" affordance
(post-launch) calls this with the saved query's result without going
through the explore-chat orchestrator at all.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Literal

import yaml

from semantic.llm.provider import LLMClient

ChartType = Literal["bar", "line", "table", "scatter", "kpi"]


@dataclass
class Text2ViewResult:
    success: bool
    yaml: str | None = None
    chart_type: ChartType | None = None
    x_field: str | None = None
    y_field: str | None = None
    color_field: str | None = None
    caption: str = ""
    error: str | None = None
    raw_args: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Forced tool — single structured output
# ---------------------------------------------------------------------------


PRESENT_VIEW_TOOL = {
    "type": "function",
    "function": {
        "name": "present_view",
        "description": (
            "Specify the chart spec and caption for the given query result. "
            "Call exactly once."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "line", "table", "scatter", "kpi"],
                    "description": (
                        "bar: categorical x + single numeric y (top-N, group counts). "
                        "line: ordered x (time, sequence) + numeric y. "
                        "scatter: two numeric measures, one row per point. "
                        "kpi: single scalar result (one row, one number). "
                        "table: heterogeneous columns or many columns where a chart wouldn't read well."
                    ),
                },
                "x_field": {
                    "type": "string",
                    "description": "Column name from the result for the x-axis (or category). Omit for kpi.",
                },
                "y_field": {
                    "type": "string",
                    "description": "Column name from the result for the y-axis (or value).",
                },
                "color_field": {
                    "type": "string",
                    "description": "Optional column for series / colour split.",
                },
                "caption": {
                    "type": "string",
                    "description": (
                        "1-2 sentence insight. Cite specific numbers from the rows. "
                        "Do not start with 'This chart' or 'The data shows'."
                    ),
                },
            },
            "required": ["chart_type", "caption"],
        },
    },
}


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = """You pick a chart type and caption for a query result. Call `present_view`
exactly once with your choice.

Choosing a chart:
- One scalar (one row, one numeric column) → kpi.
- Time series (one ordered column + one or two numeric columns) → line.
- Top-N or group counts (one categorical column + one numeric column) → bar.
- Two numeric measures forming a cloud of points → scatter.
- Many heterogeneous columns or no clear x/y → table.

The caption must mention specific values from the rows (e.g. "peaked at
1,485 in October"). Avoid "The data shows…" and "This chart…"."""


# ---------------------------------------------------------------------------
# View YAML serialisation
# ---------------------------------------------------------------------------


def view_yaml(
    *,
    name: str,
    query_name: str,
    chart_type: ChartType,
    x_field: str | None,
    y_field: str | None,
    color_field: str | None,
    caption: str,  # noqa: ARG001 — kept in signature for callers; not in view schema
    columns: list[str] | None = None,
) -> str:
    """Render a ViewDef YAML matching ``schemas/view.yaml``.

    Maps our chart-picker enum onto the four allowed view types:

      kpi          → type: metric   (measure: single column)
      table        → type: table    (visualization.columns: all)
      bar / line / scatter → type: chart  (visualization.chart_type + x + y)

    The caption is *not* part of the view schema; callers keep it
    alongside on the orchestrator's ``ChatTurnResult`` so this function
    can produce something the validator accepts.
    """
    if chart_type == "kpi":
        measure = y_field or (columns[0] if columns else "")
        body: dict = {
            "name": name,
            "type": "metric",
            "query": query_name,
            "measure": measure,
            "visualization": {},
        }
    elif chart_type == "table":
        body = {
            "name": name,
            "type": "table",
            "query": query_name,
            "visualization": {
                "columns": list(columns or []),
            },
        }
    else:  # bar, line, scatter
        viz: dict = {"chart_type": chart_type}
        if x_field:
            viz["x"] = x_field
        if y_field:
            viz["y"] = [y_field]
        if color_field:
            viz["color"] = color_field
        body = {
            "name": name,
            "type": "chart",
            "query": query_name,
            "visualization": viz,
        }
    return yaml.safe_dump(body, sort_keys=False)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def text2view_yaml(
    *,
    columns: list[str],
    rows: list[list],
    user_intent: str | None = None,
    llm_client: LLMClient,
    llm_model: str | None = None,
    view_name: str = "text2view",
    query_name: str = "text2query",
    rows_to_llm: int = 20,
) -> Text2ViewResult:
    """Pick a chart spec + caption for a query result.

    Always a single LLM call. The tool is forced — the LLM cannot return
    plain text. If the call somehow fails to produce a valid tool call,
    we surface that as ``success=False`` rather than raising.
    """
    sample_rows = [list(r) for r in rows[:rows_to_llm]]
    user_msg = {
        "intent": user_intent,
        "columns": columns,
        "rows": sample_rows,
        "row_count_total": len(rows),
    }

    messages: list[dict] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(user_msg, default=str)},
    ]

    resp = llm_client.complete(
        messages=messages,
        tools=[PRESENT_VIEW_TOOL],
        model=llm_model,
    )

    if not resp.tool_calls:
        return Text2ViewResult(
            success=False,
            error="LLM did not call present_view (no tool calls in response)",
        )

    tc = resp.tool_calls[0]
    if tc.name != "present_view":
        return Text2ViewResult(
            success=False,
            error=f"LLM called unexpected tool {tc.name!r}",
        )

    args = tc.arguments
    chart_type = args.get("chart_type")
    caption = args.get("caption", "")
    if not chart_type or not caption:
        return Text2ViewResult(
            success=False,
            error="present_view missing required chart_type or caption",
            raw_args=args,
        )

    return Text2ViewResult(
        success=True,
        chart_type=chart_type,
        x_field=args.get("x_field"),
        y_field=args.get("y_field"),
        color_field=args.get("color_field"),
        caption=caption,
        yaml=view_yaml(
            name=view_name,
            query_name=query_name,
            chart_type=chart_type,
            x_field=args.get("x_field"),
            y_field=args.get("y_field"),
            color_field=args.get("color_field"),
            caption=caption,
            columns=columns,
        ),
        raw_args=args,
    )
