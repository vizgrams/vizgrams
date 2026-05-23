# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""present_view — chart spec + caption (Epic 20 VG-231).

Terminal tool used by ``text2view``. Returns the chart picker's output;
the orchestrator ends the loop on a successful call.
"""

from __future__ import annotations

from semantic.llm.tools.registry import Tool, ToolContext, ToolResult

PARAMETERS_SCHEMA: dict = {
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
}


def _handler(args: dict, ctx: ToolContext) -> ToolResult:  # noqa: ARG001 — ctx unused for now
    """Validate the LLM's chart spec and bounce it back as the terminal result."""
    chart_type = args.get("chart_type")
    caption = args.get("caption", "")
    if not chart_type or not caption:
        return ToolResult(
            payload={"error": "missing required chart_type or caption"},
            success=False,
        )
    return ToolResult(
        payload={
            "chart_type": chart_type,
            "x_field": args.get("x_field"),
            "y_field": args.get("y_field"),
            "color_field": args.get("color_field"),
            "caption": caption,
        },
        success=True,
        terminate=True,
    )


PRESENT_VIEW = Tool(
    name="present_view",
    description=(
        "Specify the chart spec and caption for the given query result. "
        "Call exactly once."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("view_selection",),
    terminal=True,
)
