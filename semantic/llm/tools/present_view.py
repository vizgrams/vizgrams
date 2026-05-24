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


def _summarize(result: ToolResult) -> str:
    """One-line trace summary for VG-239 'Show your work'."""
    p = result.payload
    x = p.get("x_field") or "—"
    y = p.get("y_field") or "—"
    return f"chart={p.get('chart_type')} x={x} y={y}"


PRESENT_VIEW = Tool(
    name="present_view",
    description=(
        "Render the previous tool's query result as a chart. Call this "
        "exactly once after build_and_run_query or run_saved_query — the "
        "rows + columns from the most recent successful query become the "
        "data; this call picks the chart type and a caption. Don't call "
        "after run_saved_view (the saved view already carries its chart)."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    # query_authoring: visible to the unified chat orchestrator alongside
    # find_artifacts / build_and_run_query / run_saved_* (single tool loop).
    # view_selection: legacy tag kept for any caller filtering to just
    # the chart picker.
    tags=("query_authoring", "view_selection"),
    terminal=True,
    summarize=_summarize,
)
