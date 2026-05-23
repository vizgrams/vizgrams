# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""text2view — query result → chart spec + caption.

Given a query result (columns + sample rows), picks an appropriate chart
type and column-to-axis mapping, plus a one-sentence caption. Single LLM
call, no retry — the worst case is a bad chart, which is recoverable by
the user clicking "edit view" rather than the LLM looping.

The ``present_view`` tool is **forced** (``tool_choice`` pinned to it) so
the LLM emits structured output every time. The tool itself lives in
``semantic/llm/tools/present_view.py`` and is pulled from the
``ToolRegistry`` at call time — same registration powers any external
MCP server (Epic 24 / later).

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
from semantic.llm.tools.registry import (
    ToolCallTrace,
    ToolContext,
    ToolRegistry,
    summarize_tool_result,
)

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
    # VG-239: single-step trace (text2view always makes exactly one LLM
    # call; carrying it as a list keeps the shape consistent with
    # ``Text2QueryResult.trace`` so the orchestrator can concat them.)
    trace: list[ToolCallTrace] = field(default_factory=list)


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
    registry: ToolRegistry,
    llm_client: LLMClient,
    user_intent: str | None = None,
    llm_model: str | None = None,
    view_name: str = "text2view",
    query_name: str = "text2query",
    rows_to_llm: int = 20,
    tool_name: str = "present_view",
) -> Text2ViewResult:
    """Pick a chart spec + caption for a query result.

    Always a single LLM call. The ``present_view`` tool is forced via
    ``tool_choice`` so the LLM cannot return plain text. Pulls the tool
    from ``registry`` — the same registration the chat orchestrator
    uses; same registration any future MCP server would expose.
    """
    tool = registry.get(tool_name)
    if tool is None:
        return Text2ViewResult(
            success=False,
            error=f"Tool {tool_name!r} not registered",
        )

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

    # Render only the present_view tool; the LLM has nothing else to call.
    openai_tools = registry.to_openai_tools(names=[tool_name])

    resp = llm_client.complete(
        messages=messages, tools=openai_tools, model=llm_model,
    )

    if not resp.tool_calls:
        return Text2ViewResult(
            success=False,
            error="LLM did not call present_view (no tool calls in response)",
        )

    tc = resp.tool_calls[0]
    if tc.name != tool_name:
        return Text2ViewResult(
            success=False,
            error=f"LLM called unexpected tool {tc.name!r}",
        )

    # Dispatch via the registry — same as text2query — so the handler's
    # validation runs (missing chart_type / caption surface as success=False).
    # present_view doesn't read from ctx, so the default is fine.
    result = registry.dispatch(tc.name, tc.arguments, ToolContext())

    # Capture the single trace step regardless of success.
    tool_def = registry.get(tc.name)
    trace = [ToolCallTrace(
        name=tc.name, arguments=tc.arguments,
        success=result.success,
        summary=summarize_tool_result(
            tc.name, result,
            summarize_hook=tool_def.summarize if tool_def else None,
        ),
        payload=dict(result.payload),
    )]

    if not result.success:
        return Text2ViewResult(
            success=False,
            error=result.payload.get("error") or "present_view returned failure",
            raw_args=tc.arguments,
            trace=trace,
        )

    chart_type = result.payload["chart_type"]
    caption = result.payload["caption"]
    return Text2ViewResult(
        success=True,
        chart_type=chart_type,
        x_field=result.payload.get("x_field"),
        y_field=result.payload.get("y_field"),
        color_field=result.payload.get("color_field"),
        caption=caption,
        yaml=view_yaml(
            name=view_name,
            query_name=query_name,
            chart_type=chart_type,
            x_field=result.payload.get("x_field"),
            y_field=result.payload.get("y_field"),
            color_field=result.payload.get("color_field"),
            caption=caption,
            columns=columns,
        ),
        raw_args=tc.arguments,
        trace=trace,
    )
