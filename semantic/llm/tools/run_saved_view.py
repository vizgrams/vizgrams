# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""run_saved_view — invoke an existing saved view (Epic 20 VG-234).

Twin of ``run_saved_query``. Difference: views carry their own chart
spec (chart_type + axes + drilldown), so when the LLM picks this tool,
the orchestrator should bypass ``text2view``'s chart picker and use the
saved chart instead. Otherwise we'd lose the author's chart choices and
re-derive them, which defeats most of the point of reuse.

Handler delegates to ``view_service.execute_view`` (same path the
existing UI uses) and translates the view's ``type`` + ``visualization``
into the chat's normalised chart shape:

  view.type=chart  → chart_type ∈ {bar, line, scatter} from chart_type
  view.type=table  → chart_type=table, columns from visualization.columns
  view.type=metric → chart_type=kpi,   y_field from measure
  view.type=map    → chart_type=table  (chat doesn't render maps yet)
"""

from __future__ import annotations

import logging

from semantic.llm.tools.registry import Tool, ToolContext, ToolResult

logger = logging.getLogger(__name__)


PARAMETERS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": (
                "Exact name of the saved view to invoke (from find_artifacts "
                "results, kind='view'). Case-sensitive."
            ),
        },
        "params": {
            "type": "object",
            "description": (
                "Optional parameter values forwarded to the view's underlying "
                "query. Values are strings."
            ),
            "additionalProperties": {"type": "string"},
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
            "description": "Row cap. Defaults to 1000.",
        },
    },
    "required": ["name"],
}


# ---------------------------------------------------------------------------
# View-shape → chat chart-shape
# ---------------------------------------------------------------------------


def _chart_spec_from_view(view_payload: dict) -> dict:
    """Translate a view detail dict into the chat's normalised chart spec.

    Returns ``{chart_type, x_field, y_field, color_field, drilldown}``.
    Falls back to ``table`` for anything we don't have a chat renderer for.
    """
    vtype = view_payload.get("type")
    viz = view_payload.get("visualization") or {}

    if vtype == "chart":
        ct = viz.get("chart_type") or "bar"
        # Chat doesn't render calendar_heatmap; fall back to table.
        if ct not in ("bar", "line", "scatter"):
            return {
                "chart_type": "table",
                "x_field": None,
                "y_field": None,
                "color_field": None,
                "drilldown": viz.get("point_drilldown") or viz.get("row_drilldown"),
            }
        # Views store y as a list; the chat shape is a single y_field.
        # Take the first; the data table still has the rest if the user
        # wants them.
        y_list = viz.get("y") or []
        return {
            "chart_type": ct,
            "x_field": viz.get("x"),
            "y_field": y_list[0] if y_list else None,
            "color_field": viz.get("color"),
            "drilldown": viz.get("point_drilldown"),
        }
    if vtype == "table":
        return {
            "chart_type": "table",
            "x_field": None,
            "y_field": None,
            "color_field": None,
            "drilldown": viz.get("row_drilldown"),
        }
    if vtype == "metric":
        return {
            "chart_type": "kpi",
            "x_field": None,
            "y_field": view_payload.get("measure"),
            "color_field": None,
            "drilldown": None,
        }
    # map / unknown → fall back to table so the user at least sees rows.
    return {
        "chart_type": "table",
        "x_field": None, "y_field": None, "color_field": None,
        "drilldown": None,
    }


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


def _handler(args: dict, ctx: ToolContext) -> ToolResult:
    """Look the view up by name; execute via view_service; package the result."""
    if ctx.model_dir is None:
        return ToolResult(
            payload={"error": "no model_dir wired into ToolContext"},
            success=False,
        )

    name = (args.get("name") or "").strip()
    if not name:
        return ToolResult(payload={"error": "missing required arg 'name'"}, success=False)

    params = args.get("params") or {}
    limit = int(args.get("limit") or 1000)

    from api.services import view_service

    try:
        result = view_service.execute_view(
            ctx.model_dir, name, limit=limit, params=params or None,
        )
    except KeyError:
        return ToolResult(
            payload={"error": f"saved view {name!r} not found"},
            success=False,
        )
    except Exception as exc:  # noqa: BLE001 — error string goes to the LLM
        logger.warning("run_saved_view(%s) failed: %s", name, exc)
        return ToolResult(
            payload={"error": f"{type(exc).__name__}: {exc}"},
            success=False,
        )

    rows = result.get("rows", [])
    columns = result.get("columns", []) or []
    row_count = result.get("total_row_count", result.get("row_count", len(rows)))
    chart_spec = _chart_spec_from_view(result)

    # Surface the view + underlying query YAMLs for provenance + the
    # orchestrator's source-viewer tabs.
    view_yaml: str | None = None
    query_yaml: str | None = None
    underlying_query_name = result.get("query")
    try:
        view_detail = view_service.get_view(ctx.model_dir, name)
        view_yaml = view_detail.get("raw_yaml")
    except Exception:  # noqa: BLE001
        view_yaml = None
    if underlying_query_name:
        try:
            from api.services import query_service
            qd = query_service.get_query(ctx.model_dir, underlying_query_name)
            query_yaml = qd.get("raw_yaml")
        except Exception:  # noqa: BLE001
            query_yaml = None

    return ToolResult(
        payload={
            "columns": list(columns),
            "rows": [list(r) for r in rows],
            "row_count": row_count,
            "view_name": name,
            # The LLM can see the chart spec too — useful if it wants to
            # produce a caption that references the chart shape.
            "chart_type": chart_spec["chart_type"],
        },
        success=True,
        extras={
            "view_yaml": view_yaml,
            "querydef_yaml": query_yaml,
            "sql": result.get("sql"),
            "truncated": bool(result.get("truncated", False)),
            "saved_view_name": name,
            "underlying_query": underlying_query_name,
            "chart_spec": chart_spec,
        },
    )


def _summarize(result: ToolResult) -> str:
    """One-line trace summary for VG-239 'Show your work'."""
    p = result.payload
    rows = p.get("row_count", 0)
    name = p.get("view_name") or "?"
    ct = p.get("chart_type") or "?"
    return f"{name}: {rows} rows (chart={ct})"


RUN_SAVED_VIEW = Tool(
    name="run_saved_view",
    description=(
        "Invoke an existing saved view by name. Use this when find_artifacts "
        "returns a view whose description matches the user's intent — views "
        "carry their own chart spec + drilldown config, so running one "
        "preserves the author's choices end-to-end (rather than having the "
        "chat re-derive a chart with present_view)."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("query_authoring", "reuse", "view_invocation"),
    summarize=_summarize,
)
