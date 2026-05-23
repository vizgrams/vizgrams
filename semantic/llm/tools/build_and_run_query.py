# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""build_and_run_query — author + execute a query (Epic 20 VG-231).

The original tool definition + handler from ``text2query.py``, lifted into
the registry pattern. Schema is unchanged from the previous inline form
so existing prompts and behaviour are preserved.
"""

from __future__ import annotations

from semantic.llm.text2query import (
    QueryExecutionResult,
    build_querydef,
    querydef_to_yaml,
)
from semantic.llm.tools.registry import Tool, ToolContext, ToolResult

PARAMETERS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "root_entity": {
            "type": "string",
            "description": "Entity name (case-sensitive) — must match the schema",
        },
        "group_by": {
            "type": "array",
            "description": (
                "Group-by fields for aggregations. Each entry has a "
                "dotted field path and optional time-bucket format."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "format": {
                        "type": "string",
                        "description": "Optional time bucket: YYYY-MM-DD / YYYY-WW / YYYY-MM / YYYY",
                    },
                    "alias": {"type": "string"},
                },
                "required": ["field"],
            },
        },
        "measures": {
            "type": "array",
            "description": "Aggregation expressions. Leave empty for detail (non-aggregate) queries.",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Output column name (alias)",
                    },
                    "field": {
                        "type": "string",
                        "description": (
                            "Field being aggregated. For count, use the "
                            "entity's primary key column — '*' is not supported."
                        ),
                    },
                    "rollup": {
                        "type": "string",
                        "enum": ["count", "sum", "avg", "min", "max", "count_distinct"],
                    },
                },
                "required": ["name", "field", "rollup"],
            },
        },
        "attributes": {
            "type": "array",
            "description": "Output columns for detail queries (when measures is empty).",
            "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "alias": {"type": "string"},
                },
                "required": ["field"],
            },
        },
        "filters": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Expression strings, e.g. \"state == 'merged' && created_at >= '2026-04-01'\".",
        },
        "order_by": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "column": {
                        "type": "string",
                        "description": "Output column name (must match an alias above)",
                    },
                    "direction": {"type": "string", "enum": ["ASC", "DESC"]},
                },
                "required": ["column", "direction"],
            },
        },
        "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
    },
    "required": ["root_entity"],
}


def _handler(args: dict, ctx: ToolContext) -> ToolResult:
    """Build a QueryDef from args, run it via the executor, package the result."""
    if ctx.executor is None:
        return ToolResult(
            payload={"error": "no executor wired into ToolContext"},
            success=False,
        )

    try:
        qd = build_querydef(args, name="text2query")
    except Exception as exc:  # noqa: BLE001 — string is consumed by the LLM
        return ToolResult(
            payload={"error": f"{type(exc).__name__}: {exc}"},
            success=False,
        )

    exec_result: QueryExecutionResult = ctx.executor.execute(qd)

    if not exec_result.success:
        return ToolResult(
            payload={"error": exec_result.error or "execution failed"},
            success=False,
        )

    return ToolResult(
        payload={
            "columns": list(exec_result.columns),
            "rows": [list(r) for r in exec_result.rows],
            "row_count": exec_result.row_count,
        },
        success=True,
        # Structured pieces the orchestrator pulls post-loop. Not seen by the LLM.
        extras={
            "querydef": qd,
            "querydef_yaml": querydef_to_yaml(qd),
            "sql": exec_result.sql,
            "truncated": exec_result.truncated,
        },
    )


def _summarize(result: ToolResult) -> str:
    """One-line trace summary for VG-239 'Show your work'."""
    rows = result.payload.get("row_count", 0)
    cols = result.payload.get("columns", []) or []
    col_preview = ", ".join(cols[:4]) + (" …" if len(cols) > 4 else "")
    return f"{rows} rows · columns: {col_preview}" if col_preview else f"{rows} rows"


BUILD_AND_RUN_QUERY = Tool(
    name="build_and_run_query",
    description=(
        "Construct a query against the semantic layer and execute it. "
        "Returns rows + column names, or an error if validation or "
        "execution fails."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("query_authoring",),
    summarize=_summarize,
)
