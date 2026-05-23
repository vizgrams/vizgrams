# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""run_saved_query — invoke an existing named query (Epic 20 VG-233).

The "proper" reuse path for the chat: when ``find_artifacts`` returns a
saved query that matches the user's intent, the LLM should run it
verbatim rather than re-author a copy. Re-authoring is brittle (the LLM
has to guess the right measure / feature / relation names) and
loses any tuning the human author already put into the query.

Handler delegates to ``query_service.execute_query`` — the same path
the existing UI uses when a user runs a saved query — so result shape
and parameter handling stay consistent.
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
                "Exact name of the saved query to invoke (from find_artifacts "
                "results, kind='query'). Case-sensitive."
            ),
        },
        "params": {
            "type": "object",
            "description": (
                "Optional parameter values. Pass when the saved query "
                "declares parameters; values are strings (e.g. "
                "{'team_name': 'Lovelace', 'lookback_weeks': '12'})."
            ),
            "additionalProperties": {"type": "string"},
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
            "description": "Row cap for the result set. Defaults to 1000.",
        },
    },
    "required": ["name"],
}


def _handler(args: dict, ctx: ToolContext) -> ToolResult:
    """Look the query up by name; execute via query_service; package the result."""
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

    # Imported lazily to keep this module's top-level cheap and to avoid
    # a chat tool transitively pulling FastAPI deps at import time.
    from api.services import query_service

    try:
        result = query_service.execute_query(
            ctx.model_dir, name, limit=limit, params=params or None,
        )
    except KeyError:
        return ToolResult(
            payload={"error": f"saved query {name!r} not found"},
            success=False,
        )
    except Exception as exc:  # noqa: BLE001 — error string goes to the LLM
        logger.warning("run_saved_query(%s) failed: %s", name, exc)
        return ToolResult(
            payload={"error": f"{type(exc).__name__}: {exc}"},
            success=False,
        )

    rows = result.get("rows", [])
    columns = result.get("columns", []) or []
    row_count = result.get("total_row_count", result.get("row_count", len(rows)))

    # Surface the saved YAML so the orchestrator can show provenance
    # ("from query: dora_clt_trend") and the UI's source-viewer tabs work.
    raw_yaml: str | None = None
    try:
        detail = query_service.get_query(ctx.model_dir, name)
        raw_yaml = detail.get("raw_yaml")
    except Exception:  # noqa: BLE001
        raw_yaml = None

    return ToolResult(
        payload={
            "columns": list(columns),
            "rows": [list(r) for r in rows],
            "row_count": row_count,
            "query_name": name,
        },
        success=True,
        extras={
            "querydef_yaml": raw_yaml,
            "sql": result.get("sql"),
            "truncated": bool(result.get("truncated", False)),
            "saved_query_name": name,
        },
    )


def _summarize(result: ToolResult) -> str:
    """One-line trace summary for VG-239 'Show your work'."""
    p = result.payload
    rows = p.get("row_count", 0)
    cols = p.get("columns", []) or []
    col_preview = ", ".join(cols[:4]) + (" …" if len(cols) > 4 else "")
    name = p.get("query_name") or "?"
    if col_preview:
        return f"{name}: {rows} rows · columns: {col_preview}"
    return f"{name}: {rows} rows"


RUN_SAVED_QUERY = Tool(
    name="run_saved_query",
    description=(
        "Invoke an existing saved query by name. Use this when "
        "find_artifacts returns a query whose description matches the "
        "user's intent — running the saved query is far more reliable "
        "than re-authoring a similar one with build_and_run_query."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("query_authoring", "reuse"),
    summarize=_summarize,
)
