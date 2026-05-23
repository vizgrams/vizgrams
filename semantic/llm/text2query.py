# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""text2query — natural language → ``QueryDef`` + execution.

Pure function (modulo injected ``llm_client`` and ``executor``). The LLM
sees a tool-use loop with one tool, ``build_and_run_query``; the loop
returns as soon as one tool call succeeds. Validation errors from the
executor are fed back to the LLM as tool-result content so it can retry
with corrected arguments — typical convergence is 1-2 iterations.

Decoupled from the FastAPI / DB layers via two protocols:

  * ``LLMClient``       — the model that authors the query
  * ``QueryExecutor``   — runs a built ``QueryDef``, returns rows or error

This means ``text2query_yaml`` is unit-testable end-to-end with fake
implementations of both protocols (see ``tests/llm/conftest.py``). The
production orchestrator (``api/services/explore_chat.py``, VG-205) wires
in the real implementations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import yaml

from semantic.llm.provider import LLMClient, LLMResponse, ToolCall
from semantic.query import (
    PaginationDef,
    QueryAttribute,
    QueryDef,
    QueryMetric,
    SliceDef,
)

# ---------------------------------------------------------------------------
# Executor protocol — production wires the real engine; tests use a fake
# ---------------------------------------------------------------------------


@dataclass
class QueryExecutionResult:
    """The outcome of running one ``QueryDef`` against the data layer."""

    success: bool
    rows: list[list] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    sql: str | None = None
    error: str | None = None


@runtime_checkable
class QueryExecutor(Protocol):
    """Run a built ``QueryDef`` and return rows or an error.

    Implementations are responsible for compiling the QueryDef to SQL,
    executing against the right DB backend, and capping the row count
    they return to keep LLM context manageable.
    """

    def execute(self, query: QueryDef) -> QueryExecutionResult: ...


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------


@dataclass
class Text2QueryResult:
    """Output of ``text2query_yaml``.

    ``success`` is True iff at least one tool call produced rows. The
    other fields are populated from the *last successful* execution; on
    failure they may be partially set (e.g. ``error`` populated, ``sql``
    if the failure happened during execution rather than validation).
    """

    success: bool
    yaml: str | None = None
    querydef: QueryDef | None = None
    rows: list[list] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    sql: str | None = None
    error: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    messages: list[dict] = field(default_factory=list)
    iterations: int = 0


# ---------------------------------------------------------------------------
# Tool schema — what the LLM may call
# ---------------------------------------------------------------------------


BUILD_AND_RUN_QUERY_TOOL = {
    "type": "function",
    "function": {
        "name": "build_and_run_query",
        "description": (
            "Construct a query against the semantic layer and execute it. "
            "Returns rows + column names, or an error if validation or "
            "execution fails."
        ),
        "parameters": {
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
                                "enum": [
                                    "count",
                                    "sum",
                                    "avg",
                                    "min",
                                    "max",
                                    "count_distinct",
                                ],
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
                            "direction": {
                                "type": "string",
                                "enum": ["ASC", "DESC"],
                            },
                        },
                        "required": ["column", "direction"],
                    },
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            },
            "required": ["root_entity"],
        },
    },
}


# ---------------------------------------------------------------------------
# Tool args → QueryDef
# ---------------------------------------------------------------------------


_IDENTIFIER_RE = __import__("re").compile(r"^[a-z][a-z0-9_]*$")


def build_querydef(args: dict, name: str = "text2query") -> QueryDef:
    """Convert tool arguments to a ``QueryDef`` ready for execution.

    Raises ``ValueError`` if any measure name or group-by alias isn't a
    valid snake_case identifier — those become SQL aliases and the engine
    doesn't quote them, so spaces / camelCase break compilation. The LLM
    consumes the error and retries with corrected names.
    """
    root = args.get("root_entity", "")
    measures_in = args.get("measures") or []
    group_by_in = args.get("group_by") or []

    for m in measures_in:
        nm = m.get("name", "")
        if not _IDENTIFIER_RE.match(nm):
            raise ValueError(
                f"measure name {nm!r} is not a valid SQL identifier — "
                f"use snake_case (lowercase letters, digits, underscores, "
                f"must start with a letter). e.g. 'pr_count' not 'PR Count'."
            )
    for g in group_by_in:
        alias = g.get("alias")
        if alias and not _IDENTIFIER_RE.match(alias):
            raise ValueError(
                f"group_by alias {alias!r} is not a valid SQL identifier — "
                f"use snake_case. e.g. 'month' not 'Month Bucket'."
            )

    slices = [
        SliceDef(
            field=g["field"],
            alias=g.get("alias"),
            format_pattern=g.get("format"),
        )
        for g in group_by_in
    ]

    metrics: dict = {}
    for m in measures_in:
        metrics[m["name"]] = QueryMetric(field=m["field"], rollup=m["rollup"])

    order_by = [(o["column"], o["direction"].upper()) for o in args.get("order_by") or []]

    attrs: list[QueryAttribute] = []
    if not metrics:
        for a in args.get("attributes") or []:
            field = a["field"]
            attrs.append(QueryAttribute(parts=field.split("."), label=a.get("alias")))

    limit = args.get("limit")
    pag = PaginationDef(page=1, page_size=limit) if limit else PaginationDef()

    return QueryDef(
        name=name,
        entity=root,
        detail=not metrics,
        attributes=attrs,
        filters=list(args.get("filters") or []),
        slices=slices,
        metrics=metrics,
        order_by=order_by,
        pagination=pag,
    )


# ---------------------------------------------------------------------------
# QueryDef → YAML — for storage / display
# ---------------------------------------------------------------------------


def querydef_to_yaml_dict(q: QueryDef) -> dict:
    """Serialise a ``QueryDef`` to a dict suitable for ``yaml.safe_dump``.

    Mirrors the YAML shape consumed by ``parse_query_dict``.
    """
    out: dict = {"name": q.name, "root": q.entity}
    if q.attributes:
        out["attributes"] = [
            a.label or a.raw_field if a.label else a.raw_field for a in q.attributes
        ]
    if q.filters:
        out["where"] = list(q.filters)
    if q.slices:
        slices = []
        for s in q.slices:
            if s.format_pattern:
                slices.append({s.alias or s.field: f"format_time({s.field}, '{s.format_pattern}')"})
            elif s.alias:
                slices.append({s.alias: s.field})
            else:
                slices.append(s.field)
        out["attributes"] = (out.get("attributes") or []) + slices
    if q.metrics:
        measures = []
        for name, m in q.metrics.items():
            if isinstance(m, QueryMetric):
                measures.append({name: {"expr": f"{m.rollup}({m.field})"}})
        out["measures"] = measures
    if q.order_by:
        out["order"] = [{col: direction.lower()} for col, direction in q.order_by]
    return out


def querydef_to_yaml(q: QueryDef) -> str:
    return yaml.safe_dump(querydef_to_yaml_dict(q), sort_keys=False)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT_TEMPLATE = """You convert natural-language data questions into semantic-layer queries
for the `{model}` model. Call `build_and_run_query` to author and execute a
query. Stop calling tools when you have one successful result.

Rules:
- `root_entity` MUST be one of the ENTITY names below (case-sensitive).
- Field paths use dotted traversal: `author.name`,
  `repository.product.name`. First segment is either a column on the root
  entity or a relation name.
- For aggregations, populate `group_by` AND `measures`. For raw rows, leave
  `measures` empty and use `attributes`.
- For `count` rollup, set `field` to the entity's primary key (in the
  `identity:` line). Never use `*`.
- `filters` are SQL-ish expressions: `created_at >= '2026-04-01'`,
  `state == 'merged'`.
- Measure NAMES (the output column aliases) and group_by ALIASES must be
  snake_case: lowercase letters, digits, underscores only, starting with a
  letter. Bad: "PR Count", "byAuthor". Good: "pr_count", "by_author".
- On error, fix the args and retry. Don't retry the same conceptual query
  more than twice — adjust the approach.

=== MODEL SCHEMA ===

{schema}"""


def build_system_prompt(model_name: str, schema_context: str) -> str:
    return _SYSTEM_PROMPT_TEMPLATE.format(model=model_name, schema=schema_context)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _assistant_msg(resp: LLMResponse) -> dict:
    """Render an LLMResponse as an OpenAI-shape assistant message."""
    msg: dict = {"role": "assistant", "content": resp.content}
    if resp.tool_calls:
        msg["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.name, "arguments": json.dumps(tc.arguments)},
            }
            for tc in resp.tool_calls
        ]
    return msg


def _serialise_exec_result(r: QueryExecutionResult, *, max_rows: int) -> dict:
    """Render a QueryExecutionResult as the tool-result payload fed to the LLM.

    Truncates rows so wide / long results don't blow the context window.
    """
    if not r.success:
        return {"error": r.error or "execution failed"}
    rows = r.rows[:max_rows]
    payload: dict = {
        "columns": list(r.columns),
        "rows": [list(row) for row in rows],
        "row_count": r.row_count,
    }
    if r.truncated or r.row_count > max_rows:
        payload["truncated"] = True
        payload["rows_shown"] = len(rows)
    return payload


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def text2query_yaml(
    *,
    prompt: str,
    model_name: str,
    schema_context: str,
    executor: QueryExecutor,
    llm_client: LLMClient,
    history: list[dict] | None = None,
    max_iter: int = 5,
    rows_to_llm: int = 40,
    llm_model: str | None = None,
    query_name: str = "text2query",
) -> Text2QueryResult:
    """Convert ``prompt`` into a validated, executed ``QueryDef``.

    Returns as soon as one ``build_and_run_query`` tool call succeeds —
    refinement loops are out of scope for v1. The LLM may retry up to
    ``max_iter`` times to recover from validation / execution errors.

    ``history`` is a list of OpenAI-shape prior messages (e.g. from a
    previous turn in a multi-turn chat). The orchestrator owns the
    conversation memory; this function is single-turn at heart.

    ``executor`` is the seam that lets tests swap in a fake; production
    wires the real semantic-layer engine.
    """
    system = build_system_prompt(model_name, schema_context)
    messages: list[dict] = [
        {"role": "system", "content": system},
        *(history or []),
        {"role": "user", "content": prompt},
    ]

    tool_calls_seen: list[ToolCall] = []
    last_error: str | None = None
    last_querydef: QueryDef | None = None
    last_exec: QueryExecutionResult | None = None

    for iteration in range(max_iter):
        resp = llm_client.complete(
            messages=messages,
            tools=[BUILD_AND_RUN_QUERY_TOOL],
            model=llm_model,
        )
        messages.append(_assistant_msg(resp))

        if not resp.tool_calls:
            # LLM responded without calling the tool — either it's done or it
            # gave up. Either way we have nothing better to return.
            break

        any_success = False
        for tc in resp.tool_calls:
            tool_calls_seen.append(tc)
            if tc.name != "build_and_run_query":
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps({"error": f"unknown tool {tc.name!r}"}),
                })
                continue

            try:
                qd = build_querydef(tc.arguments, name=query_name)
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps({"error": last_error}),
                })
                continue

            exec_result = executor.execute(qd)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(
                    _serialise_exec_result(exec_result, max_rows=rows_to_llm),
                    default=str,
                ),
            })

            if exec_result.success:
                return Text2QueryResult(
                    success=True,
                    yaml=querydef_to_yaml(qd),
                    querydef=qd,
                    rows=exec_result.rows[:rows_to_llm],
                    columns=list(exec_result.columns),
                    row_count=exec_result.row_count,
                    truncated=exec_result.truncated or exec_result.row_count > rows_to_llm,
                    sql=exec_result.sql,
                    tool_calls=tool_calls_seen,
                    messages=messages,
                    iterations=iteration + 1,
                )

            last_error = exec_result.error
            last_querydef = qd
            last_exec = exec_result
            any_success = False  # noqa: F841 — kept for readability

    return Text2QueryResult(
        success=False,
        querydef=last_querydef,
        sql=last_exec.sql if last_exec else None,
        error=last_error or "no tool calls produced a successful query",
        tool_calls=tool_calls_seen,
        messages=messages,
        iterations=max_iter,
    )
