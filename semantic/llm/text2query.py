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
from semantic.llm.tools.registry import (
    ToolCallTrace,
    ToolContext,
    ToolRegistry,
    summarize_tool_result,
)
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
    # VG-239: structured trace of every tool the LLM called in this turn.
    # Surfaced by the orchestrator + UI so users (and us) can audit what
    # the model actually did — esp. helpful when find_artifacts is in the loop.
    trace: list[ToolCallTrace] = field(default_factory=list)
    messages: list[dict] = field(default_factory=list)
    iterations: int = 0
    # VG-234: when the successful tool was ``run_saved_view``, this carries
    # the saved view's normalised chart spec
    # ({chart_type, x_field, y_field, color_field, drilldown}) + the view's
    # raw YAML. The orchestrator uses it INSTEAD of calling text2view's
    # chart picker — preserves the saved view's chosen visualization
    # rather than re-deriving it on every turn.
    view_spec: dict | None = None
    view_yaml: str | None = None
    saved_view_name: str | None = None
    # When the successful tool was ``run_saved_query``, the saved query's
    # actual name. The orchestrator threads this into the wrapper view
    # YAML so the inline-view endpoint can look up the right saved query
    # (otherwise it falls back to the "text2query" placeholder and 404s).
    saved_query_name: str | None = None


# ---------------------------------------------------------------------------
# Tool schema — moved into ``semantic/llm/tools/build_and_run_query.py``.
# ``text2query_yaml`` consumes the tool from a ``ToolRegistry`` now; the
# rest of this module is the QueryDef + YAML helpers that the tool's
# handler imports.
# ---------------------------------------------------------------------------


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
for the `{model}` model.

Procedure for every question:

1. **Always call `find_artifacts` first** with a short summary of the
   user's question. The model's catalog has queries, views, and features
   that humans have already validated — their measure names, field
   paths, and relations are far more reliable than anything you'd guess
   from the entity schema alone. Anything with distance < 0.4 is usually
   directly relevant.
2. Inspect the matches. **Match the tool to the artifact kind**:
   - `kind='view'` with distance < 0.5 → call `run_saved_view`. Views
     carry their own chart spec + drilldown — this preserves the
     author's end-to-end choices instead of re-deriving them.
   - `kind='query'` with distance < 0.4 → call `run_saved_query`. Same
     verbatim-invocation logic for the underlying data shape.
   - Pass `params` to either when the artifact declares parameters
     (visible in the description if required).
3. **Only if no saved view or query is a good enough match**, call
   `build_and_run_query`. When you do, lift patterns from the matches:
   catalog descriptions render measures as `alias=expr(field)` — e.g.
   `avg_clt_prd=avg(change_lead_time_prd)`. The LLM-side `name` is the
   **alias** (`avg_clt_prd`); the `field` is the **inner field**
   (`change_lead_time_prd`), never the alias itself.

If `find_artifacts` returns no matches (or only weak ones with distance
> 0.6), fall back to authoring from the ENTITY SCHEMA below.

Rules for `build_and_run_query`:

- `root_entity` MUST be one of the ENTITY names below (case-sensitive).
- Field paths use dotted traversal: `author.name`,
  `belongs_to.product.name`. The **first segment** must be either a
  column on the root entity OR a relation name **listed on the
  `relations:` line** — never guess a relation name from English. If
  PullRequest's relations line says `belongs_to (N→1 Repository)`, use
  `belongs_to.<...>`, NOT `repository.<...>`.
- For aggregations, populate `group_by` AND `measures`. For raw rows,
  leave `measures` empty and use `attributes`.
- For `count` rollup, set `field` to the entity's primary key (in the
  `identity:` line). Never use `*`.
- **For `avg` / `sum` / `min` / `max`, the `field` MUST be a numeric
  column.** Don't average / sum timestamps or strings — use a duration
  feature (e.g. `change_lead_time_*`, `*_days`, `*_minutes`) that you
  found via `find_artifacts`, or call `find_artifacts(kind='feature')`
  to discover one.
- `filters` are SQL-ish expressions: `created_at >= '2026-04-01'`,
  `state == 'merged'`.
- Measure NAMES (output column aliases) and group_by ALIASES must be
  snake_case: lowercase letters, digits, underscores, starting with a
  letter. Bad: "PR Count", "byAuthor". Good: "pr_count", "by_author".
- On error, fix the args and retry. Don't retry the same conceptual
  query more than twice — adjust the approach.

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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def text2query_yaml(
    *,
    prompt: str,
    model_name: str,
    schema_context: str,
    registry: ToolRegistry,
    ctx: ToolContext,
    llm_client: LLMClient,
    history: list[dict] | None = None,
    tool_tags: tuple[str, ...] = ("query_authoring",),
    success_tool_names: tuple[str, ...] = (
        "build_and_run_query",
        "run_saved_query",
        "run_saved_view",
    ),
    max_iter: int = 5,
    rows_to_llm: int = 40,
    llm_model: str | None = None,
) -> Text2QueryResult:
    """Convert ``prompt`` into a validated, executed ``QueryDef``.

    Returns as soon as any of ``success_tool_names`` succeeds — both the
    "author from scratch" path (``build_and_run_query``) and the "invoke a
    saved query" path (``run_saved_query``) produce the same downstream
    shape (rows + columns + extras), so either terminates the loop. The
    LLM may retry up to ``max_iter`` times to recover from validation /
    execution errors.

    Tools are pulled from the ``registry`` filtered by ``tool_tags``
    (default: just the query-authoring set). The same registry can host
    other tools the LLM might compose with — e.g. ``find_artifacts``
    in VG-232 — without changing this function's signature.

    ``ctx`` carries the per-call dependencies tools need (executor,
    semantic search, etc.); see ``ToolContext``. Tests pass a fake
    executor here exactly the same way production passes the real one.
    """
    system = build_system_prompt(model_name, schema_context)
    messages: list[dict] = [
        {"role": "system", "content": system},
        *(history or []),
        {"role": "user", "content": prompt},
    ]

    openai_tools = registry.to_openai_tools(tags=tool_tags)

    tool_calls_seen: list[ToolCall] = []
    trace: list[ToolCallTrace] = []
    last_error: str | None = None

    for iteration in range(max_iter):
        resp = llm_client.complete(
            messages=messages, tools=openai_tools, model=llm_model,
        )
        messages.append(_assistant_msg(resp))

        if not resp.tool_calls:
            # LLM responded without calling any tool — either done or gave up.
            break

        for tc in resp.tool_calls:
            tool_calls_seen.append(tc)
            try:
                result = registry.dispatch(tc.name, tc.arguments, ctx)
            except KeyError:
                messages.append({
                    "role": "tool", "tool_call_id": tc.id,
                    "content": json.dumps({"error": f"unknown tool {tc.name!r}"}),
                })
                trace.append(ToolCallTrace(
                    name=tc.name, arguments=tc.arguments,
                    success=False, summary=f"unknown tool {tc.name!r}",
                ))
                continue

            messages.append({
                "role": "tool", "tool_call_id": tc.id,
                "content": result.to_tool_message_content(max_rows=rows_to_llm),
            })
            # Capture this step in the trace.
            tool_def = registry.get(tc.name)
            trace.append(ToolCallTrace(
                name=tc.name, arguments=tc.arguments,
                success=result.success,
                summary=summarize_tool_result(
                    tc.name, result,
                    summarize_hook=tool_def.summarize if tool_def else None,
                ),
                payload=dict(result.payload),
            ))

            if result.success and tc.name in success_tool_names:
                # Pull the orchestrator-only pieces out of extras.
                qd = result.extras.get("querydef")
                qd_yaml = result.extras.get("querydef_yaml")
                sql = result.extras.get("sql")
                rows = result.payload.get("rows", [])
                columns = result.payload.get("columns", [])
                row_count = result.payload.get("row_count", len(rows))
                # When the LLM invoked a saved view, carry its chart spec
                # + YAML so the orchestrator can skip text2view's chart
                # picker and reuse the author's choices verbatim.
                view_spec = result.extras.get("chart_spec") if tc.name == "run_saved_view" else None
                view_yaml = result.extras.get("view_yaml") if tc.name == "run_saved_view" else None
                saved_view_name = (
                    result.extras.get("saved_view_name")
                    if tc.name == "run_saved_view" else None
                )
                saved_query_name = (
                    result.extras.get("saved_query_name")
                    if tc.name == "run_saved_query" else None
                )
                return Text2QueryResult(
                    success=True,
                    yaml=qd_yaml,
                    querydef=qd,
                    rows=rows[:rows_to_llm],
                    columns=list(columns),
                    row_count=row_count,
                    truncated=bool(result.extras.get("truncated"))
                              or row_count > rows_to_llm,
                    sql=sql,
                    tool_calls=tool_calls_seen,
                    trace=trace,
                    messages=messages,
                    iterations=iteration + 1,
                    view_spec=view_spec,
                    view_yaml=view_yaml,
                    saved_view_name=saved_view_name,
                    saved_query_name=saved_query_name,
                )

            if not result.success:
                # Surface the most recent error to the caller in case
                # max_iter is exhausted.
                last_error = result.payload.get("error") or "tool reported failure"

    return Text2QueryResult(
        success=False,
        error=last_error or "no tool calls produced a successful query",
        tool_calls=tool_calls_seen,
        trace=trace,
        messages=messages,
        iterations=max_iter,
    )
