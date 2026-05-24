# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore-chat orchestrator — single agentic tool loop.

One LLM tool-calling loop. The model sees five tools and decides the
sequence itself; no hard-coded procedure, no distance thresholds in the
prompt. Terminal tools are ``run_saved_view`` (reuse path — produces a
saved_view ref) and ``present_view`` (build path — produces an inline
view yaml on top of the most recent query result).

Replaces the previous two-phase ``text2query`` → ``text2view`` pipeline
which was biased toward reuse via prescriptive language ("always call
find_artifacts first") + loose distance thresholds (0.5 for views).
Bias now lives only in tool descriptions, which the LLM can weigh
against the actual question.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.services import query_service, view_service
from semantic.feature import FeatureDef
from semantic.llm.provider import LLMClient, LLMResponse, ToolCall, get_default_client
from semantic.llm.schema_context import build_schema_context
from semantic.llm.text2query import QueryExecutionResult, querydef_to_yaml
from semantic.llm.text2view import view_yaml as render_view_yaml
from semantic.llm.tools import ToolCallTrace, ToolContext, ToolRegistry, build_default_registry
from semantic.llm.tools.registry import summarize_tool_result
from semantic.query import QueryDef
from semantic.yaml_adapter import YAMLAdapter

logger = logging.getLogger(__name__)

# Cap rows from any one query before they reach the LLM (text2query
# truncates further when serialising for context, but capping here keeps
# the in-process memory bounded for huge result sets).
ROWS_FROM_EXECUTOR = 1000


# ---------------------------------------------------------------------------
# Engine adapter — QueryExecutor for the existing semantic-layer stack
# ---------------------------------------------------------------------------


# Artifact names must satisfy the schema pattern ^[a-z][a-z0-9_]*$ — no
# leading underscore. These are *transient* names used only for the inline
# validation + execution path; they never get persisted as named queries
# or views unless the user explicitly saves a turn (VG-207 / VG-208).
QUERY_ARTIFACT_NAME = "text2query"
VIEW_ARTIFACT_NAME = "text2view"


@dataclass
class SemanticLayerExecutor:
    """Wrap the semantic-layer query pipeline as a ``QueryExecutor``.

    Routes through the *same* validation + execution path that the
    ``POST /query/_validate`` and ``POST /query/_execute`` endpoints use
    — ``query_service.validate_inline_query`` then
    ``query_service.execute_inline_yaml``. This means every LLM-authored
    query gets the full ontology checks (entity / attribute / relation /
    function-whitelist / format spec) before any SQL runs, and the LLM
    sees structured error paths (``slices[0].field``: …) rather than raw
    backend exceptions.

    Validation or execution failures come back as ``success=False`` — the
    LLM consumes the error string and decides whether to retry.
    """

    model_dir: Path
    rows_per_query: int = ROWS_FROM_EXECUTOR

    def execute(self, query: QueryDef) -> QueryExecutionResult:
        try:
            yaml_str = querydef_to_yaml(query)
        except Exception as exc:  # noqa: BLE001
            return QueryExecutionResult(
                success=False,
                error=f"Could not serialise QueryDef to YAML: {exc}",
            )

        try:
            validation = query_service.validate_inline_query(
                self.model_dir, QUERY_ARTIFACT_NAME, yaml_str,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("validate_inline_query raised: %s", exc)
            return QueryExecutionResult(
                success=False,
                error=f"Ontology validation crashed: {type(exc).__name__}: {exc}",
            )

        if not validation.get("valid", False):
            errs = validation.get("errors") or []
            msg = "; ".join(
                f"{e.get('path') or '<root>'}: {e.get('message') or 'invalid'}"
                for e in errs
            ) or "validation failed"
            return QueryExecutionResult(
                success=False,
                error=f"Ontology validation failed: {msg}",
                sql=validation.get("compiled_sql"),
            )

        try:
            result = query_service.execute_inline_yaml(
                self.model_dir, QUERY_ARTIFACT_NAME, yaml_str,
                limit=self.rows_per_query,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("execute_inline_yaml raised: %s", exc)
            return QueryExecutionResult(
                success=False,
                error=f"{type(exc).__name__}: {exc}",
                sql=validation.get("compiled_sql"),
            )

        return QueryExecutionResult(
            success=True,
            rows=[list(r) for r in result.get("rows", [])],
            columns=list(result.get("columns", [])),
            row_count=result.get("total_row_count", result.get("row_count", 0)),
            truncated=bool(result.get("truncated", False)),
            sql=result.get("sql"),
        )


# ---------------------------------------------------------------------------
# Result shape — what the orchestrator returns to the route handler
# ---------------------------------------------------------------------------


@dataclass
class ChatTurnResult:
    """Combined output of a single chat turn.

    VG-237 reshape: every successful turn produces one of two things —
    a reference to a saved view, or a transient inline view + (optional)
    transient inline query. The UI renders both through the same
    ``ViewContent`` component the explorer uses, so charts and
    drilldowns are uniform across the product.

    Old fields (rows, columns, chart_type, caption, …) are gone; that
    data is encapsulated in the view + the UI fetches it via
    ``executeView`` / ``executeViewInline``.

    Diagnostics-only fields (trace, query_yaml, view_yaml, sql) are
    kept for the "Show your work" tab.
    """

    success: bool
    error: str | None = None
    iterations: int = 0
    # VG-239: tool-use trace.
    trace: list[ToolCallTrace] = None  # type: ignore[assignment]

    # Exactly one populated on success: saved_view (path A) OR
    # inline_view (paths B / C).
    saved_view: dict | None = None      # {name: str, params: dict}
    inline_view: dict | None = None     # {view_yaml, query_yaml?, params}

    # Diagnostics — populated when available, shown in the "Show your work" tab.
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None

    def __post_init__(self) -> None:
        if self.trace is None:
            self.trace = []


# ---------------------------------------------------------------------------
# History conversion — frontend turns → OpenAI-shape messages
# ---------------------------------------------------------------------------


def _history_to_openai(history: list[dict] | None) -> list[dict]:
    """Translate a list of ``{role, content}`` turns into OpenAI messages.

    Only ``role`` and ``content`` are passed through — query / chart
    payloads from prior turns are intentionally dropped so the LLM
    re-derives them from the current user prompt + assistant caption
    context, which is enough for drilldown to work.
    """
    out: list[dict] = []
    for turn in history or []:
        role = turn.get("role")
        content = turn.get("content") or ""
        if role in ("user", "assistant") and content:
            out.append({"role": role, "content": content})
    return out


def _build_semantic_search():
    """Return a configured ``SemanticSearch`` or None if embeddings are off.

    Centralises the graceful-degradation logic: missing API key or CH
    means ``find_artifacts`` returns an empty match list instead of
    crashing the turn.
    """
    try:
        from semantic.llm.embeddings import get_default_provider
        from semantic.llm.embeddings.search import SemanticSearch
        from semantic.llm.embeddings.store import EmbeddingsStore

        provider = get_default_provider()
        if provider is None:
            return None
        store = EmbeddingsStore()
        # Don't ensure_schema() here — that runs at app startup
        # (api/main.py lifespan). If the table doesn't exist, find()
        # will fail loudly and the tool will degrade with a warning,
        # which is the right signal.
        return SemanticSearch(provider=provider, store=store)
    except Exception as exc:  # noqa: BLE001
        logger.info("Semantic search unavailable for this turn: %s", exc)
        return None


# ---------------------------------------------------------------------------
# System prompt — short, tool-focused, no prescriptive procedure
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT_TEMPLATE = """You answer the user's data question for the `{model}` model by calling tools.

Available tools:

- `find_artifacts(query, kind?, top_k?)` — semantic search over the
  model's catalog of saved queries, views, and features. Useful for
  checking whether the user's question matches existing work.

- `run_saved_view(name, params?)` — execute a saved view (terminal).
  Use only when find_artifacts returned a view that truly answers the
  user's question — typically distance ≲ 0.2. Saved views carry their
  own chart spec + drilldown; running one preserves the author's
  end-to-end choices.

- `run_saved_query(name, params?)` — execute a saved query, then call
  `present_view` to pick a chart. Same bar as run_saved_view: only when
  the catalog match really is the same question.

- `build_and_run_query(...)` — author and execute a new query from the
  ENTITY SCHEMA below. Default for novel questions and for near-misses
  in the catalog. Lift naming + measure definitions from any matches
  find_artifacts returned (descriptions render measures as
  `alias=expr(field)`).

- `present_view(chart_type, x_field?, y_field?, caption)` — pick a chart
  for the rows returned by the *most recent* build_and_run_query or
  run_saved_query (terminal). Call exactly once. Pass column names you
  saw in that tool result. Don't call after run_saved_view.

You decide the sequence. A near-miss in the catalog is a different
question — author a new query rather than reusing a saved one that
doesn't quite fit.

When authoring with `build_and_run_query`:
- `root_entity` MUST be one of the ENTITY names below (case-sensitive).
- Field paths use dotted traversal: `author.name`. First segment must
  be a column on the root entity OR a relation name listed on the
  `relations:` line — never guess.
- Aggregations: populate `group_by` AND `measures`. Raw rows: leave
  `measures` empty and use `attributes`.
- For `count` rollup, set `field` to the entity's primary key.
- For `avg` / `sum` / `min` / `max`, `field` MUST be a numeric column.
  Don't aggregate timestamps or strings — find or build a duration
  feature.
- Filters are SQL-ish: `created_at >= '2026-04-01'`, `state == 'merged'`.
- Measure names and group_by aliases are snake_case
  (lowercase letters, digits, underscores, starting with a letter).
- On error, fix the args and retry — don't loop on the same conceptual
  query.

=== MODEL SCHEMA ===

{schema}"""


def build_system_prompt(model_name: str, schema_context: str) -> str:
    return _SYSTEM_PROMPT_TEMPLATE.format(model=model_name, schema=schema_context)


# ---------------------------------------------------------------------------
# Loop state — what the orchestrator tracks across tool calls
# ---------------------------------------------------------------------------


@dataclass
class _QueryState:
    """The most recent successful build/run_saved_query in this turn.

    ``present_view`` consumes whichever of these is current to construct
    the wrapper view yaml. Updated each time a query-producing tool
    succeeds; reset never (a later successful call just overwrites).
    """
    name: str
    yaml: str | None
    columns: list[str]
    sql: str | None
    is_inline: bool                 # True for build_and_run_query (path C),
                                    # False for run_saved_query (path B).


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


def _trace_step(tc: ToolCall, result: Any, registry: ToolRegistry) -> ToolCallTrace:
    """Build one trace entry for the VG-239 'Show your work' tab."""
    tool_def = registry.get(tc.name)
    return ToolCallTrace(
        name=tc.name,
        arguments=tc.arguments,
        success=result.success,
        summary=summarize_tool_result(
            tc.name, result,
            summarize_hook=tool_def.summarize if tool_def else None,
        ),
        payload=dict(result.payload),
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def chat_turn(
    *,
    model_dir: Path,
    message: str,
    history: list[dict] | None = None,
    llm_client: LLMClient | None = None,
    executor: SemanticLayerExecutor | None = None,
    registry: ToolRegistry | None = None,
    max_iter: int = 6,
    rows_to_llm: int = 40,
    llm_model: str | None = None,
) -> ChatTurnResult:
    """One agentic tool-calling loop.

    Tools available: find_artifacts, run_saved_view, run_saved_query,
    build_and_run_query, present_view. The LLM picks the sequence.

    Terminal paths:
      - run_saved_view succeeds → ChatTurnResult.saved_view (path A)
      - present_view succeeds after build_and_run_query (path C) or
        run_saved_query (path B) → ChatTurnResult.inline_view

    Everything else (find_artifacts, failed tool calls, LLM thinking
    aloud) keeps the loop running until ``max_iter`` or until the LLM
    stops calling tools.
    """
    model_name = model_dir.name
    client = llm_client or get_default_client()
    exec_ = executor or SemanticLayerExecutor(model_dir=model_dir)
    reg = registry or build_default_registry()
    # Wire the semantic-search adapter for find_artifacts. Missing
    # OPENAI_API_KEY or unavailable ClickHouse → search is None and
    # find_artifacts gracefully returns an empty match list.
    search = _build_semantic_search()
    ctx = ToolContext(
        model_id=model_name, model_dir=model_dir,
        executor=exec_, search=search,
    )

    entities = YAMLAdapter.load_entities(model_dir / "ontology")
    features_by_entity: dict[str, list[FeatureDef]] = {}
    for fd in YAMLAdapter.load_features(model_dir / "features"):
        features_by_entity.setdefault(fd.entity_type, []).append(fd)
    schema = build_schema_context(model_name, entities, features_by_entity)

    messages: list[dict] = [
        {"role": "system", "content": build_system_prompt(model_name, schema)},
        *_history_to_openai(history),
        {"role": "user", "content": message},
    ]
    # The unified tool set — all under the ``query_authoring`` tag
    # (present_view picked up the tag in this PR specifically to join
    # the loop).
    openai_tools = reg.to_openai_tools(tags=("query_authoring",))

    trace: list[ToolCallTrace] = []
    last_query: _QueryState | None = None
    last_error: str | None = None

    for iteration in range(max_iter):
        resp = client.complete(messages=messages, tools=openai_tools, model=llm_model)
        messages.append(_assistant_msg(resp))

        if not resp.tool_calls:
            # LLM gave up or thinks it's done without calling a terminal
            # tool. Surface whatever it said as the error.
            return ChatTurnResult(
                success=False,
                error=resp.content or "LLM stopped without producing a view",
                iterations=iteration + 1,
                trace=trace,
            )

        for tc in resp.tool_calls:
            try:
                result = reg.dispatch(tc.name, tc.arguments, ctx)
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
            trace.append(_trace_step(tc, result, reg))

            if not result.success:
                last_error = result.payload.get("error") or "tool reported failure"
                continue

            # State update: query-producing tools refresh ``last_query``
            # so a subsequent present_view has the right context.
            if tc.name == "build_and_run_query":
                qd: QueryDef | None = result.extras.get("querydef")
                last_query = _QueryState(
                    name=(qd.name if qd else QUERY_ARTIFACT_NAME),
                    yaml=result.extras.get("querydef_yaml"),
                    columns=list(result.payload.get("columns") or []),
                    sql=result.extras.get("sql"),
                    is_inline=True,
                )
                continue

            if tc.name == "run_saved_query":
                last_query = _QueryState(
                    name=result.extras.get("saved_query_name")
                        or result.payload.get("query_name")
                        or QUERY_ARTIFACT_NAME,
                    yaml=result.extras.get("querydef_yaml"),
                    columns=list(result.payload.get("columns") or []),
                    sql=result.extras.get("sql"),
                    is_inline=False,
                )
                continue

            # Terminal: run_saved_view → saved_view ref (path A).
            if tc.name == "run_saved_view":
                return ChatTurnResult(
                    success=True,
                    iterations=iteration + 1,
                    trace=trace,
                    saved_view={
                        "name": result.extras.get("saved_view_name")
                            or result.payload.get("view_name"),
                        "params": tc.arguments.get("params") or {},
                    },
                    query_yaml=result.extras.get("querydef_yaml"),
                    view_yaml=result.extras.get("view_yaml"),
                    sql=result.extras.get("sql"),
                )

            # Terminal: present_view → inline_view yaml on top of the
            # most recent query (paths B / C).
            if tc.name == "present_view":
                if last_query is None:
                    # The LLM jumped to present_view without first
                    # running a query. Treat as an error and let the
                    # loop continue so it can retry the right way.
                    err = "present_view called before any query produced data"
                    messages.append({
                        "role": "tool", "tool_call_id": tc.id,
                        "content": json.dumps({"error": err}),
                    })
                    last_error = err
                    continue
                return _build_inline_view_turn(
                    model_dir=model_dir,
                    iteration=iteration + 1,
                    trace=trace,
                    last_query=last_query,
                    present_payload=result.payload,
                )

    # Loop exhausted without a terminal tool.
    return ChatTurnResult(
        success=False,
        error=last_error or "max iterations reached without producing a view",
        iterations=max_iter,
        trace=trace,
    )


def _build_inline_view_turn(
    *,
    model_dir: Path,
    iteration: int,
    trace: list[ToolCallTrace],
    last_query: _QueryState,
    present_payload: dict,
) -> ChatTurnResult:
    """Package a present_view result + the prior query state as inline_view.

    Builds the wrapper view yaml, runs it past validate_inline_view (best
    effort — failure just logs), and returns a ChatTurnResult shaped like
    the historic paths B/C output.
    """
    wrapper_view_yaml = render_view_yaml(
        name=VIEW_ARTIFACT_NAME,
        query_name=last_query.name,
        chart_type=present_payload["chart_type"],
        x_field=present_payload.get("x_field"),
        y_field=present_payload.get("y_field"),
        color_field=present_payload.get("color_field"),
        caption=present_payload.get("caption", ""),
        columns=last_query.columns,
    )

    # Best-effort validation. Mirrors the historic flow — broken views
    # still ship; the UI degrades gracefully.
    try:
        view_validation = view_service.validate_inline_view(
            model_dir, VIEW_ARTIFACT_NAME, wrapper_view_yaml,
            known_query_columns={last_query.name: last_query.columns},
        )
        if not view_validation.get("valid", False):
            errs = view_validation.get("errors") or []
            msg = "; ".join(
                f"{e.get('path') or '<root>'}: {e.get('message') or 'invalid'}"
                for e in errs
            ) or "view validation failed"
            logger.info("View YAML rejected by validator: %s", msg)
    except Exception as exc:  # noqa: BLE001
        logger.warning("validate_inline_view raised: %s", exc)

    return ChatTurnResult(
        success=True,
        iterations=iteration,
        trace=trace,
        inline_view=_inline_view_payload(
            view_yaml=wrapper_view_yaml,
            query_yaml=last_query.yaml,
            is_query_inline=last_query.is_inline,
        ),
        query_yaml=last_query.yaml,
        view_yaml=wrapper_view_yaml,
        sql=last_query.sql,
    )


def _inline_view_payload(
    *, view_yaml: str, query_yaml: str | None, is_query_inline: bool,
) -> dict:
    """Build the ``inline_view`` payload for the chat response.

    ``is_query_inline``: True for path C (we just authored the query
    ourselves; the inline-view endpoint must execute it transient). False
    for path B (query is already saved in api.db; the inline view's
    ``query:`` reference resolves to that saved query and we send
    ``query_yaml=None``).
    """
    return {
        "view_yaml": view_yaml,
        "query_yaml": query_yaml if is_query_inline else None,
        "params": {},
    }


