# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore-chat orchestrator (Epic 19 VG-205).

Chains the two ``semantic/llm/text2X`` capabilities into one assistant
turn: text2query authors and executes a query, then text2view picks a
chart spec and writes a caption. The orchestrator owns conversation
state (passed in by the caller — ephemeral in v1) and the
``SemanticLayerExecutor`` that adapts the existing engine to the
``QueryExecutor`` protocol.

Plain Python — no LangGraph. If/when we add a planner agent or
multi-agent coordination, the orchestrator gets richer; the text2X
modules don't change.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.services import query_service, view_service
from semantic.feature import FeatureDef
from semantic.llm.provider import LLMClient, get_default_client
from semantic.llm.schema_context import build_schema_context
from semantic.llm.text2query import (
    QueryExecutionResult,
    Text2QueryResult,
    querydef_to_yaml,
    text2query_yaml,
)
from semantic.llm.text2view import Text2ViewResult, text2view_yaml
from semantic.llm.tools import ToolCallTrace, ToolContext, ToolRegistry, build_default_registry
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

    ``content`` is the user-facing caption from text2view; ``error`` is
    populated when the turn failed at any step.
    """

    success: bool
    content: str = ""
    error: str | None = None
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None
    columns: list[str] = None  # type: ignore[assignment]
    rows: list[list] = None  # type: ignore[assignment]
    row_count: int = 0
    truncated: bool = False
    chart_type: str | None = None
    x_field: str | None = None
    y_field: str | None = None
    color_field: str | None = None
    iterations: int = 0
    # VG-239: tool-use trace across text2query + text2view for the
    # "Show your work" UI tab. Ordered; first entry = first tool call.
    trace: list[ToolCallTrace] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.columns is None:
            self.columns = []
        if self.rows is None:
            self.rows = []
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
    text2query_kwargs: dict[str, Any] | None = None,
    text2view_kwargs: dict[str, Any] | None = None,
) -> ChatTurnResult:
    """Run one assistant turn: prompt → query → chart spec + caption.

    ``llm_client`` and ``executor`` are injectable for testability and to
    let callers swap providers per request (e.g. a per-customer model
    routing layer). Defaults: ``get_default_client()`` (reads env) and
    a ``SemanticLayerExecutor`` for ``model_dir``.
    """
    model_name = model_dir.name
    client = llm_client or get_default_client()
    exec_ = executor or SemanticLayerExecutor(model_dir=model_dir)
    reg = registry or build_default_registry()
    # Wire the semantic-search adapter for the find_artifacts tool. If
    # embeddings aren't configured (no OPENAI_API_KEY, or ClickHouse
    # unavailable), search stays None and find_artifacts degrades to an
    # empty match list — chat keeps working.
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

    q_result: Text2QueryResult = text2query_yaml(
        prompt=message,
        model_name=model_name,
        schema_context=schema,
        registry=reg,
        ctx=ctx,
        llm_client=client,
        history=_history_to_openai(history),
        **(text2query_kwargs or {}),
    )

    if not q_result.success:
        return ChatTurnResult(
            success=False,
            error=q_result.error or "query authoring failed",
            iterations=q_result.iterations,
            trace=list(q_result.trace),
        )

    v_result: Text2ViewResult = text2view_yaml(
        columns=q_result.columns,
        rows=q_result.rows,
        registry=reg,
        user_intent=message,
        llm_client=client,
        query_name=QUERY_ARTIFACT_NAME,
        view_name=VIEW_ARTIFACT_NAME,
        **(text2view_kwargs or {}),
    )

    if not v_result.success:
        # Query worked but chart picker failed — still return the data
        # with a table fallback so the user sees something useful.
        return ChatTurnResult(
            success=True,
            content=f"Here are the results. (Chart selection failed: {v_result.error})",
            query_yaml=q_result.yaml,
            sql=q_result.sql,
            columns=q_result.columns,
            rows=q_result.rows,
            row_count=q_result.row_count,
            truncated=q_result.truncated,
            chart_type="table",
            iterations=q_result.iterations,
            trace=list(q_result.trace) + list(v_result.trace),
        )

    # Run the generated view YAML through the same validator the
    # POST /view route uses. We pass `known_query_columns` so axes-vs-
    # columns checks work even though the underlying query isn't saved.
    view_yaml_to_return: str | None = v_result.yaml
    if v_result.yaml:
        try:
            view_validation = view_service.validate_inline_view(
                model_dir, VIEW_ARTIFACT_NAME, v_result.yaml,
                known_query_columns={QUERY_ARTIFACT_NAME: q_result.columns},
            )
            if not view_validation.get("valid", False):
                errs = view_validation.get("errors") or []
                msg = "; ".join(
                    f"{e.get('path') or '<root>'}: {e.get('message') or 'invalid'}"
                    for e in errs
                ) or "view validation failed"
                logger.info("View YAML rejected by validator: %s", msg)
                view_yaml_to_return = None  # drop invalid YAML; chart spec still usable
        except Exception as exc:  # noqa: BLE001
            logger.warning("validate_inline_view raised: %s", exc)
            view_yaml_to_return = None

    return ChatTurnResult(
        success=True,
        content=v_result.caption,
        query_yaml=q_result.yaml,
        view_yaml=view_yaml_to_return,
        sql=q_result.sql,
        columns=q_result.columns,
        rows=q_result.rows,
        row_count=q_result.row_count,
        truncated=q_result.truncated,
        chart_type=v_result.chart_type,
        x_field=v_result.x_field,
        y_field=v_result.y_field,
        color_field=v_result.color_field,
        iterations=q_result.iterations,
        trace=list(q_result.trace) + list(v_result.trace),
    )
