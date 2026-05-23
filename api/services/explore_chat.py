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

from core.db import get_backend
from core.model_config import load_database_config
from engine.query_runner import build_aggregate_query, build_detail_query
from semantic.feature import FeatureDef
from semantic.llm.provider import LLMClient, get_default_client
from semantic.llm.schema_context import build_schema_context
from semantic.llm.text2query import (
    QueryExecutionResult,
    Text2QueryResult,
    text2query_yaml,
)
from semantic.llm.text2view import Text2ViewResult, text2view_yaml
from semantic.query import QueryDef
from semantic.types import expand_event_entities
from semantic.yaml_adapter import YAMLAdapter

logger = logging.getLogger(__name__)

# Cap rows from any one query before they reach the LLM (text2query
# truncates further when serialising for context, but capping here keeps
# the in-process memory bounded for huge result sets).
ROWS_FROM_EXECUTOR = 1000


# ---------------------------------------------------------------------------
# Engine adapter — QueryExecutor for the existing semantic-layer stack
# ---------------------------------------------------------------------------


@dataclass
class SemanticLayerExecutor:
    """Wrap the existing engine as a ``QueryExecutor``.

    Builds aggregate or detail SQL via ``engine.query_runner``, runs it
    against the model's configured backend, and returns a
    ``QueryExecutionResult``. Errors (validation, compilation, execution)
    are caught and returned as ``success=False`` rather than raised — the
    LLM consumes the error text and decides whether to retry.
    """

    model_dir: Path
    rows_per_query: int = ROWS_FROM_EXECUTOR

    def execute(self, query: QueryDef) -> QueryExecutionResult:
        try:
            entities_list = YAMLAdapter.load_entities(self.model_dir / "ontology")
            entities = expand_event_entities({e.name: e for e in entities_list})
            features_by_entity: dict[str, dict[str, FeatureDef]] = {}
            for fd in YAMLAdapter.load_features(self.model_dir / "features"):
                attr_name = fd.feature_id.split(".")[-1]
                features_by_entity.setdefault(fd.entity_type, {})[attr_name] = fd

            if query.entity not in entities:
                return QueryExecutionResult(
                    success=False,
                    error=f"Entity {query.entity!r} not found in model schema",
                )

            dialect = load_database_config(self.model_dir).get("backend", "sqlite")

            if query.is_aggregate:
                sql = build_aggregate_query(
                    query, entities,
                    features_by_entity=features_by_entity,
                    dialect=dialect,
                )
            else:
                page_size = query.pagination.page_size or 100
                sql = build_detail_query(
                    query, entities, page=1, page_size=page_size,
                    features_by_entity=features_by_entity, dialect=dialect,
                )

            backend = get_backend(self.model_dir)
            backend.connect()
            try:
                all_rows = list(backend.execute(sql))
                columns = [
                    c.split(".", 1)[-1] if "." in c else c
                    for c in (backend.last_columns or [])
                ]
            finally:
                backend.close()

            row_count = len(all_rows)
            rows = [list(r) for r in all_rows[: self.rows_per_query]]
            truncated = row_count > self.rows_per_query
            return QueryExecutionResult(
                success=True,
                rows=rows,
                columns=columns,
                row_count=row_count,
                truncated=truncated,
                sql=sql,
            )
        except Exception as exc:  # noqa: BLE001 — error string is consumed by the LLM
            logger.warning("SemanticLayerExecutor failed: %s: %s", type(exc).__name__, exc)
            return QueryExecutionResult(
                success=False,
                error=f"{type(exc).__name__}: {exc}",
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

    def __post_init__(self) -> None:
        if self.columns is None:
            self.columns = []
        if self.rows is None:
            self.rows = []


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

    entities = YAMLAdapter.load_entities(model_dir / "ontology")
    features_by_entity: dict[str, list[FeatureDef]] = {}
    for fd in YAMLAdapter.load_features(model_dir / "features"):
        features_by_entity.setdefault(fd.entity_type, []).append(fd)
    schema = build_schema_context(model_name, entities, features_by_entity)

    q_result: Text2QueryResult = text2query_yaml(
        prompt=message,
        model_name=model_name,
        schema_context=schema,
        executor=exec_,
        llm_client=client,
        history=_history_to_openai(history),
        **(text2query_kwargs or {}),
    )

    if not q_result.success:
        return ChatTurnResult(
            success=False,
            error=q_result.error or "query authoring failed",
            iterations=q_result.iterations,
        )

    v_result: Text2ViewResult = text2view_yaml(
        columns=q_result.columns,
        rows=q_result.rows,
        user_intent=message,
        llm_client=client,
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
        )

    return ChatTurnResult(
        success=True,
        content=v_result.caption,
        query_yaml=q_result.yaml,
        view_yaml=v_result.yaml,
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
    )
