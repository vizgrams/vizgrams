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

    # VG-237 path A: the LLM invoked a saved view. Return a saved_view ref
    # — the UI renders it through the same component the explorer uses.
    # No text2view call needed; the saved view carries its own chart spec.
    if q_result.saved_view_name:
        return ChatTurnResult(
            success=True,
            iterations=q_result.iterations,
            trace=list(q_result.trace),
            saved_view={"name": q_result.saved_view_name, "params": {}},
            query_yaml=q_result.yaml,
            view_yaml=q_result.view_yaml,
            sql=q_result.sql,
        )

    # Paths B (run_saved_query) and C (build_and_run_query) both need
    # text2view to PICK a chart shape that wraps the data. The wrapper
    # view becomes the inline-view we return.
    #
    # ``query_name`` is what text2view writes into the view's ``query:``
    # field. The inline-view endpoint uses it to find the underlying
    # query — saved-by-name for path B, inline by-the-same-name for path
    # C. We resolve through three sources in order: the inline querydef
    # (path C), the saved query's real name (path B), and finally the
    # placeholder (defensive — both prior should cover live runs).
    wrapper_query_name = (
        (q_result.querydef.name if q_result.querydef else None)
        or q_result.saved_query_name
        or QUERY_ARTIFACT_NAME
    )

    v_result: Text2ViewResult = text2view_yaml(
        columns=q_result.columns,
        rows=q_result.rows,
        registry=reg,
        user_intent=message,
        llm_client=client,
        query_name=wrapper_query_name,
        view_name=VIEW_ARTIFACT_NAME,
        **(text2view_kwargs or {}),
    )

    if not v_result.success:
        # Chart picker failed. Wrap the data in a minimal table view so the
        # UI still has something to render through the standard path.
        fallback_view = _fallback_table_view_yaml(
            view_name=VIEW_ARTIFACT_NAME,
            query_name=wrapper_query_name,
            columns=q_result.columns,
        )
        return ChatTurnResult(
            success=True,
            iterations=q_result.iterations,
            trace=list(q_result.trace) + list(v_result.trace),
            inline_view=_inline_view_payload(
                view_yaml=fallback_view,
                query_yaml=q_result.yaml,
                # Path B passes query_yaml=None because the query is saved.
                # Detect: q_result.querydef is None when run_saved_query was the success path.
                is_query_inline=q_result.querydef is not None,
            ),
            query_yaml=q_result.yaml,
            view_yaml=fallback_view,
            sql=q_result.sql,
        )

    # Validate the wrapper view YAML against the same schema the existing
    # POST /view routes use.
    if v_result.yaml:
        try:
            view_validation = view_service.validate_inline_view(
                model_dir, VIEW_ARTIFACT_NAME, v_result.yaml,
                known_query_columns={wrapper_query_name: q_result.columns},
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
        iterations=q_result.iterations,
        trace=list(q_result.trace) + list(v_result.trace),
        inline_view=_inline_view_payload(
            view_yaml=v_result.yaml or "",
            query_yaml=q_result.yaml,
            is_query_inline=q_result.querydef is not None,
        ),
        query_yaml=q_result.yaml,
        view_yaml=v_result.yaml,
        sql=q_result.sql,
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


def _fallback_table_view_yaml(*, view_name: str, query_name: str, columns: list[str]) -> str:
    """Minimal table-view YAML for when text2view fails.

    Lets the UI still render the data through ``ViewContent`` instead of
    failing the whole turn. Lists every result column so nothing's hidden.
    """
    import yaml as _yaml
    return _yaml.safe_dump({
        "name": view_name,
        "type": "table",
        "query": query_name,
        "visualization": {"columns": list(columns)},
    }, sort_keys=False)
