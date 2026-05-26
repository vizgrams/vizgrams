# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""find_artifacts — semantic search over the catalog (Epic 20 VG-232).

Lets the LLM ask *"what's already here for monthly PR throughput?"*
before authoring a new query. The handler is a thin wrapper around
``SemanticSearch.find`` from VG-230 (semantic/llm/embeddings/search.py).

Empty / degraded behaviour: when no ``SemanticSearch`` is wired into
``ToolContext`` (e.g. embeddings disabled because no OPENAI_API_KEY or
ClickHouse unavailable), the handler returns an empty match list with a
``warning`` so the LLM knows reuse isn't an option for this model. The
chat keeps working — it just falls back to authoring every query from
scratch.
"""

from __future__ import annotations

import logging
from pathlib import Path

from semantic.llm.tools.registry import Tool, ToolContext, ToolResult
from semantic.yaml_adapter import YAMLAdapter

logger = logging.getLogger(__name__)

# Restrict ``kind`` to the artifact types we actually embed (see
# embeddings/store.py INDEXED_ARTIFACT_TYPES). Mappers / extractors aren't
# indexed and we don't want the LLM asking for them.
_VALID_KINDS = ("query", "view", "feature", "entity", "application")

PARAMETERS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": (
                "Natural-language description of what you're looking for. "
                "Examples: 'weekly PR throughput', 'open issues by team', "
                "'days between commit and merge'."
            ),
            "minLength": 1,
        },
        "kind": {
            "type": "string",
            "enum": list(_VALID_KINDS),
            "description": (
                "Filter to one artifact type. Omit to search across all. "
                "Use 'view' to find existing charts; 'query' for raw data "
                "shapes; 'feature' for reusable computed columns."
            ),
        },
        "top_k": {
            "type": "integer",
            "minimum": 1,
            "maximum": 20,
            "default": 5,
            "description": "How many matches to return, ranked by relevance.",
        },
    },
    "required": ["query"],
}


def _handler(args: dict, ctx: ToolContext) -> ToolResult:
    """Run the search; format matches for LLM consumption."""
    query = (args.get("query") or "").strip()
    if not query:
        return ToolResult(
            payload={"matches": [], "error": "empty query"},
            success=False,
        )

    if ctx.search is None:
        # Embeddings not wired — degrade gracefully so the LLM can still
        # author. Include a hint so a curious LLM can adapt its strategy.
        return ToolResult(
            payload={
                "matches": [],
                "warning": "Semantic search not configured for this model — "
                           "no existing artifacts available for reuse.",
            },
            success=True,
        )

    kind = args.get("kind")
    kinds = [kind] if kind in _VALID_KINDS else None
    top_k = int(args.get("top_k") or 5)

    try:
        hits = ctx.search.find(
            query, model_id=ctx.model_id, kinds=kinds, top_k=top_k,
        )
    except Exception as exc:  # noqa: BLE001 — handlers must not raise
        logger.warning("find_artifacts failed: %s", exc)
        return ToolResult(
            payload={
                "matches": [],
                "error": f"{type(exc).__name__}: {exc}",
            },
            success=False,
        )

    matches = []
    for h in hits:
        m = {
            "kind": h.kind,
            "name": h.name,
            "description": h.description,
            # Round so the LLM doesn't fixate on noise in the 6th decimal.
            "distance": round(h.distance, 3),
        }
        if ctx.model_dir is not None:
            if h.kind == "view":
                m.update(_enrich_view(ctx.model_dir, h.name))
            elif h.kind == "query":
                m.update(_enrich_query(ctx.model_dir, h.name))
        matches.append(m)
    return ToolResult(
        payload={"matches": matches, "count": len(matches)},
        success=True,
    )


# ---------------------------------------------------------------------------
# Shape enrichment — surface chart_type / root / measures so the LLM can
# judge fit before invoking run_saved_view / run_saved_query. Without this,
# the LLM reuses by name proximity alone and picks tables when the user
# implied a chart, or PR-rooted queries for Person-rooted questions.
# ---------------------------------------------------------------------------


def _enrich_view(model_dir: Path, name: str) -> dict:
    try:
        v = YAMLAdapter.load_view(name, model_dir / "views")
        if v is None:
            return {}
        out: dict = {"chart_type": _chart_type_label(v)}
        q = YAMLAdapter.load_query(v.query, model_dir / "queries")
        if q is not None:
            out["root"] = getattr(q, "entity", None)
            out["measures"] = sorted((getattr(q, "metrics", {}) or {}).keys())
        return out
    except Exception:  # noqa: BLE001 — enrichment is best-effort
        return {}


def _enrich_query(model_dir: Path, name: str) -> dict:
    try:
        q = YAMLAdapter.load_query(name, model_dir / "queries")
        if q is None:
            return {}
        return {
            "root": getattr(q, "entity", None),
            "measures": sorted((getattr(q, "metrics", {}) or {}).keys()),
            "has_params": bool(getattr(q, "parameters", None)),
        }
    except Exception:  # noqa: BLE001 — enrichment is best-effort
        return {}


def _chart_type_label(v) -> str:
    """Flatten ``ViewDef.type`` + nested ``visualization.chart_type`` into one
    label the LLM can read (``bar`` / ``line`` / ``kpi`` / ``table`` / ...)."""
    vtype = getattr(v, "type", None)
    if vtype == "chart":
        return (getattr(v, "visualization", None) or {}).get("chart_type") or "chart"
    if vtype == "metric":
        return "kpi"
    return vtype or ""


def _summarize(result: ToolResult) -> str:
    """One-line trace summary for VG-239 'Show your work'."""
    matches = result.payload.get("matches") or []
    if not matches:
        warn = result.payload.get("warning")
        return warn or "no matches"
    top = matches[0]
    return (
        f"{len(matches)} matches; top: {top.get('kind')}/{top.get('name')} "
        f"(dist {top.get('distance')})"
    )


FIND_ARTIFACTS = Tool(
    name="find_artifacts",
    description=(
        "Semantic search over the model's existing artifact catalog "
        "(queries, views, features, entities, applications). Call this "
        "before authoring a new query so you can reuse what's already "
        "there. Each view hit carries chart_type + root + measures; each "
        "query hit carries root + measures + has_params. Smaller distance "
        "= closer match by description, BUT a close-by-name hit is not "
        "enough on its own — confirm chart_type matches what the user "
        "wants (bar/line/kpi/table) and root matches the subject of the "
        "question (e.g. Person for 'who…', PullRequest for 'PRs…'). If "
        "no hit fits both, prefer build_and_run_query over forcing the "
        "wrong saved artifact."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("query_authoring", "catalog"),
    summarize=_summarize,
)
