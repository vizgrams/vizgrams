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

from semantic.llm.tools.registry import Tool, ToolContext, ToolResult

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

    matches = [
        {
            "kind": h.kind,
            "name": h.name,
            "description": h.description,
            # Round so the LLM doesn't fixate on noise in the 6th decimal.
            "distance": round(h.distance, 3),
        }
        for h in hits
    ]
    return ToolResult(
        payload={"matches": matches, "count": len(matches)},
        success=True,
    )


FIND_ARTIFACTS = Tool(
    name="find_artifacts",
    description=(
        "Semantic search over the model's existing artifact catalog "
        "(queries, views, features, entities, applications). Call this "
        "before authoring a new query — a near match means you can reuse "
        "or adapt instead of rebuilding. Smaller distance = better match; "
        "anything under ~0.3 is usually worth reusing directly."
    ),
    parameters_schema=PARAMETERS_SCHEMA,
    handler=_handler,
    tags=("query_authoring", "catalog"),
)
