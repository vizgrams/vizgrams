# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""SemanticSearch — the seam consumed by LLM tools (Epic 20 VG-230).

A thin facade over a provider + store: the LLM tool calls
``find(query="monthly PR throughput", model_id="example")`` and gets back
ranked matches without knowing anything about embeddings, vectors, or
the underlying ClickHouse table.

Decoupling matters because Epic 24 will swap the provider (cheap local
embeddings for short prompts vs. text-embedding-3-small for long ones)
and Epic 26+ might swap the store (a vector DB if we ever outgrow CH).
Tools see neither change.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from semantic.llm.embeddings.provider import EmbeddingProvider
from semantic.llm.embeddings.store import EmbeddingsStore, StoredEmbedding

logger = logging.getLogger(__name__)


@dataclass
class SearchHit:
    """One ranked artifact match, in the shape the LLM tool wants."""

    kind: str            # 'query' | 'view' | 'feature' | 'entity' | 'application'
    name: str
    description: str
    distance: float      # cosine distance — smaller = more similar (0..2)


class SemanticSearch:
    """High-level catalog search.

    Constructor takes the two protocols so tests can pass fakes for both
    (``FakeEmbeddingProvider`` + ``FakeStore`` in ``tests/llm/embeddings/conftest.py``).
    Production wiring uses ``get_default_provider()`` + ``EmbeddingsStore``.
    """

    def __init__(self, *, provider: EmbeddingProvider, store: EmbeddingsStore) -> None:
        self.provider = provider
        self.store = store

    def find(
        self,
        query: str,
        *,
        model_id: str,
        kinds: list[str] | None = None,
        top_k: int = 5,
        max_distance: float | None = None,
    ) -> list[SearchHit]:
        """Embed ``query`` and return the top-K artifact matches.

        ``kinds`` restricts to one or more artifact types ('query', 'view',
        'feature', etc.). ``max_distance`` filters out poor matches before
        returning — the orchestrator's "is this a strong-enough match to
        reuse, or should we author new?" decision boundary.
        """
        if not query.strip():
            return []
        embedding = self.provider.embed_one(query)
        try:
            hits: list[StoredEmbedding] = self.store.find(
                model_id=model_id,
                query_embedding=embedding.vector,
                embed_model=self.provider.model,
                artifact_types=kinds,
                top_k=top_k,
                max_distance=max_distance,
            )
        except Exception as exc:  # noqa: BLE001 — search failures degrade chat, never crash it
            logger.warning("SemanticSearch.find failed: %s", exc)
            return []
        return [
            SearchHit(
                kind=h.artifact_type,
                name=h.artifact_name,
                description=h.description,
                distance=h.distance or 0.0,
            )
            for h in hits
        ]
