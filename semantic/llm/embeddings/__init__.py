# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Semantic search over the artifact catalog (Epic 20 VG-230).

Two-stage retrieval for the Explore Chat:

  1. User prompt → embed → cosine search over the artifact catalog
     (queries, views, features, entities, applications) → top-K
     matches.
  2. Top-K matches + ontology summary → LLM authors response.

This stage replaces the "stuff every artifact into the system prompt"
approach, which doesn't scale past a few dozen artifacts.

Modules:

  ``provider``  — ``EmbeddingProvider`` protocol + ``OpenAIEmbeddingProvider``;
                  env-driven factory ``get_default_provider()``.

  ``store``     — ``EmbeddingsStore`` backed by a ClickHouse table
                  (``vizgrams_meta.artifact_embeddings``); upsert + cosine
                  search via the engine's ``cosineDistance`` function.

  ``search``    — ``SemanticSearch.find(query, model_id, kind?, k)``:
                  high-level facade that embeds the query then asks the
                  store. The seam consumed by LLM tools.

  ``index``     — background-thread indexer + per-kind text builders. The
                  hook fired by ``metadata_db.record_version`` after every
                  artifact save.
"""

from semantic.llm.embeddings.provider import (
    EmbeddingProvider,
    EmbeddingResult,
    OpenAIEmbeddingProvider,
    get_default_provider,
)

__all__ = [
    "EmbeddingProvider",
    "EmbeddingResult",
    "OpenAIEmbeddingProvider",
    "get_default_provider",
]
