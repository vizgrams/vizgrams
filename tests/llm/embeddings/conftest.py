# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Fakes for the embeddings stack.

Two protocols cover the whole stack — fakes for both keep every test
hermetic (no OpenAI, no ClickHouse).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import pytest

from semantic.llm.embeddings.provider import EmbeddingResult
from semantic.llm.embeddings.store import StoredEmbedding


@dataclass
class FakeEmbeddingProvider:
    """Deterministic stub: derives a vector from a hash of the text.

    Different inputs → different vectors, same input → same vector.
    Good enough to exercise the cache/skip logic without burning API credits.
    """

    model: str = "fake-embed"
    dim: int = 8
    received: list[list[str]] = field(default_factory=list)

    def _vec(self, text: str) -> list[float]:
        h = hashlib.sha256(text.encode()).digest()
        # Take the first `dim` bytes, normalise to [-1, 1] floats.
        raw = [(b / 127.5) - 1.0 for b in h[: self.dim]]
        return raw

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.received.append(list(texts))
        return [self._vec(t) for t in texts]

    def embed_one(self, text: str) -> EmbeddingResult:
        # Delegate to embed_batch so tracking is consistent across both APIs.
        vec = self.embed_batch([text])[0]
        return EmbeddingResult(vector=vec, model=self.model, dim=self.dim)


@dataclass
class FakeStore:
    """In-memory stand-in for ``EmbeddingsStore``.

    Keyed by (model_id, artifact_type, artifact_name, embed_model) so the
    same artifact in different models doesn't collide. Search uses
    Python-side cosine distance — fine for test sizes.
    """

    rows: dict[tuple, dict] = field(default_factory=dict)
    schema_ensured: int = 0

    def ensure_schema(self) -> None:
        self.schema_ensured += 1

    def upsert(self, *, model_id, artifact_type, artifact_name, description,
               content_hash_val, embed_model, embedding):
        key = (model_id, artifact_type, artifact_name, embed_model)
        self.rows[key] = {
            "description": description, "content_hash": content_hash_val,
            "embedding": list(embedding),
        }

    def current_hash(self, *, model_id, artifact_type, artifact_name, embed_model):
        row = self.rows.get((model_id, artifact_type, artifact_name, embed_model))
        return row["content_hash"] if row else None

    def delete(self, *, model_id, artifact_type, artifact_name):
        for key in [k for k in self.rows if k[:3] == (model_id, artifact_type, artifact_name)]:
            del self.rows[key]

    def find(self, *, model_id, query_embedding, embed_model,
             artifact_types=None, top_k=5, max_distance=None):
        import math

        def cosine_distance(a, b):
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a))
            nb = math.sqrt(sum(y * y for y in b))
            if na == 0 or nb == 0:
                return 1.0
            return 1.0 - (dot / (na * nb))

        candidates = []
        for (m, t, n, em), row in self.rows.items():
            if m != model_id or em != embed_model:
                continue
            if artifact_types and t not in artifact_types:
                continue
            dist = cosine_distance(query_embedding, row["embedding"])
            if max_distance is not None and dist > max_distance:
                continue
            candidates.append(StoredEmbedding(
                model_id=m, artifact_type=t, artifact_name=n,
                description=row["description"], distance=dist,
            ))
        candidates.sort(key=lambda h: h.distance or 0.0)
        return candidates[:top_k]


@pytest.fixture
def fake_provider():
    return FakeEmbeddingProvider()


@pytest.fixture
def fake_store():
    return FakeStore()
