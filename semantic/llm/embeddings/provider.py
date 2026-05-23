# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Embedding provider abstraction (Epic 20 VG-230).

Mirrors the shape of ``semantic/llm/provider.py``: a thin Protocol +
concrete provider classes + an env-driven factory. Lets the rest of the
embeddings stack stay provider-agnostic (OpenAI today; Anthropic /
sentence-transformers / Bedrock are slot-in additions for Epic 24).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class EmbeddingResult:
    """Output of ``embed_one`` / per-row output of ``embed_batch``."""

    vector: list[float]
    model: str          # e.g. 'text-embedding-3-small'
    dim: int            # vector length; redundant with len(vector) but cheap


@runtime_checkable
class EmbeddingProvider(Protocol):
    """A pluggable embedding backend.

    Implementations are responsible for batching, retries, and rate-limit
    handling on the wire. Callers pass a list of strings and get a list
    of vectors back in the same order.
    """

    model: str
    dim: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...

    def embed_one(self, text: str) -> EmbeddingResult: ...


# ---------------------------------------------------------------------------
# OpenAI implementation
# ---------------------------------------------------------------------------

# Known dimensions for OpenAI embedding models. Used so callers can spec
# storage column widths without an extra round-trip just to discover dims.
_OPENAI_DIMENSIONS: dict[str, int] = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


class OpenAIEmbeddingProvider:
    """``EmbeddingProvider`` backed by the OpenAI Embeddings API."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "text-embedding-3-small",
        batch_size: int = 128,
    ) -> None:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ImportError(
                "openai package is required for OpenAIEmbeddingProvider. "
                "Install with: poetry add openai"
            ) from exc
        self._client = OpenAI(api_key=api_key)
        self.model = model
        self.dim = _OPENAI_DIMENSIONS.get(model, 1536)
        self._batch_size = batch_size

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        # OpenAI accepts a single batch up to ~2048 inputs in practice, but
        # we chunk smaller to stay well under any provider-side limits and
        # to keep individual requests bounded in latency.
        for i in range(0, len(texts), self._batch_size):
            chunk = texts[i : i + self._batch_size]
            resp = self._client.embeddings.create(model=self.model, input=chunk)
            # OpenAI returns embeddings in input order — defensive sort by index
            # to guarantee ordering even if the API ever changes.
            sorted_data = sorted(resp.data, key=lambda d: d.index)
            out.extend(d.embedding for d in sorted_data)
        return out

    def embed_one(self, text: str) -> EmbeddingResult:
        vec = self.embed_batch([text])[0]
        return EmbeddingResult(vector=vec, model=self.model, dim=self.dim)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_default_provider() -> EmbeddingProvider | None:
    """Return a configured ``EmbeddingProvider``, or None if disabled.

    Reads ``VZ_EMBEDDING_PROVIDER`` (default ``openai``) and the relevant
    credentials. Returns ``None`` on missing credentials so the caller can
    gracefully disable indexing rather than crash the app — a model
    without embeddings still chats fine, it just doesn't get reuse.
    """
    provider = os.environ.get("VZ_EMBEDDING_PROVIDER", "openai").lower()
    model_override = os.environ.get("VZ_EMBEDDING_MODEL")

    if provider == "none":
        return None

    if provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return None
        return OpenAIEmbeddingProvider(
            api_key=api_key,
            model=model_override or "text-embedding-3-small",
        )

    raise ValueError(
        f"Unknown VZ_EMBEDDING_PROVIDER: {provider!r}. "
        f"Currently supported: 'openai', 'none'."
    )
