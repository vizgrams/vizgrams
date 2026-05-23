# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic/llm/embeddings/provider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from semantic.llm.embeddings.provider import (
    OpenAIEmbeddingProvider,
    get_default_provider,
)

# ---------------------------------------------------------------------------
# OpenAIEmbeddingProvider — exercises batching + ordering via a mock client
# ---------------------------------------------------------------------------


def _mock_openai_response(*vectors):
    """Build a stand-in for the OpenAI embeddings response shape."""
    resp = MagicMock()
    resp.data = [
        MagicMock(index=i, embedding=v) for i, v in enumerate(vectors)
    ]
    return resp


def test_embed_one_returns_result_with_model_and_dim():
    provider = OpenAIEmbeddingProvider(api_key="sk-test")
    provider._client = MagicMock()
    provider._client.embeddings.create.return_value = _mock_openai_response([0.1, 0.2, 0.3])

    out = provider.embed_one("hello")
    assert out.vector == [0.1, 0.2, 0.3]
    assert out.model == "text-embedding-3-small"
    assert out.dim == 1536  # known dim of the default model


def test_embed_batch_returns_one_vector_per_input():
    provider = OpenAIEmbeddingProvider(api_key="sk-test")
    provider._client = MagicMock()
    provider._client.embeddings.create.return_value = _mock_openai_response(
        [0.1, 0.2], [0.3, 0.4], [0.5, 0.6],
    )

    out = provider.embed_batch(["a", "b", "c"])
    assert out == [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]


def test_embed_batch_chunks_large_inputs():
    """With batch_size=2 a 5-input call should fire 3 chunked requests."""
    provider = OpenAIEmbeddingProvider(api_key="sk-test", batch_size=2)
    provider._client = MagicMock()
    provider._client.embeddings.create.side_effect = [
        _mock_openai_response([0.1], [0.2]),
        _mock_openai_response([0.3], [0.4]),
        _mock_openai_response([0.5]),
    ]

    out = provider.embed_batch(["a", "b", "c", "d", "e"])
    assert out == [[0.1], [0.2], [0.3], [0.4], [0.5]]
    assert provider._client.embeddings.create.call_count == 3


def test_embed_batch_empty_input_returns_empty():
    provider = OpenAIEmbeddingProvider(api_key="sk-test")
    provider._client = MagicMock()
    assert provider.embed_batch([]) == []
    provider._client.embeddings.create.assert_not_called()


def test_embed_batch_sorts_returned_data_by_index():
    """OpenAI returns in input order; we defensively re-sort by `index`."""
    provider = OpenAIEmbeddingProvider(api_key="sk-test")
    provider._client = MagicMock()
    resp = MagicMock()
    resp.data = [
        MagicMock(index=2, embedding=[0.3]),
        MagicMock(index=0, embedding=[0.1]),
        MagicMock(index=1, embedding=[0.2]),
    ]
    provider._client.embeddings.create.return_value = resp

    out = provider.embed_batch(["a", "b", "c"])
    assert out == [[0.1], [0.2], [0.3]]


def test_known_model_dim_lookup():
    p = OpenAIEmbeddingProvider(api_key="x", model="text-embedding-3-large")
    assert p.dim == 3072


def test_unknown_model_falls_back_to_default_dim():
    p = OpenAIEmbeddingProvider(api_key="x", model="some-future-model")
    assert p.dim == 1536


# ---------------------------------------------------------------------------
# get_default_provider — env-driven factory
# ---------------------------------------------------------------------------


def test_factory_returns_openai_when_key_present(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("VZ_EMBEDDING_PROVIDER", raising=False)
    monkeypatch.delenv("VZ_EMBEDDING_MODEL", raising=False)
    with patch("openai.OpenAI"):
        provider = get_default_provider()
    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider.model == "text-embedding-3-small"


def test_factory_returns_none_when_openai_key_missing(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("VZ_EMBEDDING_PROVIDER", raising=False)
    assert get_default_provider() is None


def test_factory_returns_none_when_disabled(monkeypatch):
    monkeypatch.setenv("VZ_EMBEDDING_PROVIDER", "none")
    assert get_default_provider() is None


def test_factory_honours_model_override(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("VZ_EMBEDDING_MODEL", "text-embedding-3-large")
    with patch("openai.OpenAI"):
        provider = get_default_provider()
    assert provider.model == "text-embedding-3-large"
    assert provider.dim == 3072


def test_factory_rejects_unknown_provider(monkeypatch):
    monkeypatch.setenv("VZ_EMBEDDING_PROVIDER", "cohere")
    with pytest.raises(ValueError, match="Unknown VZ_EMBEDDING_PROVIDER"):
        get_default_provider()
