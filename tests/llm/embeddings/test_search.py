# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic/llm/embeddings/search."""

from __future__ import annotations

from semantic.llm.embeddings.search import SearchHit, SemanticSearch


def _seed(store, provider, model_id, kind, name, description):
    """Convenience: embed `description` and stuff it into the store."""
    vec = provider.embed_one(description).vector
    store.upsert(
        model_id=model_id, artifact_type=kind, artifact_name=name,
        description=description, content_hash_val="x",
        embed_model=provider.model, embedding=vec,
    )


def test_find_returns_hits_sorted_by_distance(fake_provider, fake_store):
    _seed(fake_store, fake_provider, "example", "query", "pr_throughput",
          "weekly count of merged pull requests")
    _seed(fake_store, fake_provider, "example", "query", "deploy_count",
          "deployment volume by service")

    search = SemanticSearch(provider=fake_provider, store=fake_store)
    hits = search.find("weekly count of merged pull requests", model_id="example")

    assert len(hits) == 2
    assert all(isinstance(h, SearchHit) for h in hits)
    # Exact-match query should rank first; nearest distance ~= 0
    assert hits[0].name == "pr_throughput"
    assert hits[0].distance == 0.0  # identical text → identical vector → 0 distance
    # Ordering invariant
    assert hits[0].distance <= hits[1].distance


def test_find_filters_by_kind(fake_provider, fake_store):
    _seed(fake_store, fake_provider, "example", "query", "x", "foo")
    _seed(fake_store, fake_provider, "example", "view", "y", "foo")
    _seed(fake_store, fake_provider, "example", "feature", "z", "foo")

    search = SemanticSearch(provider=fake_provider, store=fake_store)
    hits = search.find("foo", model_id="example", kinds=["query", "view"])
    kinds = {h.kind for h in hits}
    assert kinds == {"query", "view"}


def test_find_filters_by_model(fake_provider, fake_store):
    _seed(fake_store, fake_provider, "alpha", "query", "shared_name", "foo")
    _seed(fake_store, fake_provider, "beta", "query", "shared_name", "foo")

    search = SemanticSearch(provider=fake_provider, store=fake_store)
    alpha_hits = search.find("foo", model_id="alpha")
    beta_hits = search.find("foo", model_id="beta")
    assert {h.name for h in alpha_hits} == {"shared_name"}
    assert {h.name for h in beta_hits} == {"shared_name"}
    # Different model_id → different (single) result
    assert all(h.kind == "query" for h in alpha_hits + beta_hits)


def test_find_honours_top_k(fake_provider, fake_store):
    for i in range(10):
        _seed(fake_store, fake_provider, "m", "query", f"q{i}", f"description {i}")
    search = SemanticSearch(provider=fake_provider, store=fake_store)
    hits = search.find("description 3", model_id="m", top_k=3)
    assert len(hits) == 3


def test_find_respects_max_distance(fake_provider, fake_store):
    _seed(fake_store, fake_provider, "m", "query", "match", "PR count by author")
    _seed(fake_store, fake_provider, "m", "query", "miss", "completely unrelated topic")
    search = SemanticSearch(provider=fake_provider, store=fake_store)
    hits = search.find("PR count by author", model_id="m", max_distance=0.001)
    # Only the exact-match should sneak under the tight threshold
    assert [h.name for h in hits] == ["match"]


def test_find_empty_query_returns_empty(fake_provider, fake_store):
    search = SemanticSearch(provider=fake_provider, store=fake_store)
    assert search.find("", model_id="m") == []
    assert search.find("   ", model_id="m") == []


def test_find_swallows_store_errors_to_empty_list(fake_provider, fake_store):
    """SemanticSearch must degrade gracefully — chat works without it."""
    class Boom:
        def find(self, **kwargs):
            raise RuntimeError("CH unreachable")

    search = SemanticSearch(provider=fake_provider, store=Boom())
    assert search.find("anything", model_id="m") == []
