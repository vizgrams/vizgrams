# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the DuckDB-backed ``EmbeddingsStore``.

Exercises the real store against a per-test on-disk DuckDB file. The
existing suite uses ``FakeStore`` for behavioural coverage of the
indexer / reconciler / search — these tests specifically pin the SQL
behaviour the fake mirrors (upsert dedup, cosine ordering, delete cascade)
so the two implementations don't drift.
"""

from __future__ import annotations

import pytest

from semantic.llm.embeddings.store import EmbeddingsStore, content_hash


@pytest.fixture
def store(tmp_path):
    """Return a per-test store with schema pre-applied. Uses on-disk
    duckdb so the path resolution + connection lifecycle get exercised
    too, not just an in-memory shortcut."""
    s = EmbeddingsStore(db_path=tmp_path / "embeddings.duckdb")
    s.ensure_schema()
    yield s
    s.close()


def _row(name: str = "q1", vec: list[float] | None = None) -> dict:
    """Canonical upsert kwargs — one place to change if the signature grows."""
    return {
        "model_id": "iagai",
        "artifact_type": "query",
        "artifact_name": name,
        "description": f"desc for {name}",
        "content_hash_val": content_hash(f"body of {name}"),
        "embed_model": "fake-embed",
        "embedding": vec if vec is not None else [1.0, 0.0, 0.0],
    }


# ---------------------------------------------------------------------------
# Schema + lifecycle
# ---------------------------------------------------------------------------


def test_ensure_schema_is_idempotent(tmp_path):
    """Called from API startup + reconciler + reindex CLI. If a second
    call raised (``CREATE TABLE`` without IF NOT EXISTS), any restart of
    a running deploy would crash."""
    s = EmbeddingsStore(db_path=tmp_path / "e.duckdb")
    s.ensure_schema()
    s.ensure_schema()  # second call must be a no-op
    s.close()


def test_default_db_path_honours_env_override(tmp_path, monkeypatch):
    """``VZ_EMBEDDINGS_DB`` is the tests + prod escape hatch. Regression
    guard for a rename or import path breaking the env lookup."""
    from semantic.llm.embeddings.store import _default_db_path
    override = tmp_path / "custom.duckdb"
    monkeypatch.setenv("VZ_EMBEDDINGS_DB", str(override))
    assert _default_db_path() == override


# ---------------------------------------------------------------------------
# Upsert + current_hash
# ---------------------------------------------------------------------------


def test_upsert_then_current_hash_returns_the_stored_hash(store):
    kwargs = _row()
    store.upsert(**kwargs)
    got = store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        embed_model="fake-embed",
    )
    assert got == kwargs["content_hash_val"]


def test_current_hash_returns_none_for_unknown(store):
    """The indexer skips re-embedding when ``current_hash == new_hash``,
    so an unknown artifact MUST return None (not empty string) or the
    indexer would compare None == "some-hash" and re-embed forever."""
    got = store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="never-inserted",
        embed_model="fake-embed",
    )
    assert got is None


def test_upsert_replaces_row_on_pk_conflict(store):
    """The indexer re-runs on every artifact save. If two upserts with
    the same PK produce two rows, the search returns duplicates and
    ``current_hash`` becomes non-deterministic. Verify one row survives."""
    store.upsert(**_row())
    store.upsert(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        description="new description",
        content_hash_val="new-hash",
        embed_model="fake-embed", embedding=[0.0, 1.0, 0.0],
    )
    # One row, latest values.
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        embed_model="fake-embed",
    ) == "new-hash"

    results = store.find(
        model_id="iagai", query_embedding=[0.0, 1.0, 0.0],
        embed_model="fake-embed",
    )
    assert len(results) == 1
    assert results[0].description == "new description"


def test_upsert_isolates_by_embed_model(store):
    """Two different embed_models (openai vs. mistral) MUST coexist —
    otherwise upgrading the embedder mid-flight would delete the old
    rows before the reconciler could refresh them."""
    store.upsert(**_row(name="q1"))
    store.upsert(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        description="mistral desc", content_hash_val="mistral-hash",
        embed_model="mistral-embed", embedding=[0.5, 0.5, 0.5],
    )
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        embed_model="fake-embed") == _row()["content_hash_val"]
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="q1",
        embed_model="mistral-embed") == "mistral-hash"


# ---------------------------------------------------------------------------
# find_outdated
# ---------------------------------------------------------------------------


def test_find_outdated_returns_only_older_versions(store):
    """When TEXT_BUILDER_VERSION bumps from 1 → 2, the reconciler needs
    to know which rows still have the old version. Verify the version
    filter is strictly ``<``, not ``<=``."""
    store.upsert(**_row(name="old-v1"))  # default text_builder_version=1
    store.upsert(
        **_row(name="current-v2"),
    )  # will bump below

    # Manually mark 'current-v2' as version 2 by re-upserting.
    kw = _row(name="current-v2")
    kw_v2 = {**kw, "text_builder_version": 2}
    store.upsert(**kw_v2)

    outdated = store.find_outdated(
        model_id="iagai", embed_model="fake-embed", current_version=2,
    )
    assert ("query", "old-v1") in outdated
    assert ("query", "current-v2") not in outdated


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_removes_row_across_embed_models(store):
    """When an artifact is renamed / removed from the ontology, the
    hook deletes all its embedding rows. That must include every
    embed_model variant, not just the current one."""
    store.upsert(**_row(name="gone"))
    store.upsert(
        model_id="iagai", artifact_type="query", artifact_name="gone",
        description="other embed", content_hash_val="other-hash",
        embed_model="mistral-embed", embedding=[0.0, 1.0, 0.0],
    )
    store.delete(model_id="iagai", artifact_type="query", artifact_name="gone")
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="gone",
        embed_model="fake-embed") is None
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="gone",
        embed_model="mistral-embed") is None


def test_delete_leaves_other_artifacts_alone(store):
    store.upsert(**_row(name="keep"))
    store.upsert(**_row(name="gone"))
    store.delete(model_id="iagai", artifact_type="query", artifact_name="gone")
    assert store.current_hash(
        model_id="iagai", artifact_type="query", artifact_name="keep",
        embed_model="fake-embed") is not None


# ---------------------------------------------------------------------------
# find — cosine distance ordering
# ---------------------------------------------------------------------------


def test_find_returns_nearest_first(store):
    """The whole point of the store. Query vector [1,0,0]; three
    candidates at cos-distances 0, 0.5, 1.0. Result must come back in
    ascending distance."""
    store.upsert(**_row(name="exact",   vec=[1.0, 0.0, 0.0]))          # dist 0
    store.upsert(**_row(name="middle",  vec=[0.7071, 0.7071, 0.0]))    # dist ≈ 0.29
    store.upsert(**_row(name="far",     vec=[0.0, 1.0, 0.0]))          # dist 1

    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed", top_k=3,
    )
    names = [h.artifact_name for h in hits]
    assert names == ["exact", "middle", "far"]
    assert hits[0].distance == pytest.approx(0.0, abs=1e-5)
    assert hits[2].distance == pytest.approx(1.0, abs=1e-5)


def test_find_respects_top_k(store):
    for i in range(5):
        store.upsert(**_row(name=f"q{i}", vec=[1.0 - i * 0.1, 0.0, 0.0]))
    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed", top_k=2,
    )
    assert len(hits) == 2


def test_find_filters_by_artifact_types(store):
    """The orchestrator's ``find_artifacts`` LLM tool asks for a specific
    kind — queries only, or views only. The store must respect the filter."""
    store.upsert(**_row(name="qA"))
    kw = _row(name="vA")
    kw["artifact_type"] = "view"
    store.upsert(**kw)

    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed", artifact_types=["view"],
    )
    assert [h.artifact_type for h in hits] == ["view"]


def test_find_respects_max_distance(store):
    """The orchestrator uses ``max_distance`` as the "strong match"
    threshold below which reuse is preferred over generating a new query.
    A candidate above the threshold MUST be excluded, not just deprioritised."""
    store.upsert(**_row(name="close", vec=[1.0, 0.0, 0.0]))
    store.upsert(**_row(name="far",   vec=[0.0, 1.0, 0.0]))
    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed", max_distance=0.5,
    )
    names = [h.artifact_name for h in hits]
    assert "close" in names
    assert "far" not in names


def test_find_scopes_by_model_id(store):
    """A view named 'foo' in model 'iagai' must not appear in a search
    scoped to model 'openflights', or the LLM would confuse two
    orgs' data."""
    store.upsert(**_row(name="foo"))
    kw = _row(name="foo")
    kw["model_id"] = "openflights"
    store.upsert(**kw)
    hits = store.find(
        model_id="openflights", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed",
    )
    assert len(hits) == 1
    assert hits[0].model_id == "openflights"


def test_find_returns_empty_when_no_rows(store):
    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed",
    )
    assert hits == []


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


def test_concurrent_upserts_serialize_cleanly(store):
    """The indexer runs on a background thread pool; ``find`` runs on the
    request path. DuckDB is single-writer per connection, so the store
    holds a lock. Verify a many-thread hammer doesn't corrupt state."""
    import threading

    def worker(i):
        store.upsert(**_row(name=f"q{i}", vec=[float(i % 10) / 10, 0.0, 0.0]))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All 30 rows survived, no duplicates.
    hits = store.find(
        model_id="iagai", query_embedding=[1.0, 0.0, 0.0],
        embed_model="fake-embed", top_k=100,
    )
    assert len({h.artifact_name for h in hits}) == 30
