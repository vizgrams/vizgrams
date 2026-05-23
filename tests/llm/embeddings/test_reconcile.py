# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for embeddings.reconcile — self-heal on text-builder version bumps."""

from __future__ import annotations

from pathlib import Path

import pytest

from semantic.llm.embeddings.reconcile import reconcile_all_models, reconcile_model


@pytest.fixture
def stub_metadata_db(monkeypatch):
    """Patch the metadata_db loader so tests don't need a real api.db."""

    contents: dict[tuple, str] = {}

    def get_current_content(model_dir, kind, name):
        return contents.get((Path(model_dir).name, kind, name))

    monkeypatch.setattr(
        "semantic.llm.embeddings.reconcile.metadata_db.get_current_content",
        get_current_content,
    )
    return contents


def _seed_stale_row(store, model_id, kind, name, version=1, embed_model="fake-embed"):
    key = (model_id, kind, name, embed_model)
    store.rows[key] = {
        "description": "stale description",
        "content_hash": "old-hash",
        "embedding": [0.0] * 8,
        "text_builder_version": version,
    }


# ---------------------------------------------------------------------------
# reconcile_model
# ---------------------------------------------------------------------------


def test_reconcile_returns_empty_report_when_nothing_stale(fake_provider, fake_store, tmp_path):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report == {"stale": 0, "reindexed": 0, "failed": 0}


def test_reconcile_re_embeds_stale_rows(fake_provider, fake_store, tmp_path, stub_metadata_db):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    # Seed: one v1 row in the store + matching content in api.db
    _seed_stale_row(fake_store, "demo", "query", "old_q", version=1)
    stub_metadata_db[("demo", "query", "old_q")] = "name: old_q\nroot: X\n"

    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report == {"stale": 1, "reindexed": 1, "failed": 0}

    # Row should now be at v2 (the test-default 'current')
    key = ("demo", "query", "old_q", "fake-embed")
    assert fake_store.rows[key]["text_builder_version"] == 2
    # Hash changed because the embedding text changed (new format)
    assert fake_store.rows[key]["content_hash"] != "old-hash"


def test_reconcile_skips_rows_at_current_version(fake_provider, fake_store, tmp_path, stub_metadata_db):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    _seed_stale_row(fake_store, "demo", "query", "fresh_q", version=2)
    stub_metadata_db[("demo", "query", "fresh_q")] = "name: fresh_q\nroot: X\n"

    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report["stale"] == 0
    # Provider must not have been called for an up-to-date row
    assert fake_provider.received == []


def test_reconcile_deletes_orphans(fake_provider, fake_store, tmp_path, stub_metadata_db):
    """Embedding row exists, but artifact has been deleted from api.db."""
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    _seed_stale_row(fake_store, "demo", "query", "orphan", version=1)
    # Note: nothing added to stub_metadata_db — so get_current_content returns None

    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report["stale"] == 1
    assert report["reindexed"] == 0
    # Row gone — store.delete fired
    assert ("demo", "query", "orphan", "fake-embed") not in fake_store.rows


def test_reconcile_handles_individual_artifact_failures(
    fake_provider, fake_store, tmp_path, stub_metadata_db,
):
    """One artifact failing must not stop other artifacts from re-embedding."""
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    _seed_stale_row(fake_store, "demo", "query", "good", version=1)
    _seed_stale_row(fake_store, "demo", "query", "bad", version=1)
    stub_metadata_db[("demo", "query", "good")] = "name: good\nroot: X\n"
    stub_metadata_db[("demo", "query", "bad")] = "name: bad\nroot: X\n"

    # Make the provider raise specifically for the 'bad' artifact's text
    original_embed = fake_provider.embed_batch

    def selectively_failing(texts):
        if any("bad" in t for t in texts):
            raise RuntimeError("synthetic embed failure for bad")
        return original_embed(texts)

    fake_provider.embed_batch = selectively_failing

    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report["stale"] == 2
    assert report["reindexed"] == 1
    # bad still v1 (re-embed swallowed the error in index_now)
    assert fake_store.rows[("demo", "query", "bad", "fake-embed")]["text_builder_version"] == 1


def test_reconcile_survives_store_query_failure(fake_provider, fake_store, tmp_path):
    """find_outdated crashing must not propagate — return empty report instead."""
    def boom(**kwargs):
        raise RuntimeError("CH unreachable")

    fake_store.find_outdated = boom  # type: ignore[method-assign]
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    report = reconcile_model(
        "demo", model_dir,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report == {"stale": 0, "reindexed": 0, "failed": 0}


# ---------------------------------------------------------------------------
# reconcile_all_models
# ---------------------------------------------------------------------------


def test_reconcile_all_walks_every_model_subdir(
    fake_provider, fake_store, tmp_path, stub_metadata_db,
):
    (tmp_path / "alpha").mkdir()
    (tmp_path / "beta").mkdir()
    (tmp_path / "_not_a_model.txt").write_text("file, not dir")

    _seed_stale_row(fake_store, "alpha", "query", "q1", version=1)
    _seed_stale_row(fake_store, "beta", "query", "q2", version=1)
    stub_metadata_db[("alpha", "query", "q1")] = "name: q1\n"
    stub_metadata_db[("beta", "query", "q2")] = "name: q2\n"

    report = reconcile_all_models(
        tmp_path,
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report["total_stale"] == 2
    assert report["total_reindexed"] == 2
    assert set(report["models"]) == {"alpha", "beta"}


def test_reconcile_all_handles_missing_models_dir(fake_provider, fake_store, tmp_path):
    """Pointing at a nonexistent dir is a no-op, not a crash."""
    report = reconcile_all_models(
        tmp_path / "nope",
        provider=fake_provider, store=fake_store, current_version=2,
    )
    assert report == {"total_stale": 0, "total_reindexed": 0, "models": {}}
