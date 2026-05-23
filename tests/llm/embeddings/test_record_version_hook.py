# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests that core/metadata_db.record_version fires the index hook correctly."""

from __future__ import annotations

import pytest

from core import metadata_db


@pytest.fixture
def model_dir(tmp_path):
    d = tmp_path / "demo"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def clear_hook():
    """Belt-and-braces: every test starts with the hook cleared."""
    metadata_db.set_index_hook(None)
    yield
    metadata_db.set_index_hook(None)


def test_hook_fires_on_new_artifact(model_dir):
    received = []
    metadata_db.set_index_hook(
        lambda md, kind, name, content: received.append((kind, name, content)),
    )
    written = metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    assert written is True
    assert received == [("query", "x", "name: x\n")]


def test_hook_does_not_fire_on_no_op_save(model_dir):
    received = []
    metadata_db.set_index_hook(
        lambda md, kind, name, content: received.append((kind, name, content)),
    )
    metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    # Same content again — record_version returns False, hook should NOT re-fire
    written = metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    assert written is False
    assert len(received) == 1  # only the first call


def test_hook_fires_on_each_content_change(model_dir):
    received = []
    metadata_db.set_index_hook(
        lambda md, kind, name, content: received.append(content),
    )
    metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    metadata_db.record_version(model_dir, "query", "x", "name: x\ndescription: a\n")
    metadata_db.record_version(model_dir, "query", "x", "name: x\ndescription: b\n")
    assert len(received) == 3


def test_hook_raising_does_not_fail_the_save(model_dir, caplog):
    metadata_db.set_index_hook(
        lambda md, kind, name, content: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    written = metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    assert written is True  # save still succeeds
    # …and the version is queryable
    current = metadata_db.get_current_content(model_dir, "query", "x")
    assert current == "name: x\n"
    # …and the exception was logged, not propagated
    assert any("index hook raised" in rec.message for rec in caplog.records)


def test_no_hook_registered_is_a_no_op(model_dir):
    """The default (no hook) must not break record_version."""
    metadata_db.set_index_hook(None)
    written = metadata_db.record_version(model_dir, "query", "x", "name: x\n")
    assert written is True
