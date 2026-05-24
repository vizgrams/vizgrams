# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the ownership surface (VG-250 / VG-251 / VG-252).

Covers the metadata_db layer: schema migration, ``record_version``
stamping ``created_by`` + ``created_via``, and the ``get_owner`` /
``list_owners`` helpers used by the service layer to populate
artifact responses.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.metadata_db import (
    get_owner,
    list_owners,
    record_version,
)


@pytest.fixture
def model_dir(tmp_path) -> Path:
    d = tmp_path / "demo_model"
    d.mkdir()
    return d


class TestRecordVersionStamping:
    def test_legacy_callers_without_kwargs_write_nulls(self, model_dir):
        """Backwards compatibility: pre-VG-250 call sites still work."""
        record_version(model_dir, "view", "v1", "name: v1\n")
        owner = get_owner(model_dir, "view", "v1")
        assert owner is not None
        assert owner["created_by"] is None
        assert owner["created_via"] is None
        assert owner["created_at"]  # always set by record_version

    def test_user_id_and_via_persist(self, model_dir):
        record_version(
            model_dir, "view", "v1", "name: v1\n",
            user_id="user-abc", via="editor",
        )
        owner = get_owner(model_dir, "view", "v1")
        assert owner["created_by"] == "user-abc"
        assert owner["created_via"] == "editor"

    def test_new_version_overwrites_owner_on_each_save(self, model_dir):
        """The current version's owner is whoever made the latest edit —
        good for "show who last touched this" semantics. Historical
        rows keep their author in the version log."""
        record_version(model_dir, "view", "v1", "v1\n", user_id="alice", via="editor")
        record_version(model_dir, "view", "v1", "v2\n", user_id="bob", via="chat")
        owner = get_owner(model_dir, "view", "v1")
        assert owner["created_by"] == "bob"
        assert owner["created_via"] == "chat"


class TestGetOwner:
    def test_missing_artifact_returns_none(self, model_dir):
        assert get_owner(model_dir, "view", "nope") is None

    def test_returns_fields_for_current_version(self, model_dir):
        record_version(model_dir, "view", "v1", "v1\n", user_id="u", via="editor")
        owner = get_owner(model_dir, "view", "v1")
        assert set(owner.keys()) == {"created_by", "created_via", "created_at"}


class TestListOwners:
    def test_empty_returns_empty(self, model_dir):
        assert list_owners(model_dir, "view") == {}

    def test_keyed_by_name_for_current_versions_only(self, model_dir):
        record_version(model_dir, "view", "a", "a\n", user_id="u1", via="editor")
        record_version(model_dir, "view", "b", "b\n", user_id="u2", via="chat")
        # New version on "a" — list should show the latest owner only.
        record_version(model_dir, "view", "a", "a2\n", user_id="u3", via="editor")
        out = list_owners(model_dir, "view")
        assert set(out.keys()) == {"a", "b"}
        assert out["a"]["created_by"] == "u3"
        assert out["b"]["created_by"] == "u2"

    def test_filters_by_type(self, model_dir):
        record_version(model_dir, "view", "v", "v\n", user_id="u")
        record_version(model_dir, "query", "q", "q\n", user_id="u")
        assert set(list_owners(model_dir, "view").keys()) == {"v"}
        assert set(list_owners(model_dir, "query").keys()) == {"q"}


class TestMigrationOnExistingDb:
    """Simulates: app upgraded from a pre-VG-250 build. Old artifact_versions
    rows pre-date the new columns; the migration adds them as nullable."""

    def _drop_owner_columns_and_remove_migration(self, model_dir: Path) -> None:
        """Recreate the table without ownership cols + clear the migration flag,
        as if the DB came from an older deploy.

        SQLite can't DROP COLUMN before 3.35 portably; rebuild the table.
        """
        import sqlite3

        from core.metadata_db import get_api_db_path
        conn = sqlite3.connect(str(get_api_db_path()))
        try:
            conn.executescript("""
                CREATE TABLE av_old AS
                  SELECT id, model_id, type, name, version_num, content, checksum,
                         message, created_at, is_current
                  FROM artifact_versions;
                DROP TABLE artifact_versions;
                ALTER TABLE av_old RENAME TO artifact_versions;
            """)
            conn.execute(
                "DELETE FROM schema_migrations WHERE name='add_ownership_columns'"
            )
            conn.commit()
        finally:
            conn.close()

    def test_migration_adds_columns_on_legacy_db(self, model_dir):
        # Seed an artifact, then mutilate the DB to look pre-VG-250.
        record_version(model_dir, "view", "v_legacy", "name: v\n")
        self._drop_owner_columns_and_remove_migration(model_dir)

        # Any subsequent _connect re-runs the migration → columns added,
        # and the new ``get_owner`` query works without error.
        owner = get_owner(model_dir, "view", "v_legacy")
        assert owner is not None
        assert owner["created_by"] is None       # legacy row has no owner
        assert owner["created_via"] is None
        assert owner["created_at"]               # original timestamp survived
