# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the artifact certification surface (VG-258)."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.metadata_db import (
    CERTIFIABLE_TYPES,
    certify,
    get_certification,
    is_certified,
    list_certifications,
    record_version,
    uncertify,
)


@pytest.fixture
def model_dir(tmp_path) -> Path:
    d = tmp_path / "demo_model"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Single-artifact CRUD
# ---------------------------------------------------------------------------


class TestCertifyAndUncertify:
    def test_uncertified_artifact_returns_none(self, model_dir):
        assert get_certification(model_dir, "view", "missing") is None
        assert is_certified(model_dir, "view", "missing") is False

    def test_certify_then_lookup(self, model_dir):
        certify(model_dir, "view", "dora_clt", user_id="user-123")
        assert is_certified(model_dir, "view", "dora_clt")
        c = get_certification(model_dir, "view", "dora_clt")
        assert c is not None
        assert c["certified_by"] == "user-123"
        assert c["certified_at"]  # ISO timestamp, non-empty

    def test_certify_is_idempotent_and_refreshes_timestamp(self, model_dir):
        certify(model_dir, "view", "v", user_id="alice")
        first = get_certification(model_dir, "view", "v")
        certify(model_dir, "view", "v", user_id="bob")
        second = get_certification(model_dir, "view", "v")
        # Same row, new certifier — no duplicate entries created
        assert second["certified_by"] == "bob"
        # Listing returns one row, not two
        assert len(list_certifications(model_dir, "view")) == 1
        assert second["certified_at"] >= first["certified_at"]

    def test_uncertify_removes_row(self, model_dir):
        certify(model_dir, "query", "q1", user_id="u")
        assert uncertify(model_dir, "query", "q1") is True
        assert is_certified(model_dir, "query", "q1") is False

    def test_uncertify_on_missing_row_returns_false(self, model_dir):
        assert uncertify(model_dir, "query", "never_existed") is False

    def test_user_id_optional_for_system_marks(self, model_dir):
        certify(model_dir, "feature", "f", user_id=None)
        c = get_certification(model_dir, "feature", "f")
        assert c["certified_by"] is None


class TestCertifiableTypes:
    def test_certifying_non_certifiable_type_raises(self, model_dir):
        for t in ("entity", "mapper", "extractor", "application"):
            with pytest.raises(ValueError, match="Cannot certify"):
                certify(model_dir, t, "x")

    def test_certifiable_types_is_what_we_expect(self):
        assert {"view", "query", "feature"} == CERTIFIABLE_TYPES


# ---------------------------------------------------------------------------
# Batched lookup
# ---------------------------------------------------------------------------


class TestListCertifications:
    def test_empty_model_returns_empty_dict(self, model_dir):
        assert list_certifications(model_dir) == {}

    def test_returns_all_types_when_unfiltered(self, model_dir):
        certify(model_dir, "view", "v1", user_id="u")
        certify(model_dir, "query", "q1", user_id="u")
        certify(model_dir, "feature", "f1", user_id="u")
        out = list_certifications(model_dir)
        assert set(out.keys()) == {("view", "v1"), ("query", "q1"), ("feature", "f1")}

    def test_filters_by_type(self, model_dir):
        certify(model_dir, "view", "v1", user_id="u")
        certify(model_dir, "query", "q1", user_id="u")
        out = list_certifications(model_dir, "view")
        assert set(out.keys()) == {("view", "v1")}

    def test_other_model_is_isolated(self, model_dir, tmp_path):
        other = tmp_path / "other_model"
        other.mkdir()
        certify(model_dir, "view", "v", user_id="u")
        assert list_certifications(other) == {}


# ---------------------------------------------------------------------------
# Backfill migration (one-shot)
# ---------------------------------------------------------------------------


class TestBackfillExisting:
    def _reset_migration(self) -> None:
        """Simulate "the migration has never run on this DB."

        In production the migration runs once on the first connect after the
        new code lands; this helper recreates that scenario in tests, since
        the test DB starts fresh and the migration runs as a no-op on the
        very first ``_connect``.
        """
        import sqlite3

        from core.metadata_db import get_api_db_path
        conn = sqlite3.connect(str(get_api_db_path()))
        conn.execute("DELETE FROM schema_migrations WHERE name='backfill_certify_existing'")
        conn.commit()
        conn.close()

    def test_existing_artifacts_certified_when_migration_runs(self, model_dir):
        record_version(model_dir, "view", "pre_existing_view", "name: v\n")
        record_version(model_dir, "query", "pre_existing_query", "name: q\n")
        record_version(model_dir, "feature", "pre_existing_feature", "name: f\n")
        # Pretend the migration hasn't been applied to this DB yet, then
        # trigger any operation that goes through ``_connect`` to re-run it.
        self._reset_migration()
        list_certifications(model_dir)  # forces a connect

        out = list_certifications(model_dir)
        assert ("view", "pre_existing_view") in out
        assert ("query", "pre_existing_query") in out
        assert ("feature", "pre_existing_feature") in out
        # Backfill leaves certified_by NULL (system mark, no user attribution).
        assert out[("view", "pre_existing_view")]["certified_by"] is None

    def test_entities_and_mappers_are_not_backfilled(self, model_dir):
        record_version(model_dir, "entity", "Person", "name: Person\n")
        record_version(model_dir, "mapper", "person_mapper", "rows: []\n")
        self._reset_migration()
        list_certifications(model_dir)

        out = list_certifications(model_dir)
        assert ("entity", "Person") not in out
        assert ("mapper", "person_mapper") not in out

    def test_migration_runs_only_once(self, model_dir):
        """After the migration has been applied, new artifacts must not get
        auto-certified — only the first run's snapshot is treated as legacy.
        """
        record_version(model_dir, "view", "pre_existing", "name: v\n")
        self._reset_migration()
        list_certifications(model_dir)  # first run: backfills pre_existing

        record_version(model_dir, "view", "after_migration", "name: v\n")
        # second connect: migration already applied → no-op
        assert is_certified(model_dir, "view", "pre_existing")
        assert not is_certified(model_dir, "view", "after_migration")
