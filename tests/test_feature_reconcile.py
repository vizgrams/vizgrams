# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic.feature reconcile engine (ADR-113)."""

import sqlite3
import textwrap
from pathlib import Path

import pytest

from semantic.feature import FeatureDef, load_feature_yamls, reconcile, reconcile_with_backend
from core.db import SQLiteBackend
from semantic.types import AttributeDef, ColumnType, EntityDef, SemanticHint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'test.db'}"


def _db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


def _connect(tmp_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(_db_path(tmp_path))


def _simple_feature(feature_id: str = "widget.score", definition: str | None = None) -> FeatureDef:
    """Return a minimal valid FeatureDef for testing."""
    if definition is None:
        definition = (
            "SELECT widget_key AS entity_id, value AS value FROM widget"
        )
    return FeatureDef(
        feature_id=feature_id,
        name="Widget Score",
        entity_type="Widget",
        entity_key="widget_key",
        data_type="FLOAT",
        materialization_mode="materialized",
        raw_sql=definition,
    )


def _make_widget_table(conn: sqlite3.Connection) -> None:
    """Create a minimal widget table for testing."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS widget "
        "(widget_key TEXT PRIMARY KEY, value REAL)"
    )
    conn.commit()


def _simple_entity(name: str = "Widget") -> EntityDef:
    return EntityDef(
        name=name,
        identity=[AttributeDef("widget_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[AttributeDef("value", ColumnType.FLOAT)],
    )


# ---------------------------------------------------------------------------
# test_creates_tables
# ---------------------------------------------------------------------------

class TestCreatesTables:
    def test_tables_created_on_first_run(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()

        assert "__attribute_registry" in tables
        assert "__feature_definition" in tables
        assert "__feature_value" in tables

    def test_idempotent_second_run(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))
        reconcile([fd], {}, _db_url(tmp_path))  # second run must not raise

        conn = _connect(tmp_path)
        rows = conn.execute("SELECT COUNT(*) FROM __feature_definition").fetchone()
        conn.close()
        assert rows[0] == 1  # only one definition row


# ---------------------------------------------------------------------------
# test_upserts_feature_definition
# ---------------------------------------------------------------------------

class TestUpsertsFeatureDefinition:
    def test_new_feature_written(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        row = conn.execute(
            "SELECT feature_id, name, version FROM __feature_definition WHERE feature_id=?",
            (fd.feature_id,),
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] == "widget.score"
        assert row[2] == 1  # initial version

    def test_changed_definition_bumps_version(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd_v1 = _simple_feature(definition="SELECT widget_key AS entity_id, value AS value FROM widget")
        reconcile([fd_v1], {}, _db_url(tmp_path))

        fd_v2 = _simple_feature(
            definition="SELECT widget_key AS entity_id, CAST(value * 2 AS REAL) AS value FROM widget"
        )
        reconcile([fd_v2], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        row = conn.execute(
            "SELECT version FROM __feature_definition WHERE feature_id=?",
            (fd_v1.feature_id,),
        ).fetchone()
        conn.close()

        assert row[0] == 2  # bumped

    def test_unchanged_definition_keeps_version(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))
        reconcile([fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        row = conn.execute(
            "SELECT version FROM __feature_definition WHERE feature_id=?",
            (fd.feature_id,),
        ).fetchone()
        conn.close()

        assert row[0] == 1  # unchanged


# ---------------------------------------------------------------------------
# test_recomputes_values
# ---------------------------------------------------------------------------

class TestRecomputesValues:
    def test_feature_value_populated(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.execute("INSERT INTO widget (widget_key, value) VALUES ('w1', 3.5)")
        conn.execute("INSERT INTO widget (widget_key, value) VALUES ('w2', 7.0)")
        conn.commit()
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT entity_id, value FROM __feature_value WHERE feature_id=?",
                (fd.feature_id,),
            )
        }
        conn.close()

        assert rows["w1"] == "3.5"
        assert rows["w2"] == "7.0"

    def test_null_values_stored_as_null(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.execute("INSERT INTO widget (widget_key, value) VALUES ('w1', NULL)")
        conn.commit()
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        row = conn.execute(
            "SELECT value FROM __feature_value WHERE feature_id=? AND entity_id=?",
            (fd.feature_id, "w1"),
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] is None


# ---------------------------------------------------------------------------
# test_dry_run_no_writes
# ---------------------------------------------------------------------------

class TestDryRunNoWrites:
    def test_dry_run_leaves_db_empty(self, tmp_path, capsys):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.execute("INSERT INTO widget (widget_key, value) VALUES ('w1', 1.0)")
        conn.commit()
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path), dry_run=True)

        conn = _connect(tmp_path)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()

        # __attribute_registry, feature_definition, feature_value are created but empty
        assert "__feature_value" in tables
        row = sqlite3.connect(_db_path(tmp_path)).execute(
            "SELECT COUNT(*) FROM __feature_value"
        ).fetchone()
        assert row[0] == 0

        out = capsys.readouterr().out
        assert "dry-run" in out.lower()

    def test_dry_run_prints_new_feature(self, tmp_path, capsys):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        fd = _simple_feature()
        reconcile([fd], {}, _db_url(tmp_path), dry_run=True)

        out = capsys.readouterr().out
        assert "NEW" in out
        assert "widget.score" in out


# ---------------------------------------------------------------------------
# test_dependency_ordering
# ---------------------------------------------------------------------------

class TestDependencyOrdering:
    def test_dependent_feature_computed_after_dependency(self, tmp_path):
        """fc_to_pr_open_hours depends on first_commit; execution order must respect this."""
        conn = _connect(tmp_path)
        conn.execute("""
            CREATE TABLE pr (
                pr_key TEXT PRIMARY KEY,
                created_at TEXT,
                first_ts TEXT
            )
        """)
        conn.execute(
            "INSERT INTO pr VALUES ('pr1', '2024-01-10T10:00:00', '2024-01-08T08:00:00')"
        )
        conn.commit()
        conn.close()

        fd_base = FeatureDef(
            feature_id="pr.first_ts",
            name="First TS",
            entity_type="Pr",
            entity_key="pr_key",
            data_type="STRING",
            materialization_mode="materialized",
            raw_sql="SELECT pr_key AS entity_id, first_ts AS value FROM pr",
        )
        fd_dep = FeatureDef(
            feature_id="pr.hours",
            name="Hours",
            entity_type="Pr",
            entity_key="pr_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            dependencies=["pr.first_ts"],
            raw_sql="SELECT pr_key AS entity_id, 99.0 AS value FROM pr",
        )

        # Pass in reverse order; topo sort should handle it
        reconcile([fd_dep, fd_base], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        fids = [
            r[0]
            for r in conn.execute("SELECT DISTINCT feature_id FROM __feature_value")
        ]
        conn.close()

        assert "pr.first_ts" in fids
        assert "pr.hours" in fids


# ---------------------------------------------------------------------------
# test_abort_on_invalid_sql
# ---------------------------------------------------------------------------

class TestAbortOnInvalidSql:
    def test_abort_before_any_writes(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.execute("INSERT INTO widget (widget_key, value) VALUES ('w1', 1.0)")
        conn.commit()
        conn.close()

        bad_fd = FeatureDef(
            feature_id="widget.bad",
            name="Bad",
            entity_type="Widget",
            entity_key="widget_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            raw_sql=(
                "SELECT widget_key AS entity_id, value AS value "
                "FROM widget ORDER BY widget_key"
            ),
        )

        with pytest.raises(ValueError, match="ORDER BY"):
            reconcile([bad_fd], {}, _db_url(tmp_path))

        conn = _connect(tmp_path)
        # Tables are created (step 1 runs), but feature_definition must be empty
        count = conn.execute("SELECT COUNT(*) FROM __feature_definition").fetchone()[0]
        conn.close()
        assert count == 0

    def test_abort_on_missing_entity_id_column(self, tmp_path):
        conn = _connect(tmp_path)
        _make_widget_table(conn)
        conn.close()

        bad_fd = FeatureDef(
            feature_id="widget.bad",
            name="Bad",
            entity_type="Widget",
            entity_key="widget_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            raw_sql="SELECT widget_key AS id, value AS value FROM widget",
        )

        with pytest.raises(ValueError, match="entity_id"):
            reconcile([bad_fd], {}, _db_url(tmp_path))


# ---------------------------------------------------------------------------
# Phase 2: definition: vs raw_sql: YAML key equivalence
# ---------------------------------------------------------------------------

_WIDGET_SQL = "SELECT widget_key AS entity_id, value AS value FROM widget"

_FEATURE_YAML_TEMPLATE = textwrap.dedent("""\
    feature_id: widget.score
    name: Widget Score
    entity_type: Widget
    entity_key: widget_key
    data_type: FLOAT
    materialization_mode: materialized
    {key}: |
      {sql}
""")


class TestRawSqlKeyEquivalence:
    """Both 'definition:' and 'raw_sql:' YAML keys must load identically."""

    def test_definition_key_loads(self, tmp_path):
        feat_dir = tmp_path / "feat"
        feat_dir.mkdir()
        (feat_dir / "feature.yaml").write_text(
            _FEATURE_YAML_TEMPLATE.format(key="definition", sql=_WIDGET_SQL)
        )
        fds = load_feature_yamls(feat_dir)
        assert len(fds) == 1
        fd = fds[0]
        assert fd.feature_type == "raw_sql"
        assert _WIDGET_SQL in fd.raw_sql

    def test_raw_sql_key_loads(self, tmp_path):
        feat_dir = tmp_path / "feat"
        feat_dir.mkdir()
        path = feat_dir / "feature.yaml"
        path.write_text(_FEATURE_YAML_TEMPLATE.format(key="raw_sql", sql=_WIDGET_SQL))
        fds = load_feature_yamls(feat_dir)
        assert len(fds) == 1
        fd = fds[0]
        assert fd.feature_type == "raw_sql"
        assert _WIDGET_SQL in fd.raw_sql

    def test_both_keys_produce_identical_raw_sql(self, tmp_path):
        def_dir = tmp_path / "def"
        raw_dir = tmp_path / "raw"
        def_dir.mkdir()
        raw_dir.mkdir()
        (def_dir / "w.yaml").write_text(
            _FEATURE_YAML_TEMPLATE.format(key="definition", sql=_WIDGET_SQL)
        )
        (raw_dir / "w.yaml").write_text(
            _FEATURE_YAML_TEMPLATE.format(key="raw_sql", sql=_WIDGET_SQL)
        )
        fd_def = load_feature_yamls(def_dir)[0]
        fd_raw = load_feature_yamls(raw_dir)[0]
        assert fd_def.raw_sql.strip() == fd_raw.raw_sql.strip()

    def test_definition_key_reconciles(self, tmp_path):
        feat_dir = tmp_path / "feat"
        feat_dir.mkdir()
        (feat_dir / "w.yaml").write_text(
            _FEATURE_YAML_TEMPLATE.format(key="definition", sql=_WIDGET_SQL)
        )
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        conn.execute("CREATE TABLE widget (widget_key TEXT PRIMARY KEY, value REAL)")
        conn.execute("INSERT INTO widget VALUES ('w1', 1.0)")
        conn.commit()
        conn.close()

        fds = load_feature_yamls(feat_dir)
        reconcile(fds, {}, f"sqlite:///{tmp_path / 'test.db'}")

        conn = sqlite3.connect(str(tmp_path / "test.db"))
        row = conn.execute(
            "SELECT value FROM __feature_value WHERE feature_id='widget.score' AND entity_id='w1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "1.0"


# ---------------------------------------------------------------------------
# Tests for reconcile_with_backend (backend-agnostic path)
# ---------------------------------------------------------------------------

def _make_backend_with_widget(seed_rows: list[tuple] | None = None) -> SQLiteBackend:
    """Return a connected in-memory SQLiteBackend with a seeded widget table."""
    backend = SQLiteBackend()  # in-memory
    backend.connect()
    backend.execute(
        "CREATE TABLE IF NOT EXISTS widget (widget_key TEXT PRIMARY KEY, value REAL)"
    )
    for key, val in (seed_rows or []):
        backend.execute(
            "INSERT INTO widget (widget_key, value) VALUES (?, ?)", (key, val)
        )
    return backend


class TestReconcileWithBackend:
    """reconcile_with_backend mirrors reconcile() but uses any DBBackend."""

    def test_creates_feature_tables(self):
        backend = _make_backend_with_widget()
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend)
        backend.close()

        # reconnect and verify tables were created
        b2 = SQLiteBackend(":memory:")
        # Tables are in-memory so we verify via the backend we used
        backend2 = _make_backend_with_widget()
        reconcile_with_backend([fd], {}, backend2)
        tables_rows = backend2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {r[0] for r in tables_rows}
        backend2.close()
        assert "__feature_definition" in tables
        assert "__feature_value" in tables
        assert "__attribute_registry" in tables

    def test_computes_feature_values(self):
        backend = _make_backend_with_widget([("w1", 3.5), ("w2", 7.0)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend)

        rows = backend.execute(
            "SELECT entity_id, value FROM __feature_value WHERE feature_id=?",
            (fd.feature_id,),
        )
        result = {r[0]: r[1] for r in rows}
        backend.close()

        assert result["w1"] == "3.5"
        assert result["w2"] == "7.0"

    def test_null_values_stored_as_null(self):
        backend = _make_backend_with_widget([("w1", None)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend)

        rows = backend.execute(
            "SELECT value FROM __feature_value WHERE feature_id=? AND entity_id=?",
            (fd.feature_id, "w1"),
        )
        backend.close()
        assert rows[0][0] is None

    def test_idempotent_second_run(self):
        backend = _make_backend_with_widget([("w1", 1.0)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend)
        reconcile_with_backend([fd], {}, backend)  # second run must not raise

        rows = backend.execute("SELECT COUNT(*) FROM __feature_definition")
        backend.close()
        assert rows[0][0] == 1

    def test_changed_definition_bumps_version(self):
        backend = _make_backend_with_widget([("w1", 1.0)])
        fd_v1 = _simple_feature(
            definition="SELECT widget_key AS entity_id, value AS value FROM widget"
        )
        reconcile_with_backend([fd_v1], {}, backend)

        fd_v2 = _simple_feature(
            definition=(
                "SELECT widget_key AS entity_id, CAST(value * 2 AS REAL) AS value FROM widget"
            )
        )
        reconcile_with_backend([fd_v2], {}, backend)

        rows = backend.execute(
            "SELECT version FROM __feature_definition WHERE feature_id=?",
            (fd_v1.feature_id,),
        )
        backend.close()
        assert rows[0][0] == 2

    def test_unchanged_definition_keeps_version(self):
        backend = _make_backend_with_widget([("w1", 1.0)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend)
        reconcile_with_backend([fd], {}, backend)

        rows = backend.execute(
            "SELECT version FROM __feature_definition WHERE feature_id=?",
            (fd.feature_id,),
        )
        backend.close()
        assert rows[0][0] == 1

    def test_dry_run_no_data_written(self, capsys):
        backend = _make_backend_with_widget([("w1", 1.0)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend, dry_run=True)

        rows = backend.execute("SELECT COUNT(*) FROM __feature_value")
        backend.close()
        assert rows[0][0] == 0
        out = capsys.readouterr().out
        assert "dry-run" in out.lower()

    def test_dry_run_prints_new_feature(self, capsys):
        backend = _make_backend_with_widget()
        fd = _simple_feature()
        reconcile_with_backend([fd], {}, backend, dry_run=True)
        backend.close()

        out = capsys.readouterr().out
        assert "NEW" in out
        assert "widget.score" in out

    def test_abort_on_invalid_sql(self):
        backend = _make_backend_with_widget([("w1", 1.0)])
        bad_fd = FeatureDef(
            feature_id="widget.bad",
            name="Bad",
            entity_type="Widget",
            entity_key="widget_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            raw_sql=(
                "SELECT widget_key AS entity_id, value AS value "
                "FROM widget ORDER BY widget_key"
            ),
        )
        with pytest.raises(ValueError, match="ORDER BY"):
            reconcile_with_backend([bad_fd], {}, backend)

        rows = backend.execute("SELECT COUNT(*) FROM __feature_definition")
        backend.close()
        assert rows[0][0] == 0

    def test___attribute_registry_populated(self):
        entity = _simple_entity()
        backend = _make_backend_with_widget([("w1", 1.0)])
        fd = _simple_feature()
        reconcile_with_backend([fd], {"Widget": entity}, backend)

        rows = backend.execute(
            "SELECT attribute_name, source_type FROM __attribute_registry"
        )
        registry = {r[0]: r[1] for r in rows}
        backend.close()

        assert "score" in registry
        assert registry["score"] == "feature"
        assert "widget_key" in registry
        assert registry["widget_key"] == "column"
