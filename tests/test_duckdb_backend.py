# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for DuckDBBackend (Phase 1 of the CH→DuckDB migration).

Unlike ClickHouse, DuckDB runs in-process — no external server is needed.
The duckdb package is a required dependency, so these tests run as part of
the standard suite.
"""

from pathlib import Path

import pytest

duckdb = pytest.importorskip(
    "duckdb",
    reason="duckdb not installed (poetry install)",
)

from core.db import DuckDBBackend  # noqa: E402 — after importorskip

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db(tmp_path: Path) -> DuckDBBackend:
    backend = DuckDBBackend(db_path=tmp_path / "test.duckdb")
    backend.connect()
    yield backend
    backend.close()


# ---------------------------------------------------------------------------
# connect / close
# ---------------------------------------------------------------------------

def test_connect_close(tmp_path: Path):
    b = DuckDBBackend(db_path=tmp_path / "x.duckdb")
    b.connect()
    assert b._conn is not None
    b.close()
    assert b._conn is None


def test_connect_creates_parent_dir(tmp_path: Path):
    b = DuckDBBackend(db_path=tmp_path / "nested" / "deep" / "x.duckdb")
    b.connect()
    assert (tmp_path / "nested" / "deep").is_dir()
    b.close()


def test_in_memory(tmp_path: Path):
    b = DuckDBBackend(db_path=None)
    b.connect()
    b.execute("CREATE TABLE t (id INTEGER)")
    b.execute("INSERT INTO t VALUES (1)")
    assert b.execute("SELECT * FROM t") == [[1]]
    b.close()


# ---------------------------------------------------------------------------
# create_table / table_exists / get_columns
# ---------------------------------------------------------------------------

def test_create_table_and_table_exists(db: DuckDBBackend):
    assert not db.table_exists("widgets")
    db.create_table("widgets", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    assert db.table_exists("widgets")


def test_create_table_is_idempotent(db: DuckDBBackend):
    db.create_table("widgets", {"id": "INTEGER"}, primary_keys=["id"])
    db.create_table("widgets", {"id": "INTEGER"}, primary_keys=["id"])  # IF NOT EXISTS
    assert db.table_exists("widgets")


def test_create_table_maps_generic_types_to_duckdb(db: DuckDBBackend):
    db.create_table(
        "widgets",
        {"id": "INTEGER", "name": "TEXT", "score": "FLOAT"},
        primary_keys=["id"],
    )
    cols = db.get_columns("widgets")
    assert cols == ["id", "name", "score"]


def test_create_table_caches_primary_keys(db: DuckDBBackend):
    db.create_table("widgets", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    assert db._pk_cache["widgets"] == ["id"]


def test_create_table_composite_pk(db: DuckDBBackend):
    db.create_table(
        "memberships",
        {"person_key": "TEXT", "team_key": "TEXT", "active": "BOOLEAN"},
        primary_keys=["person_key", "team_key"],
    )
    assert db._pk_cache["memberships"] == ["person_key", "team_key"]


# ---------------------------------------------------------------------------
# add_columns
# ---------------------------------------------------------------------------

def test_add_columns(db: DuckDBBackend):
    db.create_table("widgets", {"id": "INTEGER"}, primary_keys=["id"])
    db.add_columns("widgets", {"extra": "TEXT", "amount": "FLOAT"})
    cols = db.get_columns("widgets")
    assert "extra" in cols
    assert "amount" in cols


def test_add_columns_is_idempotent(db: DuckDBBackend):
    db.create_table("widgets", {"id": "INTEGER"}, primary_keys=["id"])
    db.add_columns("widgets", {"extra": "TEXT"})
    db.add_columns("widgets", {"extra": "TEXT"})  # already exists, no-op
    cols = db.get_columns("widgets")
    assert cols.count("extra") == 1


# ---------------------------------------------------------------------------
# upsert (insert + conflict resolution)
# ---------------------------------------------------------------------------

def test_upsert_inserts_new_row(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1, "name": "Alice"})
    rows = db.execute("SELECT id, name FROM dim")
    assert rows == [[1, "Alice"]]


def test_upsert_updates_on_pk_conflict(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1, "name": "Alice"})
    db.upsert("dim", {"id": 1, "name": "Bob"})
    rows = db.execute("SELECT id, name FROM dim")
    assert rows == [[1, "Bob"]]


def test_upsert_serializes_dict_to_json(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "meta": "TEXT"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1, "meta": {"key": "val"}})
    rows = db.execute("SELECT meta FROM dim")
    assert rows[0][0] == '{"key": "val"}'


def test_upsert_composite_pk(db: DuckDBBackend):
    db.create_table(
        "memberships",
        {"person_key": "TEXT", "team_key": "TEXT", "is_primary": "INTEGER"},
        primary_keys=["person_key", "team_key"],
    )
    db.upsert("memberships", {"person_key": "p1", "team_key": "t1", "is_primary": 1})
    db.upsert("memberships", {"person_key": "p1", "team_key": "t1", "is_primary": 0})
    rows = db.execute("SELECT person_key, team_key, is_primary FROM memberships")
    assert rows == [["p1", "t1", 0]]


def test_upsert_no_pk_falls_back_to_plain_insert(db: DuckDBBackend):
    db._conn.execute("CREATE TABLE log (event VARCHAR)")
    db.upsert("log", {"event": "click"})
    db.upsert("log", {"event": "click"})  # duplicate allowed — no PK
    rows = db.execute("SELECT event FROM log")
    assert rows == [["click"], ["click"]]


def test_upsert_pk_only_columns_do_nothing(db: DuckDBBackend):
    db.create_table("seen", {"key": "TEXT"}, primary_keys=["key"])
    db.upsert("seen", {"key": "x"})
    db.upsert("seen", {"key": "x"})  # conflict, but nothing to update
    rows = db.execute("SELECT key FROM seen")
    assert rows == [["x"]]


# ---------------------------------------------------------------------------
# append (fact insert with inserted_at)
# ---------------------------------------------------------------------------

def test_append_inserts_with_timestamp(db: DuckDBBackend):
    db.create_table(
        "facts",
        {"id": "INTEGER", "event": "TEXT", "inserted_at": "TEXT"},
        primary_keys=["id"],
    )
    db.append("facts", {"id": 1, "event": "click"})
    rows = db.execute("SELECT id, event, inserted_at FROM facts")
    assert len(rows) == 1
    assert rows[0][1] == "click"
    assert rows[0][2] is not None and rows[0][2].startswith("20")


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------

def test_truncate_removes_all_rows(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1})
    db.upsert("dim", {"id": 2})
    db.truncate("dim")
    assert db.execute("SELECT COUNT(*) FROM dim") == [[0]]


# ---------------------------------------------------------------------------
# execute / last_columns
# ---------------------------------------------------------------------------

def test_last_columns_populated_for_select(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1, "name": "x"})
    db.execute("SELECT id, name FROM dim")
    assert db.last_columns == ["id", "name"]


def test_last_columns_empty_for_ddl(db: DuckDBBackend):
    db.execute("CREATE TABLE t (id INTEGER)")
    assert db.last_columns == []


def test_execute_with_positional_params(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.upsert("dim", {"id": 1, "name": "Alice"})
    db.upsert("dim", {"id": 2, "name": "Bob"})
    rows = db.execute("SELECT name FROM dim WHERE id = ?", [2])
    assert rows == [["Bob"]]


# ---------------------------------------------------------------------------
# _task_runs metadata
# ---------------------------------------------------------------------------

def test_ensure_meta_table_creates_task_runs(db: DuckDBBackend):
    db.ensure_meta_table()
    assert db.table_exists("_task_runs")
    cols = db.get_columns("_task_runs")
    assert {"task_name", "started_at", "completed_at", "record_count", "status", "param_key"}.issubset(cols)


def test_ensure_meta_table_is_idempotent(db: DuckDBBackend):
    db.ensure_meta_table()
    db.ensure_meta_table()  # second call must not error
    assert db.table_exists("_task_runs")


def test_get_last_run_returns_max_started_at(db: DuckDBBackend):
    db.ensure_meta_table()
    db.record_run("ingest", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 5, "success")
    db.record_run("ingest", "2026-01-02T00:00:00", "2026-01-02T00:01:00", 7, "success")
    assert db.get_last_run("ingest") == "2026-01-02T00:00:00"


def test_get_last_run_filters_by_status(db: DuckDBBackend):
    db.ensure_meta_table()
    db.record_run("ingest", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 0, "failed")
    db.record_run("ingest", "2026-01-02T00:00:00", "2026-01-02T00:01:00", 5, "success")
    assert db.get_last_run("ingest") == "2026-01-02T00:00:00"


def test_get_last_run_with_param_key(db: DuckDBBackend):
    db.ensure_meta_table()
    db.record_run("extract", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 3, "success", param_key="us")
    db.record_run("extract", "2026-01-02T00:00:00", "2026-01-02T00:01:00", 4, "success", param_key="eu")
    assert db.get_last_run("extract", param_key="us") == "2026-01-01T00:00:00"
    assert db.get_last_run("extract", param_key="eu") == "2026-01-02T00:00:00"
    assert db.get_last_run("extract") is None  # no NULL-param-key rows


def test_get_last_run_unknown_task_is_none(db: DuckDBBackend):
    db.ensure_meta_table()
    assert db.get_last_run("never_ran") is None


# ---------------------------------------------------------------------------
# get_backend factory wiring
# ---------------------------------------------------------------------------

def test_get_backend_returns_duckdb(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("VZ_DATABASE_BACKEND", "duckdb")
    (tmp_path / "config.yaml").write_text("models: []\n")
    from core.db import get_backend  # noqa: PLC0415
    backend = get_backend(tmp_path)
    try:
        assert isinstance(backend, DuckDBBackend)
        assert backend.db_path.endswith("data/data.duckdb")
    finally:
        if backend._conn is not None:
            backend.close()
