# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the DB abstraction layer."""

import pytest

from core.db import SQLiteBackend


@pytest.fixture
def db():
    backend = SQLiteBackend()  # in-memory
    backend.connect()
    yield backend
    backend.close()


def test_connect_and_close(db):
    assert db.conn is not None
    db.close()
    assert db.conn is None


def test_create_table_and_exists(db):
    assert not db.table_exists("test_table")
    db.create_table(
        "test_table",
        {"id": "INTEGER", "name": "TEXT"},
        primary_keys=["id"],
    )
    assert db.table_exists("test_table")


def test_get_columns(db):
    db.create_table(
        "test_table",
        {"id": "INTEGER", "name": "TEXT", "value": "REAL"},
        primary_keys=["id"],
    )
    cols = db.get_columns("test_table")
    assert cols == ["id", "name", "value"]


def test_add_columns(db):
    db.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    db.add_columns("test_table", {"extra": "TEXT", "score": "REAL"})
    cols = db.get_columns("test_table")
    assert "extra" in cols
    assert "score" in cols


def test_upsert_insert(db):
    db.create_table(
        "dim_table",
        {"id": "INTEGER", "name": "TEXT"},
        primary_keys=["id"],
    )
    db.upsert("dim_table", {"id": 1, "name": "Alice"})
    rows = db.execute("SELECT * FROM dim_table")
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_upsert_update(db):
    db.create_table(
        "dim_table",
        {"id": "INTEGER", "name": "TEXT"},
        primary_keys=["id"],
    )
    db.upsert("dim_table", {"id": 1, "name": "Alice"})
    db.upsert("dim_table", {"id": 1, "name": "Bob"})
    rows = db.execute("SELECT * FROM dim_table")
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_upsert_serializes_dicts(db):
    db.create_table(
        "dim_table",
        {"id": "INTEGER", "data": "TEXT"},
        primary_keys=["id"],
    )
    db.upsert("dim_table", {"id": 1, "data": {"key": "value"}})
    rows = db.execute("SELECT data FROM dim_table")
    assert rows[0]["data"] == '{"key": "value"}'


def test_append_adds_inserted_at(db):
    db.create_table(
        "fact_table",
        {"event": "TEXT", "inserted_at": "TEXT"},
        primary_keys=[],
    )
    db.append("fact_table", {"event": "deploy"})
    rows = db.execute("SELECT * FROM fact_table")
    assert len(rows) == 1
    assert rows[0]["inserted_at"] is not None
    assert "T" in rows[0]["inserted_at"]  # ISO 8601


def test_append_allows_duplicates(db):
    db.create_table(
        "fact_table",
        {"event": "TEXT", "inserted_at": "TEXT"},
        primary_keys=[],
    )
    db.append("fact_table", {"event": "deploy"})
    db.append("fact_table", {"event": "deploy"})
    rows = db.execute("SELECT * FROM fact_table")
    assert len(rows) == 2


def test_execute_returns_rows(db):
    db.create_table("t", {"id": "INTEGER", "v": "TEXT"}, primary_keys=["id"])
    db.upsert("t", {"id": 1, "v": "a"})
    db.upsert("t", {"id": 2, "v": "b"})
    rows = db.execute("SELECT v FROM t ORDER BY id")
    assert [r["v"] for r in rows] == ["a", "b"]


# --- _task_runs meta table ---

def test_ensure_meta_table(db):
    assert not db.table_exists("_task_runs")
    db.ensure_meta_table()
    assert db.table_exists("_task_runs")


def test_ensure_meta_table_idempotent(db):
    db.ensure_meta_table()
    db.ensure_meta_table()  # Should not raise
    assert db.table_exists("_task_runs")


def test_get_last_run_no_runs(db):
    db.ensure_meta_table()
    assert db.get_last_run("nonexistent_task") is None


def test_record_and_get_last_run(db):
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 100, "success")
    assert db.get_last_run("my_task") == "2025-01-01T00:00:00Z"


def test_get_last_run_returns_max(db):
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 100, "success")
    db.record_run("my_task", "2025-02-01T00:00:00Z", "2025-02-01T00:05:00Z", 50, "success")
    assert db.get_last_run("my_task") == "2025-02-01T00:00:00Z"


def test_get_last_run_ignores_partial(db):
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 100, "success")
    db.record_run("my_task", "2025-02-01T00:00:00Z", "2025-02-01T00:05:00Z", 50, "partial")
    assert db.get_last_run("my_task") == "2025-01-01T00:00:00Z"


def test_get_last_run_isolates_tasks(db):
    db.ensure_meta_table()
    db.record_run("task_a", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 10, "success")
    db.record_run("task_b", "2025-02-01T00:00:00Z", "2025-02-01T00:05:00Z", 20, "success")
    assert db.get_last_run("task_a") == "2025-01-01T00:00:00Z"
    assert db.get_last_run("task_b") == "2025-02-01T00:00:00Z"


# --- param_key support ---

def test_record_run_with_param_key(db):
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 10, "success", param_key="board_id=1")
    db.record_run("my_task", "2025-02-01T00:00:00Z", "2025-02-01T00:05:00Z", 20, "success", param_key="board_id=2")
    assert db.get_last_run("my_task", param_key="board_id=1") == "2025-01-01T00:00:00Z"
    assert db.get_last_run("my_task", param_key="board_id=2") == "2025-02-01T00:00:00Z"


def test_get_last_run_param_key_none_ignores_param_runs(db):
    """Task-level get_last_run (param_key=None) should not see param_key runs."""
    db.ensure_meta_table()
    db.record_run("my_task", "2025-03-01T00:00:00Z", "2025-03-01T00:05:00Z", 5, "success", param_key="board_id=1")
    assert db.get_last_run("my_task") is None  # No task-level run recorded


def test_get_last_run_param_key_isolates_from_task_level(db):
    """Param-key lookups should not see task-level (param_key=NULL) runs."""
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 50, "success")
    assert db.get_last_run("my_task", param_key="board_id=99") is None


def test_ensure_meta_table_migrates_param_key(db):
    """Calling ensure_meta_table on an old schema should add param_key."""
    db.ensure_meta_table()
    cols = db.get_columns("_task_runs")
    assert "param_key" in cols
