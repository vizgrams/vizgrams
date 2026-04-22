# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for schema management: type inference, json_path, table creation."""

import pytest

from core.db import SQLiteBackend
from core.types import ColumnDef, OutputConfig, RowSource, WriteMode
from engine.schema import ensure_table, extract_json_path, infer_sqlite_type

# --- infer_sqlite_type ---

def test_infer_int():
    assert infer_sqlite_type(42) == "INTEGER"


def test_infer_float():
    assert infer_sqlite_type(3.14) == "REAL"


def test_infer_bool_as_integer():
    assert infer_sqlite_type(True) == "INTEGER"


def test_infer_str():
    assert infer_sqlite_type("hello") == "TEXT"


def test_infer_dict():
    assert infer_sqlite_type({"a": 1}) == "TEXT"


def test_infer_list():
    assert infer_sqlite_type([1, 2]) == "TEXT"


def test_infer_none():
    assert infer_sqlite_type(None) == "TEXT"


# --- extract_json_path ---

def test_simple_path():
    record = {"id": 42, "name": "Test"}
    assert extract_json_path(record, "$.id") == 42
    assert extract_json_path(record, "$.name") == "Test"


def test_nested_path():
    record = {"fields": {"status": {"name": "Done"}}}
    assert extract_json_path(record, "$.fields.status.name") == "Done"


def test_missing_key_returns_none():
    record = {"a": 1}
    assert extract_json_path(record, "$.b") is None


def test_missing_nested_key_returns_none():
    record = {"a": {"b": 1}}
    assert extract_json_path(record, "$.a.c") is None
    assert extract_json_path(record, "$.x.y.z") is None


def test_dict_value_serialized():
    record = {"changelog": {"histories": [{"id": 1}]}}
    result = extract_json_path(record, "$.changelog")
    assert result == '{"histories": [{"id": 1}]}'


def test_list_value_serialized():
    record = {"tags": ["a", "b"]}
    result = extract_json_path(record, "$.tags")
    assert result == '["a", "b"]'


def test_dict_value_raw():
    record = {"changelog": {"histories": [{"id": 1}]}}
    result = extract_json_path(record, "$.changelog", serialize=False)
    assert result == {"histories": [{"id": 1}]}


def test_list_value_raw():
    record = {"tags": ["a", "b"]}
    result = extract_json_path(record, "$.tags", serialize=False)
    assert result == ["a", "b"]


def test_invalid_path_prefix():
    with pytest.raises(ValueError, match="must start with"):
        extract_json_path({}, "id")


# --- ensure_table ---

@pytest.fixture
def db():
    backend = SQLiteBackend()
    backend.connect()
    yield backend
    backend.close()


def test_ensure_table_creates_upsert(db):
    output = OutputConfig(
        table="things",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="name", json_path="$.name"),
        ],
    )
    records = [{"id": 1, "name": "Alice"}]
    table = ensure_table(output, [], records, db)

    assert table == "things"
    assert db.table_exists("things")
    cols = db.get_columns("things")
    assert "id" in cols
    assert "name" in cols
    assert "inserted_at" not in cols  # UPSERT has no inserted_at


def test_ensure_table_creates_append_with_inserted_at(db):
    output = OutputConfig(
        table="events",
        write_mode=WriteMode.APPEND,
        primary_keys=[],
        columns=[ColumnDef(name="event", json_path="$.event")],
    )
    records = [{"event": "deploy"}]
    table = ensure_table(output, [], records, db)

    assert table == "events"
    cols = db.get_columns("events")
    assert "inserted_at" in cols


def test_ensure_table_adds_new_columns(db):
    output = OutputConfig(
        table="evolving",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    ensure_table(output, [], [{"id": 1}], db)

    # Now add a column
    output.columns.append(ColumnDef(name="extra", json_path="$.extra"))
    ensure_table(output, [], [{"id": 1, "extra": "val"}], db)

    cols = db.get_columns("evolving")
    assert "extra" in cols


def test_ensure_table_type_override(db):
    output = OutputConfig(
        table="typed",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="data", json_path="$.data", type="JSON"),
        ],
    )
    ensure_table(output, [], [{"id": 1, "data": {"key": "val"}}], db)

    # JSON type stored as TEXT in SQLite — verify table was created
    assert db.table_exists("typed")


def test_ensure_table_with_context_columns(db):
    output = OutputConfig(
        table="with_ctx",
        write_mode=WriteMode.UPSERT,
        primary_keys=["board_id", "id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    ensure_table(output, ["board_id"], [{"id": 1}], db)

    cols = db.get_columns("with_ctx")
    assert "board_id" in cols


def test_ensure_table_with_inherit_columns(db):
    output = OutputConfig(
        table="with_inherit",
        write_mode=WriteMode.APPEND,
        row_source=RowSource(mode="EXPLODE", json_path="$.items", inherit={"parent_id": "$.id"}),
        columns=[ColumnDef(name="val", json_path="$.val")],
    )
    ensure_table(output, [], [{"val": "x"}], db)

    cols = db.get_columns("with_inherit")
    assert "parent_id" in cols
    assert "val" in cols
    assert "inserted_at" in cols


def test_ensure_table_empty_records(db):
    output = OutputConfig(
        table="empty",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    _table = ensure_table(output, [], [], db)
    assert db.table_exists("empty")
