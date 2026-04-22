# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Schema management: type inference, json_path extraction, table creation."""

import json

from core.db import DBBackend
from core.types import OutputConfig, WriteMode


def infer_sqlite_type(value) -> str:
    """Map a Python value to a SQLite column type."""
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


def extract_json_path(record, path: str, *, serialize: bool = True):
    """Walk a dot-notation path ($.foo.bar) into a nested dict.

    Special path "$" returns the record itself (useful when a tool yields
    a top-level list as a single item — EXPLODE mode with json_path: $).

    Returns the value at that path, or None if any key is missing.
    When serialize=True (default), serializes dict/list values to JSON strings.
    When serialize=False, returns raw Python objects.
    """
    if path == "$":
        if serialize and isinstance(record, (dict, list)):
            return json.dumps(record)
        return record
    if not path.startswith("$."):
        raise ValueError(f"json_path must start with '$.' — got {path!r}")
    keys = path[2:].split(".")
    current = record
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    if serialize and isinstance(current, (dict, list)):
        return json.dumps(current)
    return current


def _resolve_column_type(col, sample_value) -> str:
    """Determine column type from explicit override or inference."""
    if col.type:
        # JSON type is stored as TEXT in SQLite
        return "TEXT" if col.type == "JSON" else col.type
    if sample_value is not None:
        return infer_sqlite_type(sample_value)
    return "TEXT"


def ensure_table(
    output: OutputConfig, context_col_names: list[str],
    records: list[dict], db: DBBackend,
) -> str:
    """Create or update a table for the given output.

    Returns the table name.
    For EXPLODE mode, records should contain sample elements (not parent records).
    """
    table_name = output.table

    # Build column type map from first record + column defs
    col_types: dict[str, str] = {}
    sample = records[0] if records else {}

    # Add context columns first
    for ctx_col in context_col_names:
        col_types[ctx_col] = "TEXT"

    # Add inherit columns (from row_source)
    if output.row_source and output.row_source.inherit:
        for col_name in output.row_source.inherit:
            col_types[col_name] = "TEXT"

    # Add defined columns
    for col in output.columns:
        sample_value = extract_json_path(sample, col.json_path)
        col_types[col.name] = _resolve_column_type(col, sample_value)

    # Add inserted_at for APPEND tables
    if output.write_mode == WriteMode.APPEND:
        col_types["inserted_at"] = "TEXT"

    if not db.table_exists(table_name):
        db.create_table(table_name, col_types, output.primary_keys)
    else:
        # Additive schema evolution: add any new columns
        existing = set(db.get_columns(table_name))
        new_cols = {k: v for k, v in col_types.items() if k not in existing}
        if new_cols:
            db.add_columns(table_name, new_cols)

    return table_name
