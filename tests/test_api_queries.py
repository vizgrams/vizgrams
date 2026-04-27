# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/query_service.py."""

import pytest

from api.services.query_service import (
    execute_query,
    get_query,
    list_queries,
    validate_query,
)

# ---------------------------------------------------------------------------
# Minimal YAML fixtures
# ---------------------------------------------------------------------------

_WIDGET_ENTITY_YAML = """\
entity: Widget

identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  score:
    type: FLOAT
    semantic: MEASURE
"""

_WIDGET_QUERY_YAML = """\
name: widget_totals
root: Widget

measures:
  total_score:
    expr: sum(score)
  widget_count:
    expr: count(widget_key)
"""

_WIDGET_QUERY_WITH_GROUPBY_YAML = """\
name: widget_by_score
root: Widget

group_by:
  - score

measures:
  count:
    expr: count(widget_key)
"""


def _configure_ch(model_dir, ch_backend, monkeypatch):
    """Monkeypatch load_database_config to return the ch_backend test database."""
    monkeypatch.setattr(
        "core.model_config.load_database_config",
        lambda md: {
            "backend": "clickhouse", "host": "localhost", "port": 8123,
            "database": ch_backend.database, "username": "default", "password": "",
            "raw_database": f"{ch_backend.database}_raw",
            "sem_database": ch_backend.database,
        },
    )


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "config.yaml").write_text("")
    from tests.conftest import seed_artifact
    seed_artifact(tmp_path, "entity", "Widget", _WIDGET_ENTITY_YAML)
    return tmp_path


@pytest.fixture
def model_dir_with_query(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "query", "widget_totals", _WIDGET_QUERY_YAML)
    return model_dir


@pytest.fixture
def model_dir_with_db(model_dir_with_query, ch_backend, monkeypatch):
    """Model dir with a ClickHouse-backed populated DB for execute_query tests."""
    _configure_ch(model_dir_with_query, ch_backend, monkeypatch)
    ch_backend.create_table(
        "widget",
        {"widget_key": "String", "score": "float"},
        primary_keys=["widget_key"],
    )
    ch_backend.bulk_upsert("widget", [
        {"widget_key": "w1", "score": 10.0},
        {"widget_key": "w2", "score": 20.0},
        {"widget_key": "w3", "score": 30.0},
    ])
    return model_dir_with_query


# ---------------------------------------------------------------------------
# list_queries
# ---------------------------------------------------------------------------

def test_list_queries_empty_when_no_dir(tmp_path):
    result = list_queries(tmp_path)
    assert result == []


def test_list_queries_returns_names(model_dir_with_query):
    result = list_queries(model_dir_with_query)
    names = [q["name"] for q in result]
    assert "widget_totals" in names


def test_list_queries_multiple(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "query", "widget_totals", _WIDGET_QUERY_YAML)
    seed_artifact(model_dir, "query", "widget_by_score", _WIDGET_QUERY_WITH_GROUPBY_YAML)
    result = list_queries(model_dir)
    assert len(result) == 2


def test_list_queries_includes_root_and_measure_count(model_dir_with_query):
    result = list_queries(model_dir_with_query)
    q = result[0]
    assert q["root"] == "Widget"
    assert q["measure_count"] == 2


# ---------------------------------------------------------------------------
# get_query
# ---------------------------------------------------------------------------

def test_get_query_returns_expected_keys(model_dir_with_query):
    result = get_query(model_dir_with_query, "widget_totals")
    assert result["name"] == "widget_totals"
    assert result["root"] == "Widget"
    assert "measures" in result
    assert "group_by" in result
    assert "compiled_sql" in result


def test_get_query_compiled_sql_is_string_or_none(model_dir_with_query):
    result = get_query(model_dir_with_query, "widget_totals")
    # compiled_sql may be None if compilation fails, but should not raise
    assert result["compiled_sql"] is None or isinstance(result["compiled_sql"], str)


def test_get_query_raises_key_error_when_not_found(model_dir):
    with pytest.raises(KeyError):
        get_query(model_dir, "nonexistent")


# ---------------------------------------------------------------------------
# validate_query
# ---------------------------------------------------------------------------

def test_validate_query_returns_valid_field(model_dir_with_query):
    result = validate_query(model_dir_with_query, "widget_totals")
    assert "valid" in result
    assert "errors" in result


def test_validate_query_raises_key_error_when_not_found(model_dir):
    with pytest.raises(KeyError):
        validate_query(model_dir, "nonexistent")


# ---------------------------------------------------------------------------
# execute_query
# ---------------------------------------------------------------------------

def test_execute_query_raises_file_not_found_when_no_db(model_dir_with_query, ch_backend, monkeypatch):
    _configure_ch(model_dir_with_query, ch_backend, monkeypatch)
    with pytest.raises(FileNotFoundError):
        execute_query(model_dir_with_query, "widget_totals")


def test_execute_query_returns_result_shape(model_dir_with_db):
    result = execute_query(model_dir_with_db, "widget_totals")
    assert "columns" in result
    assert "rows" in result
    assert "row_count" in result
    assert "total_row_count" in result
    assert "duration_ms" in result
    assert "sql" in result


def test_execute_query_pagination(model_dir_with_db):
    result = execute_query(model_dir_with_db, "widget_totals", limit=1, offset=0)
    assert result["row_count"] <= 1


def test_execute_query_raises_key_error_when_query_not_found(model_dir_with_db):
    with pytest.raises(KeyError):
        execute_query(model_dir_with_db, "nonexistent")


def test_execute_query_raises_value_error_with_cause_on_bad_query(model_dir):
    """Compilation errors must propagate with the root cause, not swallowed."""
    # Reference an entity that doesn't exist — guaranteed compilation failure
    bad_yaml = """\
name: bad_query
root: NonExistentEntity

measures:
  cnt:
    expr: count(id)
"""
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "query", "bad_query", bad_yaml)
    with pytest.raises(ValueError, match="bad_query") as exc_info:
        execute_query(model_dir, "bad_query")
    # The error message must include the underlying cause, not just the query name
    assert "NonExistentEntity" in str(exc_info.value) or exc_info.value.__cause__ is not None


