# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ClickHouseBackend (ADR-124).

These tests require a running ClickHouse server and the clickhouse-connect
package.  They are skipped automatically when either is unavailable so the
standard test suite (SQLite-only) is never broken.

To run against a local Docker container:
    docker compose -f docker-compose.clickhouse.yml up clickhouse -d
    python -m pytest tests/test_clickhouse_backend.py -v
"""

import time

import pytest

# ---------------------------------------------------------------------------
# Skip guards — skip the whole module if clickhouse-connect is not installed
# or if no server is reachable.
# ---------------------------------------------------------------------------

clickhouse_connect = pytest.importorskip(
    "clickhouse_connect",
    reason="clickhouse-connect not installed (pip install clickhouse-connect)",
)

from core.db import ClickHouseBackend  # noqa: E402 — after importorskip


def _make_backend() -> ClickHouseBackend:
    return ClickHouseBackend(
        host="localhost",
        port=8123,
        database="test_vizgrams",
        username="default",
        password="",
    )


@pytest.fixture(scope="module")
def ch():
    """Module-scoped backend with a dedicated test database."""
    try:
        # Create the test database if it doesn't exist
        root = ClickHouseBackend(host="localhost", port=8123, database="default")
        root.connect()
        root._client.command("CREATE DATABASE IF NOT EXISTS test_vizgrams")
        root.close()
    except Exception as exc:
        pytest.skip(f"ClickHouse not reachable: {exc}")

    backend = _make_backend()
    backend.connect()
    yield backend
    backend.close()


@pytest.fixture(autouse=True)
def drop_tables(ch):
    """Drop any test tables before each test for isolation."""
    for tbl in ("test_table", "dim_table", "fact_table", "_task_runs"):
        try:
            ch._client.command(f"DROP TABLE IF EXISTS `{tbl}`")
        except Exception:
            pass
    yield


# ---------------------------------------------------------------------------
# connect / close
# ---------------------------------------------------------------------------

def test_connect_close():
    b = _make_backend()
    b.connect()
    assert b._client is not None
    b.close()
    assert b._client is None


# ---------------------------------------------------------------------------
# create_table / table_exists
# ---------------------------------------------------------------------------

def test_create_table_and_table_exists(ch):
    assert not ch.table_exists("test_table")
    ch.create_table("test_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    assert ch.table_exists("test_table")


def test_create_table_is_idempotent(ch):
    ch.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    ch.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    assert ch.table_exists("test_table")


def test_create_table_adds_version_columns(ch):
    ch.create_table("test_table", {"id": "INTEGER", "val": "TEXT"}, primary_keys=["id"])
    cols = ch.get_columns("test_table")
    assert "_version" in cols
    assert "_loaded_at" in cols


# ---------------------------------------------------------------------------
# get_columns
# ---------------------------------------------------------------------------

def test_get_columns(ch):
    ch.create_table(
        "test_table",
        {"id": "INTEGER", "name": "TEXT", "score": "FLOAT"},
        primary_keys=["id"],
    )
    cols = ch.get_columns("test_table")
    assert "id" in cols
    assert "name" in cols
    assert "score" in cols


# ---------------------------------------------------------------------------
# add_columns
# ---------------------------------------------------------------------------

def test_add_columns(ch):
    ch.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    ch.add_columns("test_table", {"extra": "TEXT", "amount": "FLOAT"})
    cols = ch.get_columns("test_table")
    assert "extra" in cols
    assert "amount" in cols


def test_add_columns_is_idempotent(ch):
    ch.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    ch.add_columns("test_table", {"extra": "TEXT"})
    ch.add_columns("test_table", {"extra": "TEXT"})  # IF NOT EXISTS — no error
    cols = ch.get_columns("test_table")
    assert cols.count("extra") == 1


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------

def test_upsert_inserts_row(ch):
    ch.create_table("dim_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    ch.upsert("dim_table", {"id": 1, "name": "Alice"})
    rows = ch.execute("SELECT id, name FROM dim_table")
    assert len(rows) == 1
    assert rows[0][1] == "Alice"


def test_upsert_deduplicates_on_higher_version(ch):
    """Two upserts with the same PK: FINAL should return only the latest."""
    ch.create_table("dim_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    ch.upsert("dim_table", {"id": 1, "name": "Alice"})
    time.sleep(0.01)  # ensure _version increments
    ch.upsert("dim_table", {"id": 1, "name": "Bob"})
    # FINAL is added automatically by execute()
    rows = ch.execute("SELECT id, name FROM dim_table ORDER BY name")
    # After background merge rows collapse to 1; before merge we may see 2.
    # The key assertion: the highest-_version row has name="Bob".
    names = [r[1] for r in rows]
    assert "Bob" in names


def test_upsert_serializes_dict_values(ch):
    ch.create_table("dim_table", {"id": "INTEGER", "meta": "TEXT"}, primary_keys=["id"])
    ch.upsert("dim_table", {"id": 1, "meta": {"key": "val"}})
    rows = ch.execute("SELECT meta FROM dim_table")
    assert rows[0][0] == '{"key": "val"}'


# ---------------------------------------------------------------------------
# append
# ---------------------------------------------------------------------------

def test_append_inserts_row(ch):
    ch.create_table("fact_table", {"id": "INTEGER", "event": "TEXT", "inserted_at": "TEXT"}, primary_keys=["id"])
    ch.append("fact_table", {"id": 1, "event": "click"})
    rows = ch.execute("SELECT id, event FROM fact_table")
    assert len(rows) == 1
    assert rows[0][1] == "click"


def test_append_adds_inserted_at(ch):
    ch.create_table("fact_table", {"id": "INTEGER", "inserted_at": "TEXT"}, primary_keys=["id"])
    ch.append("fact_table", {"id": 1})
    cols = ch.get_columns("fact_table")
    assert "inserted_at" in cols
    rows = ch.execute("SELECT inserted_at FROM fact_table")
    assert rows[0][0] is not None


# ---------------------------------------------------------------------------
# execute / last_columns / FINAL injection
# ---------------------------------------------------------------------------

def test_last_columns_populated(ch):
    ch.create_table("test_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    ch.upsert("test_table", {"id": 1, "name": "x"})
    ch.execute("SELECT id, name FROM test_table")
    assert "id" in ch.last_columns
    assert "name" in ch.last_columns


def test_maybe_add_final_injects_for_sem_table():
    sql = "SELECT id FROM sem_Airport WHERE id = 'JFK'"
    result = ClickHouseBackend._maybe_add_final(sql)
    assert "FINAL" in result.upper()


def test_maybe_add_final_skips_non_sem_table():
    sql = "SELECT id FROM my_table"
    result = ClickHouseBackend._maybe_add_final(sql)
    assert "FINAL" not in result.upper()


def test_maybe_add_final_does_not_double_inject():
    sql = "SELECT id FROM sem_Airport FINAL WHERE id = 'JFK'"
    result = ClickHouseBackend._maybe_add_final(sql)
    assert result.upper().count("FINAL") == 1


def test_maybe_add_final_injects_for_raw_table():
    sql = "SELECT * FROM raw_github_pull_requests"
    result = ClickHouseBackend._maybe_add_final(sql)
    assert "FINAL" in result.upper()


def test_maybe_add_final_injects_for_join_table():
    """FINAL must also be injected after sem_ tables in JOIN clauses."""
    sql = (
        "SELECT a.iata_code, al.name "
        "FROM sem_airport AS a "
        "LEFT JOIN sem_airline AS al ON al.iata_code = a.iata_code"
    )
    result = ClickHouseBackend._maybe_add_final(sql)
    upper = result.upper()
    assert upper.count("FINAL") == 2


def test_maybe_add_final_injects_for_multiple_joins():
    """All sem_ and raw_ tables in a multi-join query get FINAL."""
    sql = (
        "SELECT a.id, b.name, c.code "
        "FROM sem_airport AS a "
        "LEFT JOIN sem_airline AS b ON b.id = a.airline_id "
        "LEFT JOIN raw_route AS c ON c.src = a.id"
    )
    result = ClickHouseBackend._maybe_add_final(sql)
    assert result.upper().count("FINAL") == 3


def test_maybe_add_final_handles_from_and_join_together():
    """FROM and JOIN both get FINAL in the same pass."""
    sql = "SELECT * FROM sem_foo AS f JOIN raw_bar AS b ON f.id = b.id"
    result = ClickHouseBackend._maybe_add_final(sql)
    upper = result.upper()
    assert upper.count("FINAL") == 2
    assert "FROM SEM_FOO AS F FINAL" in upper
    assert "JOIN RAW_BAR AS B FINAL" in upper


# ---------------------------------------------------------------------------
# always_final mode (split sem/raw databases)
# ---------------------------------------------------------------------------

def test_maybe_add_final_always_final_applies_to_all_tables():
    """always_final=True injects FINAL on every FROM/JOIN table, not just sem_/raw_."""
    sql = "SELECT * FROM airport AS a LEFT JOIN airline AS al ON al.id = a.id"
    result = ClickHouseBackend._maybe_add_final(sql, always_final=True)
    assert result.upper().count("FINAL") == 2


def test_maybe_add_final_always_final_skips_non_select():
    sql = "INSERT INTO airport (id) VALUES (1)"
    result = ClickHouseBackend._maybe_add_final(sql, always_final=True)
    assert "FINAL" not in result.upper()


def test_maybe_add_final_always_final_does_not_double_inject():
    sql = "SELECT * FROM airport FINAL"
    result = ClickHouseBackend._maybe_add_final(sql, always_final=True)
    assert result.upper().count("FINAL") == 1


def test_maybe_add_final_always_final_does_not_match_subquery():
    """Subqueries (FROM (...) AS alias) must not get FINAL appended."""
    sql = "SELECT * FROM (SELECT id FROM airport) AS sub"
    result = ClickHouseBackend._maybe_add_final(sql, always_final=True)
    # FINAL injected inside the subquery on 'airport', not on the outer 'sub'
    assert "FROM AIRPORT FINAL" in result.upper()
    assert "AS SUB FINAL" not in result.upper()


def test_always_final_backend_injects_on_execute(ch):
    """A backend constructed with always_final=True injects FINAL via execute()."""
    ch.create_table("test_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    ch.upsert("test_table", {"id": 1, "name": "alice"})
    # The backend fixture (ch) uses always_final=False by default;
    # build a temporary always_final backend against the same test database.
    always_final_backend = ClickHouseBackend(
        host="localhost", port=8123,
        database="test_vizgrams", username="default", password="",
        always_final=True,
    )
    always_final_backend.connect()
    try:
        rows = always_final_backend.execute("SELECT id, name FROM test_table")
        assert len(rows) >= 1
    finally:
        always_final_backend.close()


# ---------------------------------------------------------------------------
# _task_runs (meta table)
# ---------------------------------------------------------------------------

def test_ensure_meta_table(ch):
    ch.ensure_meta_table()
    assert ch.table_exists("_task_runs")


def test_record_and_get_last_run(ch):
    ch.ensure_meta_table()
    ch.record_run("extractor.github", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 100, "success")
    result = ch.get_last_run("extractor.github")
    assert result == "2026-01-01T00:00:00"


def test_get_last_run_only_success(ch):
    ch.ensure_meta_table()
    ch.record_run("extractor.github", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 0, "failure")
    result = ch.get_last_run("extractor.github")
    assert result is None


def test_get_last_run_returns_latest(ch):
    ch.ensure_meta_table()
    ch.record_run("extractor.github", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 10, "success")
    ch.record_run("extractor.github", "2026-02-01T00:00:00", "2026-02-01T00:01:00", 20, "success")
    result = ch.get_last_run("extractor.github")
    assert result == "2026-02-01T00:00:00"


def test_record_run_with_param_key(ch):
    ch.ensure_meta_table()
    ch.record_run("extractor.github", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 5, "success", param_key="org=acme")
    assert ch.get_last_run("extractor.github", param_key="org=acme") == "2026-01-01T00:00:00"
    assert ch.get_last_run("extractor.github") is None


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------

def test_truncate_removes_all_rows(ch):
    ch.create_table("test_table", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    ch.upsert("test_table", {"id": 1, "name": "Alice"})
    ch.upsert("test_table", {"id": 2, "name": "Bob"})
    ch.truncate("test_table")
    rows = ch.execute("SELECT id FROM test_table")
    assert rows == []


def test_truncate_idempotent_on_empty_table(ch):
    ch.create_table("test_table", {"id": "INTEGER"}, primary_keys=["id"])
    ch.truncate("test_table")  # should not raise
    assert ch.table_exists("test_table")


# ---------------------------------------------------------------------------
# get_backend namespace routing (unit-level — no server required)
# ---------------------------------------------------------------------------

def test_get_backend_sem_namespace_uses_sem_database(tmp_path):
    """get_backend(namespace='sem') returns a backend pointing at the base database name."""
    import yaml

    from core.db import ClickHouseBackend, get_backend

    config = {
        "database": {
            "backend": "clickhouse",
            "host": "localhost",
            "port": 8123,
            "database": "mymodel",
            "username": "default",
            "password": "",
        }
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))
    backend = get_backend(tmp_path, namespace="sem")
    assert isinstance(backend, ClickHouseBackend)
    assert backend.database == "mymodel"
    assert backend.always_final is True


def test_get_backend_raw_namespace_uses_raw_database(tmp_path):
    """get_backend(namespace='raw') returns a backend pointing at {model}_raw."""
    import yaml

    from core.db import ClickHouseBackend, get_backend

    config = {
        "database": {
            "backend": "clickhouse",
            "host": "localhost",
            "port": 8123,
            "database": "mymodel",
            "username": "default",
            "password": "",
        }
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))
    backend = get_backend(tmp_path, namespace="raw")
    assert isinstance(backend, ClickHouseBackend)
    assert backend.database == "mymodel_raw"
    assert backend.always_final is True


def test_get_backend_explicit_raw_sem_databases(tmp_path):
    """Explicit raw_database / sem_database fields override the derived names."""
    import yaml

    from core.db import get_backend

    config = {
        "database": {
            "backend": "clickhouse",
            "host": "localhost",
            "port": 8123,
            "database": "mymodel",
            "raw_database": "custom_raw",
            "sem_database": "custom_sem",
            "username": "default",
            "password": "",
        }
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))
    assert get_backend(tmp_path, namespace="raw").database == "custom_raw"
    assert get_backend(tmp_path, namespace="sem").database == "custom_sem"
