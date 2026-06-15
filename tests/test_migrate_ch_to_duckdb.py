# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for tools/migrate_ch_to_duckdb.py — orchestration only.

The migration core takes any object with a ``query`` + ``query_arrow``
API, so we exercise it against a tiny FakeCHClient backed by real
pyarrow tables. A live ClickHouse end-to-end is impractical in CI; the
unit-level tests cover the orchestration logic (table discovery,
per-table copy, parity, name collisions, filters).
"""

from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")

from tools.migrate_ch_to_duckdb import (  # noqa: E402
    TableReport,
    _list_user_tables,
    _migrate_one_table,
    migrate,
)

# ---------------------------------------------------------------------------
# Fake CH client — minimum surface the migration uses
# ---------------------------------------------------------------------------


class _Result:
    def __init__(self, rows: list[list]):
        self.result_rows = rows


class FakeCHClient:
    """Tiny stand-in. Holds a {(db, table): pyarrow.Table} catalog and a
    list of system tables to return for ``system.tables`` lookups.
    """
    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], pa.Table] = {}

    def add(self, db: str, name: str, arrow_table: pa.Table) -> None:
        self.tables[(db, name)] = arrow_table

    def query(self, sql: str) -> _Result:
        # Two query patterns are issued by the migration code:
        # 1. SHOW-style: SELECT name FROM system.tables WHERE database = '...'
        # 2. COUNT: SELECT count() / countIf(...) FROM `db`.`table` ...
        if "system.tables" in sql:
            # Parse "WHERE database = 'X'" out of the SQL.
            db = sql.split("WHERE database = '", 1)[1].split("'", 1)[0]
            names = sorted(t for (d, t) in self.tables if d == db)
            return _Result([[n] for n in names])
        if "countIf(" in sql:
            # countIf(col IS NULL) — extract col and table
            col = sql.split("countIf(", 1)[1].split(" IS NULL", 1)[0]
            db, tbl = self._parse_db_table(sql)
            tbl_data = self.tables[(db, tbl)]
            null_count = tbl_data[col].null_count
            return _Result([[null_count]])
        if "count()" in sql:
            db, tbl = self._parse_db_table(sql)
            return _Result([[self.tables[(db, tbl)].num_rows]])
        raise ValueError(f"unhandled query: {sql}")

    def query_arrow(self, sql: str) -> pa.Table:
        db, tbl = self._parse_db_table(sql)
        return self.tables[(db, tbl)]

    def close(self) -> None:
        pass

    @staticmethod
    def _parse_db_table(sql: str) -> tuple[str, str]:
        # Format: `db`.`table` somewhere in the SQL.
        i = sql.index("`")
        db, rest = sql[i + 1:].split("`.`", 1)
        tbl = rest.split("`", 1)[0]
        return db, tbl


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def duck_conn(tmp_path: Path):
    con = duckdb.connect(str(tmp_path / "target.duckdb"))
    yield con
    con.close()


@pytest.fixture()
def ch():
    return FakeCHClient()


# ---------------------------------------------------------------------------
# _list_user_tables
# ---------------------------------------------------------------------------


def test_list_user_tables_returns_sorted_names(ch: FakeCHClient):
    ch.add("iagai", "team", pa.table({"team_key": ["a"]}))
    ch.add("iagai", "person", pa.table({"person_key": ["p1"]}))
    assert _list_user_tables(ch, "iagai") == ["person", "team"]


def test_list_user_tables_scopes_by_database(ch: FakeCHClient):
    ch.add("iagai", "person", pa.table({"k": ["a"]}))
    ch.add("iagai_raw", "jira_users", pa.table({"k": ["a"]}))
    assert _list_user_tables(ch, "iagai") == ["person"]
    assert _list_user_tables(ch, "iagai_raw") == ["jira_users"]


# ---------------------------------------------------------------------------
# _migrate_one_table — happy path + null parity
# ---------------------------------------------------------------------------


def test_migrate_one_table_copies_rows_and_columns(ch: FakeCHClient, duck_conn):
    src = pa.table({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    })
    ch.add("iagai", "widget", src)
    report = _migrate_one_table(
        ch_client=ch, duck=duck_conn, ch_database="iagai",
        table="widget", check_nulls=True,
    )
    assert report.parity_ok
    assert report.ch_row_count == 3
    assert report.duck_row_count == 3
    rows = duck_conn.execute("SELECT id, name FROM widget ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_migrate_one_table_drops_stale_target(ch: FakeCHClient, duck_conn):
    duck_conn.execute("CREATE TABLE widget (legacy INTEGER)")
    duck_conn.execute("INSERT INTO widget VALUES (99)")
    ch.add("iagai", "widget", pa.table({"id": [1]}))
    report = _migrate_one_table(
        ch_client=ch, duck=duck_conn, ch_database="iagai",
        table="widget", check_nulls=False,
    )
    assert report.parity_ok
    cols = [c[0] for c in duck_conn.execute("SELECT * FROM widget LIMIT 0").description]
    assert "legacy" not in cols
    assert "id" in cols


def test_migrate_one_table_records_load_errors(ch: FakeCHClient, duck_conn):
    # No table → query_arrow KeyError surfaces as report.error
    report = _migrate_one_table(
        ch_client=ch, duck=duck_conn, ch_database="iagai",
        table="nope", check_nulls=False,
    )
    assert not report.parity_ok
    assert report.error is not None


def test_migrate_one_table_null_warnings_only_when_count_differs(
    ch: FakeCHClient, duck_conn,
):
    # Source has 1 null per column. After zero-copy ingest into DuckDB
    # the null count must match; no warning expected.
    src = pa.table({
        "id": pa.array([1, 2, None]),
        "name": pa.array(["a", None, "c"]),
    })
    ch.add("iagai", "widget", src)
    report = _migrate_one_table(
        ch_client=ch, duck=duck_conn, ch_database="iagai",
        table="widget", check_nulls=True,
    )
    assert report.parity_ok
    assert report.null_warnings == []


# ---------------------------------------------------------------------------
# migrate() orchestration
# ---------------------------------------------------------------------------


def test_migrate_walks_sem_then_raw(ch: FakeCHClient, duck_conn):
    ch.add("iagai", "person", pa.table({"k": ["p1"]}))
    ch.add("iagai_raw", "jira_users", pa.table({"k": ["u1"]}))
    reports = migrate(
        ch_client=ch, duck=duck_conn,
        ch_sem_database="iagai", ch_raw_database="iagai_raw",
        check_nulls=False,
    )
    assert [(r.source_db, r.table) for r in reports] == [
        ("iagai", "person"),
        ("iagai_raw", "jira_users"),
    ]
    assert all(r.parity_ok for r in reports)


def test_migrate_only_tables_filter(ch: FakeCHClient, duck_conn):
    ch.add("iagai", "person", pa.table({"k": ["p1"]}))
    ch.add("iagai", "team", pa.table({"k": ["t1"]}))
    reports = migrate(
        ch_client=ch, duck=duck_conn,
        ch_sem_database="iagai", ch_raw_database="iagai_raw",
        only_tables={"team"}, check_nulls=False,
    )
    assert [r.table for r in reports] == ["team"]


def test_migrate_detects_name_collision_across_dbs(ch: FakeCHClient, duck_conn):
    # A table named the same in both CH databases — second occurrence is
    # flagged as a collision rather than clobbering the first.
    ch.add("iagai", "_task_runs", pa.table({"k": ["sem-1"]}))
    ch.add("iagai_raw", "_task_runs", pa.table({"k": ["raw-1", "raw-2"]}))
    reports = migrate(
        ch_client=ch, duck=duck_conn,
        ch_sem_database="iagai", ch_raw_database="iagai_raw",
        check_nulls=False,
    )
    assert len(reports) == 2
    assert reports[0].parity_ok
    assert not reports[1].parity_ok
    assert "name collision" in (reports[1].error or "")


# ---------------------------------------------------------------------------
# TableReport.parity_ok semantics
# ---------------------------------------------------------------------------


def test_table_report_parity_ok_false_on_row_mismatch():
    r = TableReport(table="x", source_db="db", ch_row_count=10, duck_row_count=9)
    assert r.parity_ok is False


def test_table_report_parity_ok_true_when_counts_match_and_no_error():
    r = TableReport(table="x", source_db="db", ch_row_count=10, duck_row_count=10)
    assert r.parity_ok is True
