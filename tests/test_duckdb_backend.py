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


def test_create_table_existing_pkless_table_caches_real_state(db: DuckDBBackend):
    # CH→DuckDB migration leaves meta tables (__feature_definition,
    # __attribute_registry, …) on disk WITHOUT PK constraints. When code
    # later runs ``create_table(..., primary_keys=['feature_id'])`` the
    # CREATE TABLE IF NOT EXISTS is a no-op — but caching the requested PK
    # would poison _upsert_sql into emitting ``ON CONFLICT (feature_id)``
    # which DuckDB rejects with "specified columns as conflict target are
    # not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT". The cache must
    # reflect the actual on-disk state instead.
    db._conn.execute("CREATE TABLE pre_existing (feature_id VARCHAR, name VARCHAR)")
    db.create_table(
        "pre_existing",
        {"feature_id": "VARCHAR", "name": "VARCHAR"},
        primary_keys=["feature_id"],
    )
    assert db._pk_cache["pre_existing"] == []
    # Sanity check: upsert now degrades to plain INSERT rather than the
    # broken ON CONFLICT path.
    db.upsert("pre_existing", {"feature_id": "f1", "name": "first"})
    db.upsert("pre_existing", {"feature_id": "f1", "name": "second"})
    rows = db.execute("SELECT feature_id, name FROM pre_existing ORDER BY name")
    # Both rows land — no PK to dedup. Acceptable behaviour for this
    # situation; the dedup script is responsible for repairing the
    # missing constraint.
    assert rows == [["f1", "first"], ["f1", "second"]]


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


def test_upsert_with_primary_keys_on_pkless_table_dedupes(db: DuckDBBackend):
    # Sister to test_bulk_upsert_with_primary_keys_on_pkless_table_dedupes:
    # the singular upsert() also needs the DELETE+INSERT fallback so the
    # feature reconcile path stops leaking duplicates into PK-less meta
    # tables like __feature_value. Pre-fix this would land 3 rows.
    db._conn.execute("CREATE TABLE __feature_value (feature_id VARCHAR, entity_id VARCHAR, value VARCHAR)")
    db.upsert("__feature_value", {"feature_id": "f1", "entity_id": "e1", "value": "v1"},
              primary_keys=["feature_id", "entity_id"])
    db.upsert("__feature_value", {"feature_id": "f1", "entity_id": "e1", "value": "v2"},
              primary_keys=["feature_id", "entity_id"])
    db.upsert("__feature_value", {"feature_id": "f1", "entity_id": "e1", "value": "v3"},
              primary_keys=["feature_id", "entity_id"])
    rows = db.execute("SELECT value FROM __feature_value")
    assert rows == [["v3"]]


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


# ---------------------------------------------------------------------------
# bulk_upsert (Phase 4)
# ---------------------------------------------------------------------------

def test_bulk_upsert_empty_is_noop(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.bulk_upsert("dim", [])
    assert db.execute("SELECT COUNT(*) FROM dim") == [[0]]


def test_bulk_upsert_inserts_many_rows(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.bulk_upsert("dim", [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Carol"},
    ])
    rows = db.execute("SELECT id, name FROM dim ORDER BY id")
    assert rows == [[1, "Alice"], [2, "Bob"], [3, "Carol"]]


def test_bulk_upsert_updates_existing_keys(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.bulk_upsert("dim", [{"id": 1, "name": "Alice"}])
    db.bulk_upsert("dim", [
        {"id": 1, "name": "Anne"},   # update
        {"id": 2, "name": "Bob"},    # insert
    ])
    rows = db.execute("SELECT id, name FROM dim ORDER BY id")
    assert rows == [[1, "Anne"], [2, "Bob"]]


def test_bulk_upsert_heterogeneous_keys(db: DuckDBBackend):
    """Multi-group mappers emit candidates where one group sets repo_key and
    another sets issue_key. The union of keys is written for every row, with
    NULL filling positions a particular row didn't supply."""
    db.create_table(
        "contribution",
        {
            "id": "TEXT",
            "repository_key": "TEXT",
            "issue_key": "TEXT",
        },
        primary_keys=["id"],
    )
    db.bulk_upsert("contribution", [
        {"id": "c1", "repository_key": "repo-a"},
        {"id": "c2", "issue_key": "ISSUE-9"},
    ])
    rows = db.execute(
        "SELECT id, repository_key, issue_key FROM contribution ORDER BY id"
    )
    assert rows == [["c1", "repo-a", None], ["c2", None, "ISSUE-9"]]


def test_bulk_upsert_serialises_dict_values(db: DuckDBBackend):
    db.create_table("dim", {"id": "INTEGER", "meta": "TEXT"}, primary_keys=["id"])
    db.bulk_upsert("dim", [
        {"id": 1, "meta": {"k": "v"}},
        {"id": 2, "meta": ["a", "b"]},
    ])
    rows = db.execute("SELECT id, meta FROM dim ORDER BY id")
    assert rows == [[1, '{"k": "v"}'], [2, '["a", "b"]']]


def test_bulk_upsert_no_pk_appends(db: DuckDBBackend):
    db._conn.execute("CREATE TABLE log (event TEXT)")
    db.bulk_upsert("log", [{"event": "a"}, {"event": "a"}, {"event": "b"}])
    rows = db.execute("SELECT event FROM log ORDER BY event")
    assert rows == [["a"], ["a"], ["b"]]


def test_bulk_upsert_with_primary_keys_on_pkless_table_dedupes(db: DuckDBBackend):
    # Simulates the post-migration state: the table on disk lacks the
    # YAML-declared PK. Without the DELETE+INSERT fallback every batch would
    # append duplicates instead of upserting.
    db._conn.execute("CREATE TABLE raw_events (id INTEGER, payload TEXT)")
    db.bulk_upsert("raw_events", [
        {"id": 1, "payload": "v1"},
        {"id": 2, "payload": "v2"},
    ], primary_keys=["id"])
    db.bulk_upsert("raw_events", [
        {"id": 1, "payload": "v1-updated"},  # would have been a dupe pre-fix
        {"id": 3, "payload": "v3"},
    ], primary_keys=["id"])
    rows = db.execute("SELECT id, payload FROM raw_events ORDER BY id")
    assert rows == [[1, "v1-updated"], [2, "v2"], [3, "v3"]]


def test_bulk_upsert_pkless_composite_primary_keys(db: DuckDBBackend):
    db._conn.execute("CREATE TABLE raw_x (a INTEGER, b INTEGER, v TEXT)")
    db.bulk_upsert("raw_x", [
        {"a": 1, "b": 1, "v": "first"},
        {"a": 1, "b": 2, "v": "first"},
    ], primary_keys=["a", "b"])
    # (1,1) is in the new batch and should be replaced; (1,2) is left alone.
    db.bulk_upsert("raw_x", [
        {"a": 1, "b": 1, "v": "second"},
    ], primary_keys=["a", "b"])
    rows = db.execute("SELECT a, b, v FROM raw_x ORDER BY a, b")
    assert rows == [[1, 1, "second"], [1, 2, "first"]]


def test_bulk_upsert_with_primary_keys_on_pk_table_uses_on_conflict(db: DuckDBBackend):
    # When the table has the matching PK constraint, we must NOT take the
    # DELETE+INSERT path — ON CONFLICT is cheaper. Pass primary_keys anyway
    # and verify behaviour stays correct (data ends up in the right state).
    db.create_table("dim", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    db.bulk_upsert("dim", [{"id": 1, "name": "Alice"}], primary_keys=["id"])
    db.bulk_upsert("dim", [
        {"id": 1, "name": "Anne"},
        {"id": 2, "name": "Bob"},
    ], primary_keys=["id"])
    rows = db.execute("SELECT id, name FROM dim ORDER BY id")
    assert rows == [[1, "Anne"], [2, "Bob"]]


# ---------------------------------------------------------------------------
# bulk_scd2 (Phase 4)
# ---------------------------------------------------------------------------

class _ScdCtx:
    """Minimal stand-in for engine.mapper._WriteContext for backend tests."""
    def __init__(self, key_col, tracked_cols, managed_cols=None, initial_valid_from=None):
        self.key_col = key_col
        self.tracked_cols = tracked_cols
        self.managed_cols = managed_cols or {"valid_from", "valid_to"}
        self.initial_valid_from = initial_valid_from


def _make_scd2_table(db: DuckDBBackend, table: str = "person") -> None:
    """Create a Person-shaped SCD2 table. Composite PK (key, valid_from)."""
    db.create_table(
        table,
        {
            "person_key": "TEXT",
            "name": "TEXT",
            "team": "TEXT",
            "valid_from": "TEXT",
            "valid_to": "TEXT",
        },
        primary_keys=["person_key", "valid_from"],
    )


def test_bulk_scd2_empty_is_noop(db: DuckDBBackend):
    _make_scd2_table(db)
    ctx = _ScdCtx(key_col="person_key", tracked_cols=["name", "team"])
    assert db.bulk_scd2("person", [], ctx) == (0, 0)


def test_bulk_scd2_inserts_new_rows(db: DuckDBBackend):
    _make_scd2_table(db)
    ctx = _ScdCtx(
        key_col="person_key", tracked_cols=["name", "team"],
        initial_valid_from="2025-01-01",
    )
    new, scd = db.bulk_scd2("person", [
        {"person_key": "p1", "name": "Alice", "team": "platform"},
        {"person_key": "p2", "name": "Bob", "team": "frontend"},
    ], ctx)
    assert (new, scd) == (2, 0)
    rows = db.execute(
        "SELECT person_key, name, team, valid_from, valid_to "
        "FROM person ORDER BY person_key"
    )
    assert rows == [
        ["p1", "Alice", "platform", "2025-01-01", None],
        ["p2", "Bob",   "frontend", "2025-01-01", None],
    ]


def test_bulk_scd2_closes_open_row_and_inserts_new_on_change(db: DuckDBBackend):
    _make_scd2_table(db)
    ctx = _ScdCtx(
        key_col="person_key", tracked_cols=["team"],
        initial_valid_from="2025-01-01",
    )
    db.bulk_scd2("person", [{"person_key": "p1", "name": "Alice", "team": "platform"}], ctx)
    new, scd = db.bulk_scd2("person", [
        {"person_key": "p1", "name": "Alice", "team": "frontend"},  # team changed
    ], ctx)
    assert (new, scd) == (0, 1)
    rows = db.execute(
        "SELECT person_key, team, valid_from, valid_to "
        "FROM person ORDER BY valid_from"
    )
    # Two rows: the closed history row + the new open row.
    assert len(rows) == 2
    assert rows[0][:3] == ["p1", "platform", "2025-01-01"]
    assert rows[0][3] is not None  # valid_to set on closed row
    assert rows[1][1] == "frontend"
    assert rows[1][3] is None      # open row


def test_bulk_scd2_no_op_when_tracked_cols_unchanged(db: DuckDBBackend):
    _make_scd2_table(db)
    ctx = _ScdCtx(
        key_col="person_key", tracked_cols=["team"],
        initial_valid_from="2025-01-01",
    )
    db.bulk_scd2("person", [{"person_key": "p1", "name": "Alice", "team": "platform"}], ctx)
    new, scd = db.bulk_scd2("person", [
        {"person_key": "p1", "name": "Alicia", "team": "platform"},  # name not tracked
    ], ctx)
    assert (new, scd) == (0, 0)
    rows = db.execute("SELECT COUNT(*) FROM person")
    assert rows == [[1]]


def test_bulk_scd2_mixed_batch(db: DuckDBBackend):
    """Insert, update, and no-op in the same batch."""
    _make_scd2_table(db)
    ctx = _ScdCtx(
        key_col="person_key", tracked_cols=["team"],
        initial_valid_from="2025-01-01",
    )
    db.bulk_scd2("person", [
        {"person_key": "p1", "name": "Alice", "team": "platform"},
        {"person_key": "p2", "name": "Bob",   "team": "frontend"},
    ], ctx)
    new, scd = db.bulk_scd2("person", [
        {"person_key": "p1", "name": "Alice", "team": "platform"},   # no-op
        {"person_key": "p2", "name": "Bob",   "team": "infra"},      # change
        {"person_key": "p3", "name": "Carol", "team": "platform"},   # new
    ], ctx)
    assert (new, scd) == (1, 1)
    # Total rows: p1 (1 open), p2 (closed + open = 2), p3 (1 open) = 4
    assert db.execute("SELECT COUNT(*) FROM person") == [[4]]
    # Only one open row per person.
    rows = db.execute(
        "SELECT person_key, team FROM person WHERE valid_to IS NULL "
        "ORDER BY person_key"
    )
    assert rows == [
        ["p1", "platform"],
        ["p2", "infra"],
        ["p3", "platform"],
    ]


def test_bulk_scd2_transaction_rolls_back_on_error(db: DuckDBBackend):
    """If the close + insert atom errors, the table is left at the pre-call
    state. Force an error by trying to UPDATE a constrained column with a
    bad type after the close, simulated by closing the connection mid-call.
    """
    _make_scd2_table(db)
    ctx = _ScdCtx(
        key_col="person_key", tracked_cols=["team"],
        initial_valid_from="2025-01-01",
    )
    db.bulk_scd2("person", [{"person_key": "p1", "name": "Alice", "team": "platform"}], ctx)

    # Sabotage bulk_upsert so the close UPDATE runs but the INSERT raises.
    sentinel = RuntimeError("simulated insert failure")

    def broken_bulk_upsert(table, rows):
        raise sentinel
    db.bulk_upsert = broken_bulk_upsert  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="simulated insert failure"):
        db.bulk_scd2("person", [
            {"person_key": "p1", "name": "Alice", "team": "frontend"},
        ], ctx)

    # The closed row should NOT be visible — transaction rolled back.
    rows = db.execute("SELECT team, valid_to FROM person")
    assert rows == [["platform", None]]


# ---------------------------------------------------------------------------
# S3 plumbing (Phase 6) — local-file round-trip + SQL-shape checks for s3://
#
# Live S3 integration is gated behind VZ_TEST_S3_BUCKET (skipped in CI).
# The local-file tests fully exercise EXPORT/IMPORT DATABASE; the s3://
# tests below verify the SQL we'd send rather than reaching the network.
# ---------------------------------------------------------------------------

def test_enable_s3_installs_and_loads_httpfs(db: DuckDBBackend):
    db.enable_s3()
    # If httpfs is loaded we can call its catalog function without erroring.
    loaded = db.execute(
        "SELECT extension_name FROM duckdb_extensions() "
        "WHERE extension_name = 'httpfs' AND loaded"
    )
    assert loaded == [["httpfs"]]


def test_configure_s3_credentials_credential_chain(db: DuckDBBackend, monkeypatch):
    # DuckDB's credential_chain provider validates at CREATE SECRET time —
    # it tries every link in the chain and fails if none resolves. CI has
    # no AWS creds, so seed fake values for the env_aws link to satisfy.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKEFAKEFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fakesecret/fakesecret/fakesecret")
    db.configure_s3_credentials(
        use_credential_chain=True, region="eu-west-1",
        secret_name="vz_test_chain",
    )
    rows = db.execute(
        "SELECT name, type, provider FROM duckdb_secrets() "
        "WHERE name = 'vz_test_chain'"
    )
    assert rows == [["vz_test_chain", "s3", "credential_chain"]]


def test_configure_s3_credentials_inline(db: DuckDBBackend):
    db.configure_s3_credentials(
        key_id="AKIAFAKE", secret="secret-shh",
        region="us-east-1", secret_name="vz_test_inline",
    )
    rows = db.execute(
        "SELECT name, type FROM duckdb_secrets() WHERE name = 'vz_test_inline'"
    )
    assert rows == [["vz_test_inline", "s3"]]


def test_configure_s3_credentials_requires_key_or_chain(db: DuckDBBackend):
    with pytest.raises(ValueError, match="key_id"):
        db.configure_s3_credentials(region="eu-west-1")


def test_export_then_import_round_trip(tmp_path: Path):
    src = DuckDBBackend(db_path=tmp_path / "src.duckdb")
    src.connect()
    src.create_table("widget", {"id": "INTEGER", "name": "TEXT"}, primary_keys=["id"])
    src.bulk_upsert("widget", [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])
    dump = tmp_path / "dump"
    src.export_database(str(dump))
    src.close()

    # New DB, restore from the export.
    dst = DuckDBBackend(db_path=tmp_path / "dst.duckdb")
    dst.connect()
    try:
        dst.import_database(str(dump))
        rows = dst.execute("SELECT id, name FROM widget ORDER BY id")
        assert rows == [[1, "Alice"], [2, "Bob"]]
    finally:
        dst.close()


def test_attach_readonly_local_file(tmp_path: Path):
    """ATTACH to a local .duckdb file with READ_ONLY semantics."""
    writer = DuckDBBackend(db_path=tmp_path / "writer.duckdb")
    writer.connect()
    writer.create_table("widget", {"id": "INTEGER"}, primary_keys=["id"])
    writer.bulk_upsert("widget", [{"id": 1}, {"id": 2}])
    writer.close()

    reader = DuckDBBackend(db_path=tmp_path / "reader.duckdb")
    reader.connect()
    try:
        reader.attach_readonly(str(tmp_path / "writer.duckdb"), alias="src")
        rows = reader.execute("SELECT COUNT(*) FROM src.widget")
        assert rows == [[2]]
        # Read-only — writes against the attached schema must fail.
        duckdb_mod = pytest.importorskip("duckdb")
        with pytest.raises(duckdb_mod.Error):
            reader.execute("INSERT INTO src.widget VALUES (3)")
    finally:
        reader.close()


class _SQLCaptureConn:
    """Tiny shim that records every execute() SQL string. Used to inspect
    what the backend would send for ops we can't easily run live (S3)."""
    def __init__(self) -> None:
        self.sent: list[str] = []

    def execute(self, sql, params=None):
        self.sent.append(sql)
        class _Cursor:
            description = None
            def fetchall(self_inner): return []
        return _Cursor()

    def close(self) -> None:
        pass


def test_export_database_to_s3_uri_emits_expected_sql(db: DuckDBBackend):
    """For s3:// targets we can't easily round-trip locally, but we can
    verify the SQL the backend would send — guards against quoting and
    parameter-binding bugs even when nobody runs against a live bucket.
    """
    fake = _SQLCaptureConn()
    db._conn.close()
    db._conn = fake  # type: ignore[assignment]
    db.export_database("s3://my-bucket/snapshots/2026-06-15")
    assert any(
        "EXPORT DATABASE 's3://my-bucket/snapshots/2026-06-15'" in s
        and "FORMAT PARQUET" in s
        for s in fake.sent
    )


def test_export_database_escapes_single_quotes_in_uri(db: DuckDBBackend):
    """A URI containing a single quote shouldn't break out of the literal."""
    fake = _SQLCaptureConn()
    db._conn.close()
    db._conn = fake  # type: ignore[assignment]
    db.export_database("/tmp/it's-fine")
    assert any("/tmp/it''s-fine" in s for s in fake.sent)


def test_attach_readonly_to_s3_uri_emits_expected_sql(db: DuckDBBackend):
    """ATTACH 's3://...' (READ_ONLY) — verify SQL shape without live S3."""
    fake = _SQLCaptureConn()
    db._conn.close()
    db._conn = fake  # type: ignore[assignment]
    db.attach_readonly("s3://my-bucket/db.duckdb", alias="src")
    # First call loads httpfs; second issues the ATTACH.
    assert any("INSTALL httpfs; LOAD httpfs" in s for s in fake.sent)
    assert any(
        "ATTACH 's3://my-bucket/db.duckdb' AS src (READ_ONLY)" in s
        for s in fake.sent
    )
