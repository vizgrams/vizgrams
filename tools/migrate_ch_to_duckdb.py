# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Migrate a vizgrams model's data from ClickHouse to DuckDB.

Per-table arrow roundtrip with row-count + null-count parity. Designed
for the one-time switch in Phase 7 of the CH→DuckDB migration plan
(see project_duckdb_migration.md in claude memory). The user runs this
once per model; on a successful pass with parity-OK on every table,
Phase 8 flips the model config from `backend: clickhouse` to
`backend: duckdb`.

Usage:

  poetry run python tools/migrate_ch_to_duckdb.py \\
    --model-dir models/example \\
    --ch-database example \\
    --ch-raw-database example_raw

Aborts on row-count mismatch by default. Null-count mismatches are
reported as warnings unless --strict is set.

The source CH must be reachable; the target DuckDB file is created if
missing (path comes from the model's config.yaml unless --duckdb-path
overrides it).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import click

# ---------------------------------------------------------------------------
# Per-table report shape
# ---------------------------------------------------------------------------


@dataclass
class TableReport:
    table: str
    source_db: str
    ch_row_count: int = 0
    duck_row_count: int = 0
    null_warnings: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def parity_ok(self) -> bool:
        return self.error is None and self.ch_row_count == self.duck_row_count


# ---------------------------------------------------------------------------
# Migration core
# ---------------------------------------------------------------------------


def _list_user_tables(ch_client, database: str) -> list[str]:
    """Return tables in ``database`` excluding ClickHouse system metadata."""
    res = ch_client.query(
        f"SELECT name FROM system.tables WHERE database = '{database}' "
        f"AND engine != 'View' ORDER BY name"
    )
    return [row[0] for row in res.result_rows]


def _migrate_one_table(
    *,
    ch_client,
    duck,
    ch_database: str,
    table: str,
    check_nulls: bool,
) -> TableReport:
    """Copy a single CH table into the connected DuckDB backend.

    Always drops and recreates the target table from the arrow schema so
    we don't carry forward a stale shape. Returns a report — caller
    decides whether to abort on parity failures.
    """
    report = TableReport(table=table, source_db=ch_database)
    try:
        # FINAL collapses ReplacingMergeTree duplicates so the count matches
        # what the FINAL SELECT below actually loads. Without FINAL, count()
        # returns every pre-merge row and parity always fails on a table
        # that's seen even one upsert.
        ch_count_rows = ch_client.query(
            f"SELECT count() FROM `{ch_database}`.`{table}` FINAL"
        ).result_rows
        report.ch_row_count = int(ch_count_rows[0][0]) if ch_count_rows else 0

        arrow_table = ch_client.query_arrow(
            f"SELECT * FROM `{ch_database}`.`{table}` FINAL"
        )
        # DuckDB looks up `arrow_table` in the calling Python scope — this
        # is the official zero-copy ingestion path.
        duck.execute(f'DROP TABLE IF EXISTS "{table}"')
        duck.execute(f'CREATE TABLE "{table}" AS SELECT * FROM arrow_table')

        cnt = duck.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
        report.duck_row_count = int(cnt[0]) if cnt else 0

        if check_nulls and report.parity_ok:
            for col in arrow_table.column_names:
                ch_nulls = ch_client.query(
                    f"SELECT countIf({col} IS NULL) FROM "
                    f"`{ch_database}`.`{table}` FINAL"
                ).result_rows[0][0]
                duck_nulls = duck.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL'
                ).fetchone()[0]
                if ch_nulls != duck_nulls:
                    report.null_warnings.append(
                        f"{col}: ch={ch_nulls} duck={duck_nulls}"
                    )
    except Exception as exc:
        report.error = str(exc)
    return report


def migrate(
    *,
    ch_client,
    duck,
    ch_sem_database: str,
    ch_raw_database: str,
    only_tables: set[str] | None = None,
    check_nulls: bool = True,
) -> list[TableReport]:
    """Migrate every table in the sem + raw CH databases to DuckDB.

    ``only_tables`` filters to a fixed allowlist (for partial re-runs).
    The two CH databases are processed in order — sem first, then raw —
    and the same DuckDB file holds both since DuckDB is single-schema.
    Cross-database name collisions are surfaced as errors on the second
    table's report.
    """
    reports: list[TableReport] = []
    seen_table_names: set[str] = set()
    for ch_db in (ch_sem_database, ch_raw_database):
        tables = _list_user_tables(ch_client, ch_db)
        if only_tables:
            tables = [t for t in tables if t in only_tables]
        for table in tables:
            if table in seen_table_names:
                report = TableReport(
                    table=table, source_db=ch_db,
                    error=(
                        f"name collision: {table} already migrated from a "
                        f"prior database; skipping to avoid clobbering"
                    ),
                )
                reports.append(report)
                continue
            seen_table_names.add(table)
            reports.append(
                _migrate_one_table(
                    ch_client=ch_client, duck=duck,
                    ch_database=ch_db, table=table,
                    check_nulls=check_nulls,
                )
            )
    return reports


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_report(reports: list[TableReport]) -> tuple[int, int, int]:
    ok = sum(1 for r in reports if r.parity_ok and not r.null_warnings)
    warned = sum(1 for r in reports if r.parity_ok and r.null_warnings)
    failed = sum(1 for r in reports if not r.parity_ok)

    click.echo("\nMigration summary:")
    for r in reports:
        if r.error:
            click.echo(
                f"  ✗ {r.source_db}.{r.table:40s}  ERROR: {r.error}", err=True
            )
        elif r.ch_row_count != r.duck_row_count:
            click.echo(
                f"  ✗ {r.source_db}.{r.table:40s}  "
                f"ch={r.ch_row_count} duck={r.duck_row_count}", err=True
            )
        elif r.null_warnings:
            click.echo(
                f"  ! {r.source_db}.{r.table:40s}  rows={r.ch_row_count} "
                f"({len(r.null_warnings)} column(s) null-count mismatch)"
            )
            for w in r.null_warnings:
                click.echo(f"      {w}")
        else:
            click.echo(
                f"  ✓ {r.source_db}.{r.table:40s}  rows={r.ch_row_count}"
            )
    click.echo(f"\n{ok} OK · {warned} with null warnings · {failed} failed")
    return ok, warned, failed


@click.command()
@click.option(
    "--model-dir", required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Local model directory containing config.yaml.",
)
@click.option("--ch-host", envvar="CLICKHOUSE_HOST", default="localhost",
              show_default=True, help="Env: CLICKHOUSE_HOST.")
@click.option("--ch-port", envvar="CLICKHOUSE_PORT", default=8123,
              show_default=True, type=int, help="Env: CLICKHOUSE_PORT.")
@click.option("--ch-username", envvar="CLICKHOUSE_USERNAME", default="default",
              show_default=True, help="Env: CLICKHOUSE_USERNAME.")
@click.option("--ch-password", envvar="CLICKHOUSE_PASSWORD", default="",
              help="Env: CLICKHOUSE_PASSWORD.")
@click.option("--ch-database", help="Sem database name. Defaults to the model directory name.")
@click.option("--ch-raw-database",
              help="Raw database name. Defaults to <ch-database>_raw.")
@click.option("--duckdb-path",
              help="Override target DuckDB file. Defaults to the model's "
                   "data/data.duckdb relative to --model-dir.")
@click.option("--tables", multiple=True,
              help="Limit to these tables (repeatable). Empty = every user "
                   "table in the two CH databases.")
@click.option("--no-null-check", is_flag=True,
              help="Skip per-column null-count parity (faster on huge tables).")
@click.option("--strict", is_flag=True,
              help="Treat null-count mismatches as failures too "
                   "(default: warnings only).")
def main(
    model_dir: str,
    ch_host: str,
    ch_port: int,
    ch_username: str,
    ch_password: str,
    ch_database: str | None,
    ch_raw_database: str | None,
    duckdb_path: str | None,
    tables: tuple[str, ...],
    no_null_check: bool,
    strict: bool,
) -> None:
    """Migrate a model's data from ClickHouse to DuckDB."""
    import clickhouse_connect

    from core.db import DuckDBBackend

    model_path = Path(model_dir)
    if not ch_database:
        ch_database = model_path.name
    if not ch_raw_database:
        ch_raw_database = f"{ch_database}_raw"
    if not duckdb_path:
        duckdb_path = str(model_path / "data" / "data.duckdb")

    click.echo(
        f"Source: clickhouse://{ch_host}:{ch_port}/{ch_database} "
        f"+ {ch_raw_database}", err=True,
    )
    click.echo(f"Target: {duckdb_path}", err=True)

    ch_client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port,
        username=ch_username, password=ch_password,
    )
    duck = DuckDBBackend(db_path=duckdb_path)
    duck.connect()
    try:
        reports = migrate(
            ch_client=ch_client,
            duck=duck._conn,
            ch_sem_database=ch_database,
            ch_raw_database=ch_raw_database,
            only_tables=set(tables) if tables else None,
            check_nulls=not no_null_check,
        )
    finally:
        duck.close()
        ch_client.close()

    ok, warned, failed = _print_report(reports)

    # Exit code communicates outcome to scripts / CI:
    #   0 — every table OK (rows match; nulls match or unchecked)
    #   1 — at least one row-count mismatch or load error
    #   2 — --strict and at least one null-count warning
    if failed:
        sys.exit(1)
    if strict and warned:
        sys.exit(2)


if __name__ == "__main__":
    main()
