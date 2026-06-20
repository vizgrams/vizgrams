# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Dedup + add PRIMARY KEY to a model's DuckDB raw + sem tables.

Background: DuckDBBackend.create_table adds a PRIMARY KEY when the
caller supplies ``primary_keys`` — but only when the table is first
created. Tables that pre-date that codepath stay PK-less, so
``INSERT ... ON CONFLICT DO UPDATE`` in bulk_upsert silently falls back
to plain INSERT, and every extractor / mapper run appends a fresh copy
of every row. Mappers downstream then trip ``FanOutError`` on the LEFT
JOIN to a duplicated table.

This script reads:

  - each extractor YAML in ``<model>/extractors/`` (raw tables: PK =
    ``primary_keys`` declared on the output)
  - each entity YAML in ``<model>/ontology/`` (sem tables: PK = the
    first column under ``identity:``)

For any table whose declared PK doesn't match the on-disk PK constraint:

  1. snapshot the table
  2. keep one row per (pk) — picking the row with the highest
     ``_loaded_at`` (or ``rowid`` if no loaded_at column) — into a
     temp table
  3. drop the original, recreate it with the declared PRIMARY KEY,
     and copy the deduped snapshot back

Reports per-table before/after row counts. Skips tables whose declared
PK already matches what's on disk. Pass ``--no-sem`` to dedup only raw
extractor tables.

Usage:

  poetry run python tools/dedup_duckdb_raw_tables.py \\
      --model-dir /path/to/model

Add ``--dry-run`` to see what would change without touching the file.
The DuckDB file must not be open by another process (stop the API /
batch service before running).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import click
import duckdb
import yaml


@dataclass
class TableReport:
    table: str
    declared_pk: list[str]
    actual_pk: list[str] = field(default_factory=list)
    rows_before: int = 0
    rows_after: int = 0
    rebuilt: bool = False
    skipped_reason: str | None = None
    error: str | None = None

    @property
    def dupes_removed(self) -> int:
        return self.rows_before - self.rows_after


def _load_extractor_outputs(extractors_dir: Path) -> list[tuple[str, list[str]]]:
    """Return [(table_name, primary_keys), ...] from every extractor YAML."""
    out: list[tuple[str, list[str]]] = []
    for yml in sorted(extractors_dir.glob("*.yaml")):
        with yml.open() as f:
            doc = yaml.safe_load(f)
        tasks = doc.get("tasks") if isinstance(doc, dict) else doc
        if not tasks:
            continue
        for task in tasks:
            # Some tasks use ``output:`` (single) instead of ``outputs:`` (list).
            single = task.get("output")
            outs = task.get("outputs") or ([single] if single else [])
            for o in outs:
                table = o.get("table")
                pks = o.get("primary_keys") or []
                if table and pks:
                    out.append((table, list(pks)))
    return out


def _load_entity_pks(ontology_dir: Path) -> list[tuple[str, list[str]]]:
    """Return [(sem_table_name, [pk_col]), ...] from each entity YAML.

    Entity YAML uses ``identity: { <col>: ... }`` (sometimes a single
    string) — the first key under ``identity:`` is the entity's PK.
    Sem table name is the entity ``name`` lowercased (matching the
    materializer's convention).
    """
    out: list[tuple[str, list[str]]] = []
    for yml in sorted(ontology_dir.glob("*.yaml")):
        with yml.open() as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict):
            continue
        ent = doc.get("entity") or doc.get("name")
        identity = doc.get("identity")
        pk = None
        if isinstance(identity, dict) and identity:
            pk = next(iter(identity.keys()))
        elif isinstance(identity, list) and identity:
            first = identity[0]
            pk = first if isinstance(first, str) else (
                next(iter(first.keys())) if isinstance(first, dict) else None
            )
        elif isinstance(identity, str):
            pk = identity
        if ent and pk:
            out.append((ent.lower(), [pk]))
    return out


def _table_actual_pk(duck, table: str) -> list[str]:
    rows = duck.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
        [table],
    ).fetchall()
    return list(rows[0][0]) if rows and rows[0][0] else []


def _table_columns(duck, table: str) -> list[tuple[str, str]]:
    rows = duck.execute(
        "SELECT column_name, data_type FROM duckdb_columns() "
        "WHERE table_name = ? ORDER BY column_index",
        [table],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _rebuild_with_pk(duck, table: str, pks: list[str]) -> tuple[int, int]:
    """Dedup + rebuild ``table`` with a PRIMARY KEY. Returns (before, after)."""
    cols = _table_columns(duck, table)
    col_names = [c[0] for c in cols]
    has_loaded_at = "_loaded_at" in col_names
    order_expr = "_loaded_at DESC" if has_loaded_at else "rowid DESC"
    pk_expr = ", ".join(pks)
    col_list = ", ".join(col_names)

    before = duck.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    # Snapshot one row per PK group into a temp table, keeping the most
    # recently loaded copy when there are duplicates.
    tmp = f"_dedup_{table}"
    duck.execute(f'DROP TABLE IF EXISTS "{tmp}"')
    duck.execute(
        f'CREATE TABLE "{tmp}" AS '
        f"SELECT {col_list} FROM ("
        f"  SELECT {col_list}, "
        f"  ROW_NUMBER() OVER (PARTITION BY {pk_expr} ORDER BY {order_expr}) AS _rn "
        f'  FROM "{table}"'
        f") WHERE _rn = 1"
    )
    after = duck.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0]

    col_defs = [f"{n} {t}" for n, t in cols]
    col_defs.append(f"PRIMARY KEY ({pk_expr})")
    duck.execute(f'DROP TABLE "{table}"')
    duck.execute(f'CREATE TABLE "{table}" ({", ".join(col_defs)})')
    duck.execute(f'INSERT INTO "{table}" SELECT {col_list} FROM "{tmp}"')
    duck.execute(f'DROP TABLE "{tmp}"')
    return before, after


@click.command()
@click.option("--model-dir", required=True, type=click.Path(exists=True, file_okay=False))
@click.option("--duckdb-path", default=None, help="Override the model's database path")
@click.option("--dry-run", is_flag=True, help="Report what would change, don't write")
@click.option(
    "--include-sem/--no-sem",
    default=True,
    help="Also dedup sem tables using entity identity (default: on).",
)
def main(model_dir: str, duckdb_path: str | None, dry_run: bool, include_sem: bool) -> None:
    mdir = Path(model_dir)
    extractors_dir = mdir / "extractors"
    if not extractors_dir.is_dir():
        raise click.ClickException(f"No extractors dir at {extractors_dir}")

    if duckdb_path is None:
        # Mirror core.model_config.load_database_config's default.
        duckdb_path = str(mdir / "data" / "data.duckdb")
    if not Path(duckdb_path).exists():
        raise click.ClickException(f"DuckDB file not found: {duckdb_path}")

    outputs = _load_extractor_outputs(extractors_dir)
    msg = f"Found {len(outputs)} extractor outputs with primary_keys declared."
    if include_sem:
        ontology_dir = mdir / "ontology"
        if ontology_dir.is_dir():
            sem_outputs = _load_entity_pks(ontology_dir)
            outputs.extend(sem_outputs)
            msg += f"  Plus {len(sem_outputs)} sem tables from ontology/."
        else:
            msg += "  (No ontology/ dir — sem dedup skipped.)"
    click.echo(msg)

    duck = duckdb.connect(duckdb_path, read_only=dry_run)

    reports: list[TableReport] = []
    for table, pks in outputs:
        rep = TableReport(table=table, declared_pk=pks)
        try:
            exists = duck.execute(
                "SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [table]
            ).fetchone()
            if not exists:
                rep.skipped_reason = "table not in db"
                reports.append(rep)
                continue
            rep.actual_pk = _table_actual_pk(duck, table)
            rep.rows_before = duck.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            distinct = duck.execute(
                f'SELECT COUNT(*) FROM (SELECT DISTINCT {", ".join(pks)} FROM "{table}")'
            ).fetchone()[0]
            if rep.actual_pk == pks and distinct == rep.rows_before:
                rep.skipped_reason = "already pk + no dupes"
                reports.append(rep)
                continue
            if dry_run:
                rep.rows_after = distinct
                rep.rebuilt = False
                reports.append(rep)
                continue
            before, after = _rebuild_with_pk(duck, table, pks)
            rep.rows_before, rep.rows_after = before, after
            rep.rebuilt = True
        except Exception as e:
            rep.error = str(e)
        reports.append(rep)

    duck.close()

    click.echo("")
    click.echo(f"{'table':30s}  {'before':>8s}  {'after':>8s}  {'removed':>8s}  status")
    click.echo("-" * 80)
    for r in reports:
        if r.error:
            status = f"ERROR: {r.error[:40]}"
        elif r.skipped_reason:
            status = f"skip ({r.skipped_reason})"
        elif r.rebuilt:
            status = "rebuilt with PK"
        else:
            status = "would rebuild (dry-run)"
        click.echo(
            f"{r.table:30s}  {r.rows_before:8d}  {r.rows_after:8d}  "
            f"{r.dupes_removed:8d}  {status}"
        )

    total_removed = sum(r.dupes_removed for r in reports if not r.error)
    rebuilt = sum(1 for r in reports if r.rebuilt)
    click.echo("")
    click.echo(f"Tables rebuilt: {rebuilt}.  Total dupes removed: {total_removed}.")


if __name__ == "__main__":
    main()
