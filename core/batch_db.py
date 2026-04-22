# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""batch_db.py — central SQLite store for API jobs, pipeline runs, audit events,
and batch-service scheduled jobs.

Replaces:
  - per-model ``audit.log`` JSONL files  (VG-107)
  - in-memory API JobService             (VG-105)
  - per-model ``scryglass-batch.db``     (VG-108)

DB path resolution (first wins):
  1. ``db_path`` argument — explicit override used in tests.
  2. ``BATCH_DB_PATH`` environment variable — absolute path to the SQLite file.
  3. ``{VZ_BASE_DIR}/data/batch.db`` — service-level DB alongside api.db.
  4. ``{repo_root}/data/batch.db`` — fallback for local development.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

_DDL = """
PRAGMA journal_mode = WAL;

-- API jobs (interactive runs: feature reconcile, mapper, entity materialise)
CREATE TABLE IF NOT EXISTS api_jobs (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL DEFAULT 'api',
    status       TEXT NOT NULL,
    model        TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    completed_at TEXT,
    operation    TEXT,
    extractor    TEXT,
    entity       TEXT,
    task         TEXT,
    result       TEXT,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_jobs_model ON api_jobs (model);

-- Pipeline run logs (VG-106): extractor/mapper stage-level history
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            TEXT PRIMARY KEY,
    job_id        TEXT,
    model_id      TEXT NOT NULL,
    stage         TEXT NOT NULL,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    rows_affected INTEGER,
    status        TEXT NOT NULL DEFAULT 'running',
    error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_model ON pipeline_runs (model_id);

-- Audit events (VG-107): non-job events (model_created, map_run, pipeline_checkpoint, etc.)
CREATE TABLE IF NOT EXISTS audit_events (
    id         TEXT PRIMARY KEY,
    model_id   TEXT NOT NULL,
    event      TEXT NOT NULL,
    detail     TEXT NOT NULL,
    actor      TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_model ON audit_events (model_id, created_at);

-- Batch service: scheduled extractor/mapper/materialize jobs (VG-108)
CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    model        TEXT NOT NULL,
    operation    TEXT NOT NULL,
    tool         TEXT,
    status       TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    completed_at TEXT,
    records      INTEGER,
    duration_s   REAL,
    error        TEXT,
    triggered_by TEXT NOT NULL DEFAULT 'api'
);
CREATE INDEX IF NOT EXISTS idx_jobs_model ON jobs (model);

-- Batch service: per-job progress log
CREATE TABLE IF NOT EXISTS job_progress (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    TEXT NOT NULL REFERENCES jobs(job_id),
    ts        TEXT NOT NULL,
    message   TEXT NOT NULL
);

-- Batch service: cron schedules
CREATE TABLE IF NOT EXISTS schedules (
    model      TEXT NOT NULL,
    tool       TEXT NOT NULL,
    cron       TEXT NOT NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (model, tool)
);
"""


def get_batch_db_path(db_path: Path | None = None) -> Path:
    """Return the path to the central batch.db."""
    if db_path is not None:
        return db_path
    env = os.environ.get("BATCH_DB_PATH")
    if env:
        return Path(env)
    base_dir_env = os.environ.get("VZ_BASE_DIR")
    if base_dir_env:
        return Path(base_dir_env) / "data" / "batch.db"
    return Path(__file__).resolve().parents[1] / "data" / "batch.db"


@contextmanager
def get_connection(db_path: Path | None = None):
    """Open a connection to the central batch.db, creating it if needed."""
    path = get_batch_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path), timeout=10, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        con.executescript(_DDL)
        con.commit()
        yield con
    finally:
        con.close()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _grace_cutoff() -> str:
    """ISO timestamp 600 seconds ago — jobs newer than this may still be running."""
    return (datetime.now(UTC) - timedelta(seconds=600)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# API jobs (VG-105)
# ---------------------------------------------------------------------------


def insert_api_job(
    *,
    id: str,
    model: str,
    status: str,
    created_at: str,
    operation: str | None = None,
    extractor: str | None = None,
    entity: str | None = None,
    task: str | None = None,
    db_path: Path | None = None,
) -> None:
    with get_connection(db_path) as con:
        con.execute(
            """
            INSERT INTO api_jobs
                (id, status, model, created_at, updated_at, operation, extractor, entity, task)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (id, status, model, created_at, created_at, operation, extractor, entity, task),
        )
        con.commit()


def update_api_job(
    job_id: str,
    *,
    status: str | None = None,
    completed_at: str | None = None,
    result: dict | None = None,
    error: str | None = None,
    db_path: Path | None = None,
) -> None:
    fields: dict = {"updated_at": _now()}
    if status is not None:
        fields["status"] = status
    if completed_at is not None:
        fields["completed_at"] = completed_at
    if result is not None:
        fields["result"] = json.dumps(result)
    if error is not None:
        fields["error"] = error
    sets = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    with get_connection(db_path) as con:
        con.execute(f"UPDATE api_jobs SET {sets} WHERE id = ?", values)
        con.commit()


def get_api_job(job_id: str, model: str, db_path: Path | None = None) -> dict | None:
    with get_connection(db_path) as con:
        row = con.execute(
            "SELECT * FROM api_jobs WHERE id = ? AND model = ?", (job_id, model)
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("result"):
        try:
            d["result"] = json.loads(d["result"])
        except (json.JSONDecodeError, TypeError):
            pass
    return d


def list_api_jobs(
    model: str,
    status: str | None = None,
    operation: str | None = None,
    limit: int = 20,
    db_path: Path | None = None,
) -> list[dict]:
    query = "SELECT * FROM api_jobs WHERE model = ?"
    params: list = [model]
    if status:
        query += " AND status = ?"
        params.append(status)
    if operation:
        query += " AND operation = ?"
        params.append(operation)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_connection(db_path) as con:
        rows = con.execute(query, params).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        if d.get("result"):
            try:
                d["result"] = json.loads(d["result"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return result


def mark_orphaned_api_jobs(db_path: Path | None = None) -> int:
    """Mark running/cancelling API jobs as failed.

    Jobs started within the last 600 s are skipped — they may still be running
    in background threads.  Returns the number of jobs marked.
    """
    cutoff = _grace_cutoff()
    now = _now()
    with get_connection(db_path) as con:
        cur = con.execute(
            """
            UPDATE api_jobs
            SET status = 'failed', completed_at = ?, updated_at = ?,
                error = 'Server restarted while job was running'
            WHERE status IN ('running', 'cancelling') AND created_at < ?
            """,
            (now, now, cutoff),
        )
        con.commit()
        return cur.rowcount


# ---------------------------------------------------------------------------
# Pipeline runs (VG-106)
# ---------------------------------------------------------------------------


def insert_pipeline_run(
    *,
    model_id: str,
    stage: str,
    started_at: str,
    job_id: str | None = None,
    db_path: Path | None = None,
) -> str:
    """Insert a new pipeline run row and return its ID."""
    run_id = str(uuid.uuid4())
    with get_connection(db_path) as con:
        con.execute(
            """
            INSERT INTO pipeline_runs (id, job_id, model_id, stage, started_at, status)
            VALUES (?, ?, ?, ?, ?, 'running')
            """,
            (run_id, job_id, model_id, stage, started_at),
        )
        con.commit()
    return run_id


def finish_pipeline_run(
    run_id: str,
    *,
    status: str,
    finished_at: str,
    rows_affected: int | None = None,
    error: str | None = None,
    db_path: Path | None = None,
) -> None:
    with get_connection(db_path) as con:
        con.execute(
            """
            UPDATE pipeline_runs
            SET status = ?, finished_at = ?, rows_affected = ?, error = ?
            WHERE id = ?
            """,
            (status, finished_at, rows_affected, error, run_id),
        )
        con.commit()


# ---------------------------------------------------------------------------
# Audit events (VG-107)
# ---------------------------------------------------------------------------


def insert_audit_event(
    model_id: str,
    event: str,
    detail: str | dict,
    actor: str,
    db_path: Path | None = None,
) -> None:
    if not isinstance(detail, str):
        detail = json.dumps(detail)
    with get_connection(db_path) as con:
        con.execute(
            """
            INSERT INTO audit_events (id, model_id, event, detail, actor, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), model_id, event, detail, actor, _now()),
        )
        con.commit()


def read_audit_events(
    model_id: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Return all audit events for a model, oldest first.

    Returns dicts in the same format as the old JSONL audit.log entries:
    ``{version, timestamp, event, actor, detail}``.
    """
    with get_connection(db_path) as con:
        rows = con.execute(
            "SELECT * FROM audit_events WHERE model_id = ? ORDER BY created_at ASC",
            (model_id,),
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        try:
            detail = json.loads(d["detail"])
        except (json.JSONDecodeError, TypeError):
            detail = d["detail"]
        result.append({
            "version": 1,
            "timestamp": d["created_at"],
            "event": d["event"],
            "actor": d["actor"],
            "detail": detail,
        })
    return result


# ---------------------------------------------------------------------------
# Batch service jobs (VG-108) — thin wrappers used by batch_service/db.py
# ---------------------------------------------------------------------------


def insert_batch_job(
    con: sqlite3.Connection,
    *,
    job_id: str,
    model: str,
    operation: str,
    tool: str | None,
    status: str,
    started_at: str,
    triggered_by: str,
) -> None:
    con.execute(
        """
        INSERT INTO jobs (job_id, model, operation, tool, status, started_at, triggered_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, model, operation, tool, status, started_at, triggered_by),
    )
    con.commit()


def update_batch_job(con: sqlite3.Connection, job_id: str, **fields) -> None:
    """Update arbitrary columns on a batch job row."""
    if not fields:
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    con.execute(f"UPDATE jobs SET {sets} WHERE job_id = ?", values)
    con.commit()


def get_batch_job(con: sqlite3.Connection, job_id: str) -> dict | None:
    row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    return dict(row) if row else None


def list_batch_jobs(
    con: sqlite3.Connection,
    model: str,
    status: str | None = None,
    operation: str | None = None,
    limit: int = 20,
) -> list[dict]:
    query = "SELECT * FROM jobs WHERE model = ?"
    params: list = [model]
    if status:
        query += " AND status = ?"
        params.append(status)
    if operation:
        query += " AND operation = ?"
        params.append(operation)
    query += " ORDER BY started_at DESC LIMIT ?"
    params.append(limit)
    return [dict(row) for row in con.execute(query, params).fetchall()]


def mark_orphaned_batch_jobs(db_path: Path | None = None) -> int:
    """Mark batch service jobs in running/cancelling status as failed."""
    completed_at = _now()
    with get_connection(db_path) as con:
        cur = con.execute(
            """
            UPDATE jobs SET status = 'failed', completed_at = ?,
                error = 'Process restarted while job was running'
            WHERE status IN ('running', 'cancelling')
            """,
            (completed_at,),
        )
        con.commit()
        return cur.rowcount


def append_batch_progress(
    con: sqlite3.Connection, job_id: str, ts: str, message: str
) -> None:
    con.execute(
        "INSERT INTO job_progress (job_id, ts, message) VALUES (?, ?, ?)",
        (job_id, ts, message),
    )
    con.commit()


def get_batch_progress(con: sqlite3.Connection, job_id: str) -> list[str]:
    rows = con.execute(
        "SELECT message FROM job_progress WHERE job_id = ? ORDER BY id",
        (job_id,),
    ).fetchall()
    return [row[0] for row in rows]


def upsert_schedule(con: sqlite3.Connection, model: str, tool: str, cron: str) -> None:
    con.execute(
        """
        INSERT INTO schedules (model, tool, cron, enabled) VALUES (?, ?, ?, 1)
        ON CONFLICT (model, tool) DO UPDATE SET cron = excluded.cron
        """,
        (model, tool, cron),
    )
    con.commit()


def list_schedules(con: sqlite3.Connection, model: str) -> list[dict]:
    rows = con.execute(
        "SELECT model, tool, cron, enabled FROM schedules WHERE model = ?",
        (model,),
    ).fetchall()
    return [dict(row) for row in rows]
