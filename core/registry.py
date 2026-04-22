# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Model registry: load/save registry.yaml, append to audit.log."""

import getpass
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Per-job progress files  (.jobs/{job_id}.log)
# ---------------------------------------------------------------------------
# Batch runner jobs run in a separate process and cannot update the API's
# in-memory job service.  Instead they write progress lines to a sidecar file
# that the jobs API reads while the job is in the "running" state.
# ---------------------------------------------------------------------------


def job_progress_path(model_dir: Path, job_id: str) -> Path:
    return model_dir / ".jobs" / f"{job_id}.log"


def append_job_progress(model_dir: Path, job_id: str, message: str) -> None:
    """Append a progress line to the job's sidecar progress file."""
    path = job_progress_path(model_dir, job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(message + "\n")


def read_job_progress(model_dir: Path, job_id: str) -> list[str]:
    """Return all progress lines written so far for a running batch job."""
    path = job_progress_path(model_dir, job_id)
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if line]


def delete_job_progress(model_dir: Path, job_id: str) -> None:
    """Remove the progress sidecar file once the job reaches a terminal state."""
    path = job_progress_path(model_dir, job_id)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass

import yaml


def load_registry(models_dir: Path) -> dict[str, dict]:
    """Load registry.yaml from models_dir and return the models dict. Returns {} if absent."""
    path = models_dir / "registry.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("models", {})


def save_registry(models_dir: Path, models: dict[str, dict]) -> None:
    """Write registry.yaml into models_dir from a models dict."""
    path = models_dir / "registry.yaml"
    with open(path, "w") as f:
        yaml.dump({"models": models}, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def get_active_context(base_dir: Path) -> str | None:
    """Return the model name from .vz_context, or None."""
    ctx_file = base_dir / ".vz_context"
    if ctx_file.is_file():
        name = ctx_file.read_text().strip()
        return name or None
    return None


def current_actor() -> str:
    """Return the current OS username."""
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def append_audit(model_dir: Path, event: str, detail: str | dict, actor: str | None = None) -> None:
    """Append an audit event for a model to the central batch.db audit_events table."""
    from core.batch_db import insert_audit_event
    insert_audit_event(
        model_id=model_dir.name,
        event=event,
        detail=detail,
        actor=actor or current_actor(),
    )


def read_audit(model_dir: Path) -> list[dict]:
    """Return all audit events for a model from batch.db, oldest first.

    Returns dicts in the legacy format: ``{version, timestamp, event, actor, detail}``.
    """
    from core.batch_db import read_audit_events
    return read_audit_events(model_dir.name)


def append_job_audit(model_dir: Path, job) -> None:
    """No-op: job state is now persisted by JobService to the api_jobs table.

    Kept for call-site compatibility — callers in feature_service, mapper_service,
    and entity_service call this after job_service.complete/fail, which already
    writes the terminal state to batch.db.
    """


def read_job_history(
    model_dir: Path,
    status: str | None = None,
    operation: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Return API job history for a model from batch.db, newest first.

    Returns dicts with the same keys as the Job dataclass (job_id, model,
    operation, status, started_at, completed_at, result, error, …).
    """
    from core.batch_db import list_api_jobs
    rows = list_api_jobs(model_dir.name, status=status, operation=operation, limit=limit)
    # Map DB column names to legacy Job dataclass keys
    result = []
    for row in rows:
        result.append({
            "job_id": row["id"],
            "model": row["model"],
            "operation": row.get("operation"),
            "status": row["status"],
            "started_at": row["created_at"],
            "extractor": row.get("extractor"),
            "entity": row.get("entity"),
            "task": row.get("task"),
            "completed_at": row.get("completed_at"),
            "result": row.get("result"),
            "error": row.get("error"),
            "progress": [],
            "warnings": [],
        })
    return result


def find_orphaned_jobs(model_dir: Path) -> list[dict]:
    """Return API jobs for this model that are still in running/cancelling status.

    These are jobs that were mid-run when the server last restarted.
    Returns dicts in the Job dataclass format (job_id, model, status, …).
    """
    from core.batch_db import list_api_jobs
    rows = list_api_jobs(model_dir.name, status=None, limit=0)
    return [
        {
            "job_id": row["id"],
            "model": row["model"],
            "status": row["status"],
            "started_at": row["created_at"],
            "operation": row.get("operation"),
        }
        for row in rows
        if row["status"] in ("running", "cancelling")
    ]


def mark_orphaned_jobs(models_dir: Path) -> int:
    """Mark running/cancelling API jobs as failed on startup.

    Delegates to the central batch.db — the models_dir is no longer scanned
    since job state is stored centrally.  Jobs started within the last 600 s
    are skipped (they may belong to still-running background threads).

    Returns the number of orphans marked.
    """
    from core.batch_db import mark_orphaned_api_jobs
    return mark_orphaned_api_jobs()
