# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Job store and background task runner for long-running operations.

In-memory state (progress, warnings) is kept in the process for fast polling.
Persistent state (status, result, error) is also written to the central
batch.db ``api_jobs`` table so jobs survive server restarts (VG-105).
"""

import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

_log = logging.getLogger(__name__)


class JobStatus(StrEnum):
    running = "running"
    cancelling = "cancelling"
    cancelled = "cancelled"
    completed = "completed"
    failed = "failed"


@dataclass
class Job:
    job_id: str
    model: str
    operation: str
    status: JobStatus
    started_at: str
    # operation-specific optional fields
    extractor: str | None = None
    entity: str | None = None
    task: str | None = None
    completed_at: str | None = None
    result: dict | None = None
    error: str | None = None
    # live progress and warnings (populated during execution, not persisted)
    progress: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def _row_to_job(row: dict) -> Job:
    """Convert an api_jobs DB row to a Job dataclass."""
    return Job(
        job_id=row["id"],
        model=row["model"],
        operation=row.get("operation") or "",
        status=JobStatus(row["status"]),
        started_at=row["created_at"],
        extractor=row.get("extractor"),
        entity=row.get("entity"),
        task=row.get("task"),
        completed_at=row.get("completed_at"),
        result=row.get("result"),
        error=row.get("error"),
    )


class JobService:
    """Thread-safe job store with background task execution.

    In-memory dict is the source of truth for running jobs (fast progress
    updates).  All state transitions are also written to batch.db so that
    completed/failed jobs are visible after a server restart.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def _now(self) -> str:
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _persist_create(self, job: Job) -> None:
        try:
            from core.batch_db import insert_api_job
            insert_api_job(
                id=job.job_id,
                model=job.model,
                status=job.status.value,
                created_at=job.started_at,
                operation=job.operation,
                extractor=job.extractor,
                entity=job.entity,
                task=job.task,
            )
        except Exception:
            _log.debug("batch_db insert_api_job failed (non-fatal)", exc_info=True)

    def _persist_update(self, job_id: str, **kwargs) -> None:
        try:
            from core.batch_db import update_api_job
            update_api_job(job_id, **kwargs)
        except Exception:
            _log.debug("batch_db update_api_job failed (non-fatal)", exc_info=True)

    def create(self, model: str, operation: str, **kwargs) -> Job:
        job = Job(
            job_id=str(uuid.uuid4()),
            model=model,
            operation=operation,
            status=JobStatus.running,
            started_at=self._now(),
            **kwargs,
        )
        with self._lock:
            self._jobs[job.job_id] = job
        self._persist_create(job)
        return job

    def get(self, model: str, job_id: str) -> Job | None:
        job = self._jobs.get(job_id)
        if job is not None:
            return job if job.model == model else None
        # Fall back to DB for historical jobs (not in current process memory)
        try:
            from core.batch_db import get_api_job
            row = get_api_job(job_id, model)
            if row:
                return _row_to_job(row)
        except Exception:
            _log.debug("batch_db get_api_job failed (non-fatal)", exc_info=True)
        return None

    def list(
        self,
        model: str,
        status: JobStatus | None = None,
        operation: str | None = None,
        limit: int = 20,
    ) -> list[Job]:
        # Start with in-memory jobs for this model
        jobs = [j for j in self._jobs.values() if j.model == model]
        in_memory_ids = {j.job_id for j in jobs}

        # Supplement with DB rows for historical jobs not in memory
        try:
            from core.batch_db import list_api_jobs
            db_rows = list_api_jobs(
                model,
                status=status.value if status else None,
                operation=operation,
                limit=limit,
            )
            for row in db_rows:
                if row["id"] not in in_memory_ids:
                    jobs.append(_row_to_job(row))
        except Exception:
            _log.debug("batch_db list_api_jobs failed (non-fatal)", exc_info=True)

        if status:
            jobs = [j for j in jobs if j.status == status]
        if operation:
            jobs = [j for j in jobs if j.operation == operation]
        return sorted(jobs, key=lambda j: j.started_at, reverse=True)[:limit]

    def complete(self, job_id: str, result: dict) -> None:
        completed_at = self._now()
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = JobStatus.completed
                job.completed_at = completed_at
                job.result = result
        self._persist_update(
            job_id, status="completed", completed_at=completed_at, result=result
        )

    def fail(self, job_id: str, error: str) -> None:
        completed_at = self._now()
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = JobStatus.failed
                job.completed_at = completed_at
                job.error = error
        self._persist_update(
            job_id, status="failed", completed_at=completed_at, error=error
        )

    def cancel(self, job_id: str) -> bool:
        """Request cancellation of a running job. Returns True if the request was accepted."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status == JobStatus.running:
                job.status = JobStatus.cancelling
                self._persist_update(job_id, status="cancelling")
                return True
        return False

    def mark_cancelled(self, job_id: str) -> None:
        """Called by the background thread to confirm it has stopped."""
        completed_at = self._now()
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = JobStatus.cancelled
                job.completed_at = completed_at
        self._persist_update(
            job_id, status="cancelled", completed_at=completed_at
        )

    def is_cancelling(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        return job is not None and job.status == JobStatus.cancelling

    def update_progress(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.progress.append(message)

    def add_warning(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.warnings.append(message)

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> None:
        """Run fn(*args, **kwargs) in a daemon background thread."""
        t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
        t.start()


# Module-level singleton — imported by routers via Depends
job_service = JobService()
