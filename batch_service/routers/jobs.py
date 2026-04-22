# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Job CRUD endpoints.

POST   /api/v1/jobs                              Submit a job
GET    /api/v1/jobs/{job_id}?model=X             Get job status and progress
GET    /api/v1/jobs?model=X&status=Y&limit=N     List jobs
DELETE /api/v1/jobs/{job_id}?model=X             Cancel a running job
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from batch_service import db as jobdb
from batch_service import executor as jobexec
from batch_service.config import get_model_dir

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class JobSubmit(BaseModel):
    model: str
    tool: str | None = None
    operation: str = "extract"
    task: str | None = None
    mapper: str | None = None
    entity: str | None = None
    full_refresh: bool = False
    since: str | None = None
    triggered_by: str = "api"


class JobResponse(BaseModel):
    job_id: str
    model: str
    operation: str
    status: str
    started_at: str
    tool: str | None = None
    completed_at: str | None = None
    records: int | None = None
    duration_s: float | None = None
    error: str | None = None
    triggered_by: str
    progress: list[str] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_response(job: dict, model_dir=None) -> JobResponse:
    progress: list[str] = []
    if job["status"] in ("running", "cancelling") and model_dir is not None:
        try:
            with jobdb.get_connection(model_dir) as con:
                progress = jobdb.get_progress(con, job["job_id"])
        except Exception:
            pass
    return JobResponse(
        job_id=job["job_id"],
        model=job["model"],
        operation=job["operation"],
        status=job["status"],
        started_at=job["started_at"],
        tool=job.get("tool"),
        completed_at=job.get("completed_at"),
        records=job.get("records"),
        duration_s=job.get("duration_s"),
        error=job.get("error"),
        triggered_by=job.get("triggered_by", "api"),
        progress=progress,
    )


def _get_model_dir_or_404(model: str):
    try:
        return get_model_dir(model)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("", response_model=JobResponse, status_code=202)
def submit_job(body: JobSubmit):
    """Submit an extraction or mapper job to the thread pool."""
    model_dir = _get_model_dir_or_404(body.model)

    if body.operation == "map":
        return _submit_mapper_job(body, model_dir)
    if body.operation == "materialize":
        return _submit_materialize_job(body, model_dir)
    return _submit_extract_job(body, model_dir)


def _submit_extract_job(body: JobSubmit, model_dir) -> JobResponse:
    if not body.tool:
        raise HTTPException(status_code=422, detail="'tool' is required for extract jobs.")

    # Validate tool exists
    from engine.extractor import find_extractor
    try:
        find_extractor(model_dir, body.tool)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Check for an already-running job for this model+tool
    with jobdb.get_connection(model_dir) as con:
        running = [
            j for j in jobdb.list_jobs(con, body.model, status="running", limit=50)
            if j.get("tool") == body.tool
        ]
        running += [
            j for j in jobdb.list_jobs(con, body.model, status="cancelling", limit=50)
            if j.get("tool") == body.tool
        ]
        if running:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Job '{running[0]['job_id']}' is already running for "
                    f"model '{body.model}' tool '{body.tool}'. Cancel it first."
                ),
            )

        job_id = str(uuid.uuid4())
        since_override = "1970-01-01" if body.full_refresh else body.since
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=body.model,
            operation=body.operation,
            tool=body.tool,
            status="running",
            started_at=_now(),
            triggered_by=body.triggered_by,
        )
        job = jobdb.get_job(con, job_id)

    jobexec.submit(model_dir, job_id, body.tool, body.task, since_override)

    _log.info(
        "Extract job submitted",
        extra={
            "job_id": job_id,
            "model": body.model,
            "tool": body.tool,
            "triggered_by": body.triggered_by,
        },
    )
    return _to_response(job)


def _submit_mapper_job(body: JobSubmit, model_dir) -> JobResponse:
    # Check for an already-running mapper job for this model
    mapper_key = body.mapper or "__all__"
    with jobdb.get_connection(model_dir) as con:
        running = [
            j for j in jobdb.list_jobs(con, body.model, status="running", limit=50)
            if j.get("operation") == "map" and j.get("tool") == mapper_key
        ]
        running += [
            j for j in jobdb.list_jobs(con, body.model, status="cancelling", limit=50)
            if j.get("operation") == "map" and j.get("tool") == mapper_key
        ]
        if running:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Mapper job '{running[0]['job_id']}' is already running for "
                    f"model '{body.model}'. Cancel it first."
                ),
            )

        job_id = str(uuid.uuid4())
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=body.model,
            operation="map",
            tool=mapper_key,
            status="running",
            started_at=_now(),
            triggered_by=body.triggered_by,
        )
        job = jobdb.get_job(con, job_id)

    jobexec.submit_mapper(model_dir, job_id, body.mapper)

    _log.info(
        "Mapper job submitted",
        extra={
            "job_id": job_id,
            "model": body.model,
            "mapper": body.mapper or "all",
            "triggered_by": body.triggered_by,
        },
    )
    return _to_response(job)


def _submit_materialize_job(body: JobSubmit, model_dir) -> JobResponse:
    entity_key = body.entity or "__all__"
    with jobdb.get_connection(model_dir) as con:
        running = [
            j for j in jobdb.list_jobs(con, body.model, status="running", limit=50)
            if j.get("operation") == "materialize" and j.get("tool") == entity_key
        ]
        if running:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Materialize job '{running[0]['job_id']}' is already running for "
                    f"model '{body.model}'. Cancel it first."
                ),
            )

        job_id = str(uuid.uuid4())
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=body.model,
            operation="materialize",
            tool=entity_key,
            status="running",
            started_at=_now(),
            triggered_by=body.triggered_by,
        )
        job = jobdb.get_job(con, job_id)

    jobexec.submit_materialize(model_dir, job_id, body.entity)

    _log.info(
        "Materialize job submitted",
        extra={
            "job_id": job_id,
            "model": body.model,
            "entity": body.entity or "all",
            "triggered_by": body.triggered_by,
        },
    )
    return _to_response(job)


@router.get("", response_model=list[JobResponse])
def list_jobs(
    model: str = Query(...),
    status: str | None = Query(None),
    operation: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """List jobs for a model, newest first."""
    model_dir = _get_model_dir_or_404(model)
    with jobdb.get_connection(model_dir) as con:
        jobs = jobdb.list_jobs(con, model, status=status, operation=operation, limit=limit)
    return [_to_response(j) for j in jobs]


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: str, model: str = Query(...)):
    """Get a specific job, including live progress if still running."""
    model_dir = _get_model_dir_or_404(model)
    with jobdb.get_connection(model_dir) as con:
        job = jobdb.get_job(con, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        if job["status"] in ("running", "cancelling"):
            progress = jobdb.get_progress(con, job_id)
        else:
            progress = []

    resp = _to_response(job)
    resp.progress = progress
    return resp


@router.delete("/{job_id}", response_model=JobResponse)
def cancel_job(job_id: str, model: str = Query(...)):
    """Request cancellation of a running job."""
    model_dir = _get_model_dir_or_404(model)
    with jobdb.get_connection(model_dir) as con:
        job = jobdb.get_job(con, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        if job["status"] != "running":
            raise HTTPException(
                status_code=409,
                detail=f"Job '{job_id}' is not running (status: {job['status']}).",
            )
        jobdb.update_job(con, job_id, status="cancelling")
        job = jobdb.get_job(con, job_id)

    _log.info("Job cancel requested", extra={"job_id": job_id, "model": model})
    return _to_response(job)
