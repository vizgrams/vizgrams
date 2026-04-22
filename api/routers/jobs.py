# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException, Query

from api.batch_client import BatchServiceError, cancel_job, get_job, list_jobs
from api.dependencies import resolve_model_dir
from api.schemas.job import JobOut
from api.services.job_service import JobStatus

router = APIRouter(prefix="/model/{model}/job", tags=["jobs"])


def _to_job_out(job: dict) -> JobOut:
    """Convert a batch service job dict to the main API's JobOut schema."""
    result = None
    if job.get("records") is not None:
        result = {"records_written": job["records"], "duration_s": job.get("duration_s")}
    operation = job.get("operation", "extract")
    tool_value = job.get("tool")
    extractor = tool_value if operation == "extract" else None
    entity = tool_value if operation in ("map", "materialize") else None
    return JobOut(
        job_id=job["job_id"],
        model=job["model"],
        operation=operation,
        status=job["status"],
        started_at=job["started_at"],
        extractor=extractor,
        entity=entity,
        completed_at=job.get("completed_at"),
        result=result,
        error=job.get("error"),
        progress=job.get("progress", []),
        warnings=[],
    )


@router.get("", response_model=list[JobOut])
def list_jobs_route(
    model: str,
    status: JobStatus | None = Query(None),
    operation: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    model_dir=Depends(resolve_model_dir),
):
    try:
        jobs = list_jobs(
            model,
            status=status.value if status else None,
            operation=operation,
            limit=limit,
        )
        return [_to_job_out(j) for j in jobs]
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/{job_id}", response_model=JobOut)
def get_job_route(
    model: str,
    job_id: str,
    model_dir=Depends(resolve_model_dir),
):
    try:
        job = get_job(job_id, model)
        return _to_job_out(job)
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.delete("/{job_id}", response_model=JobOut)
def cancel_job_route(
    model: str,
    job_id: str,
    model_dir=Depends(resolve_model_dir),
):
    try:
        job = cancel_job(job_id, model)
        return _to_job_out(job)
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
