# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from core.db import BackendUnavailableError

from api.batch_client import BatchServiceError
from api.dependencies import get_job_service, require_role, resolve_model_dir
from api.limiter import limiter
from core.rbac import ModelRole
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.extractor import ExtractorDetail
from api.schemas.job import JobOut
from api.services import extractor_service
from api.services.extractor_service import ExtractorConflictError, ExtractorValidationError
from api.services.job_service import JobService
from core.version_routes import make_version_routes

router = APIRouter(prefix="/model/{model}/tool", tags=["extractors"])


def _batch_job_to_out(job: dict) -> JobOut:
    """Convert a batch service job dict to the main API's JobOut schema."""
    result = None
    if job.get("records") is not None:
        result = {"records_written": job["records"], "duration_s": job.get("duration_s")}
    return JobOut(
        job_id=job["job_id"],
        model=job["model"],
        operation=job["operation"],
        status=job["status"],
        started_at=job["started_at"],
        extractor=job.get("tool") if job.get("operation") == "extract" else None,
        completed_at=job.get("completed_at"),
        result=result,
        error=job.get("error"),
        progress=job.get("progress", []),
        warnings=[],
    )


@router.get("/{tool}/extract", response_model=ExtractorDetail)
def get_extractor(tool: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return extractor_service.get_extractor(model_dir, tool)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Extractor for tool '{tool}' not found.") from None


@router.post("/{tool}/extract/execute", response_model=JobOut, status_code=202)
@limiter.limit("30/minute")
def execute_extractor(
    request: Request,
    model: str,
    tool: str,
    task: str | None = Query(None),
    full_refresh: bool = Query(False),
    since: str | None = Query(
        None,
        description="Override since date (ISO 8601, e.g. 2026-02-20). Takes priority over incremental checkpoint.",
    ),
    model_dir: str = Depends(resolve_model_dir),
    js: JobService = Depends(get_job_service),
    _=Depends(require_role(ModelRole.OPERATOR)),
):
    try:
        job = extractor_service.execute_extractor(
            model_dir, tool, task_name=task, full_refresh=full_refresh, since=since, job_service=js
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ExtractorConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _batch_job_to_out(job)


@router.post("/{tool}/extract/validate", response_model=ValidationResult)
def validate_extractor(tool: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return extractor_service.validate_extractor(model_dir, tool)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Extractor for tool '{tool}' not found.") from None


@router.put("/{tool}/extract", response_model=ExtractorDetail)
def upsert_extractor(
    tool: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    """Validate YAML content and write (create or overwrite) an extractor file."""
    try:
        return extractor_service.create_or_replace_extractor(model_dir, tool, body.content)
    except ExtractorValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Extractor validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


router.include_router(make_version_routes("extractor", tags=["extractors"]))
