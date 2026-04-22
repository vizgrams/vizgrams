# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Batch API endpoints: schedule inspection and manual trigger.

These endpoints proxy to the vizgrams-batch microservice. The frontend
never calls the batch service directly.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from api.batch_client import BatchServiceError, get_schedules, trigger
from api.dependencies import require_role, resolve_model_dir
from api.limiter import limiter
from core.rbac import ModelRole
from api.schemas.job import JobOut

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/batch", tags=["batch"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class ScheduleEntry(BaseModel):
    tool: str
    type: str  # "extractor" or "mapper"
    cron: str
    last_success: str | None
    next_run: str | None
    due: bool


class TriggerResult(BaseModel):
    job_id: str
    model: str
    tool: str
    status: str
    started_at: str


# ---------------------------------------------------------------------------
# GET /schedule
# ---------------------------------------------------------------------------


@router.get("/schedule", response_model=list[ScheduleEntry])
def get_schedule(model: str, model_dir=Depends(resolve_model_dir)):
    """Return cron schedule status for every scheduled extractor in the model."""
    try:
        entries = get_schedules(model)
        return [
            ScheduleEntry(
                tool=e["tool"],
                type=e.get("type", "extractor"),
                cron=e["cron"],
                last_success=e.get("last_success"),
                next_run=e.get("next_run"),
                due=e.get("due", False),
            )
            for e in entries
        ]
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# POST /trigger
# ---------------------------------------------------------------------------


@router.post("/trigger", response_model=TriggerResult, status_code=202)
@limiter.limit("30/minute")
def trigger_route(
    request: Request,
    model: str,
    model_dir=Depends(resolve_model_dir),
    tool: str | None = Query(None, description="Extractor tool to trigger. Required."),
    force: bool = Query(False, description="Ignore schedule; run even if not due."),
    full_refresh: bool = Query(False, description="Re-ingest all data from epoch."),
    _=Depends(require_role(ModelRole.OPERATOR)),
):
    """Manually trigger an extractor via the batch service.

    Returns immediately with a job ID. Poll ``GET /api/v1/model/{model}/job/{job_id}``
    for status updates.
    """
    if not tool:
        raise HTTPException(
            status_code=422,
            detail="'tool' query parameter is required.",
        )
    try:
        job = trigger(model, tool, force=force, full_refresh=full_refresh)
        return TriggerResult(
            job_id=job["job_id"],
            model=job["model"],
            tool=job["tool"],
            status=job["status"],
            started_at=job["started_at"],
        )
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
