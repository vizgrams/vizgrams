# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Schedule inspection and manual trigger endpoints.

GET  /api/v1/schedules?model=X                 View schedule status for a model
POST /api/v1/schedules/{model}/{tool}/trigger  Manually trigger an extractor
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from batch_service import db as jobdb
from batch_service import executor as jobexec
from batch_service.config import get_model_dir

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/schedules", tags=["schedules"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class ScheduleEntry(BaseModel):
    model: str
    tool: str
    type: str  # "extractor" or "mapper"
    cron: str
    enabled: bool
    last_success: str | None
    next_run: str | None
    due: bool


class TriggerResponse(BaseModel):
    job_id: str
    model: str
    tool: str
    status: str
    started_at: str
    triggered_by: str = "api"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_model_dir_or_404(model: str):
    try:
        return get_model_dir(model)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=list[ScheduleEntry])
def list_schedules(model: str = Query(...)):
    """Return cron schedule status for every scheduled extractor in the model."""
    from batch.schedule import next_run_times

    model_dir = _get_model_dir_or_404(model)
    entries = next_run_times(model_dir)

    # Sync schedules into DB (keeps the schedules table up-to-date as a cache)
    if entries:
        with jobdb.get_connection(model_dir) as con:
            for e in entries:
                jobdb.upsert_schedule(con, model, e["tool"], e["cron"])

    return [
        ScheduleEntry(
            model=model,
            tool=e["tool"],
            type=e["type"],
            cron=e["cron"],
            enabled=True,
            last_success=e["last_success"],
            next_run=e["next_run"],
            due=e["due"],
        )
        for e in entries
    ]


@router.post("/{model}/{tool}/trigger", response_model=TriggerResponse, status_code=202)
def trigger(
    model: str,
    tool: str,
    force: bool = Query(False, description="Ignore schedule; run even if not due."),
    full_refresh: bool = Query(False, description="Re-ingest all data from epoch."),
):
    """Manually trigger an extractor.

    Returns immediately with a job ID. Poll ``GET /api/v1/jobs/{job_id}?model={model}``
    for status.  Use ``force=true`` to run regardless of schedule.
    """
    from batch.schedule import extractors_due
    from engine.extractor import find_extractor

    model_dir = _get_model_dir_or_404(model)

    # Validate tool exists
    try:
        find_extractor(model_dir, tool)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Schedule gate
    if not force:
        due = extractors_due(model_dir)
        if tool not in due:
            raise HTTPException(
                status_code=409,
                detail=f"Extractor '{tool}' is not due yet. Use force=true to run it anyway.",
            )

    # Check for already-running job
    with jobdb.get_connection(model_dir) as con:
        running = [
            j for j in jobdb.list_jobs(con, model, status="running", limit=50)
            if j.get("tool") == tool
        ]
        if running:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Job '{running[0]['job_id']}' is already running for "
                    f"model '{model}' tool '{tool}'. Cancel it first."
                ),
            )

        job_id = str(uuid.uuid4())
        since_override = "1970-01-01" if full_refresh else None
        started_at = _now()
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=model,
            operation="extract",
            tool=tool,
            status="running",
            started_at=started_at,
            triggered_by="api",
        )

    jobexec.submit(model_dir, job_id, tool, None, since_override)

    _log.info(
        "Manual trigger",
        extra={"job_id": job_id, "model": model, "tool": tool, "force": force},
    )
    return TriggerResponse(
        job_id=job_id,
        model=model,
        tool=tool,
        status="running",
        started_at=started_at,
    )
