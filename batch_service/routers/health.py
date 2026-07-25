# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Aggregate model-health snapshot: one row per operational target, with
last-success, next-run, and failure-cap state — the numbers a human needs
to decide whether a model is healthy or has silently stalled.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from batch_service.config import get_model_dir

router = APIRouter(prefix="/api/v1/health", tags=["health"])


class HealthTarget(BaseModel):
    name: str
    cron: str | None
    last_success: str | None
    last_attempt_at: str | None
    last_attempt_status: str | None
    next_run: str | None
    failures_since_success: int
    failure_cap: int
    cap_hit: bool
    scheduled_children: list[str] = []


class HealthSection(BaseModel):
    operation: str
    targets: list[HealthTarget]


class HealthReport(BaseModel):
    model: str
    sections: list[HealthSection]


@router.get("", response_model=HealthReport)
def get_health(model: str = Query(...)):
    from batch.schedule import health_summary

    try:
        model_dir = get_model_dir(model)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return health_summary(model_dir)
