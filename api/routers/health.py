# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Aggregate model-health endpoint. Proxies to the batch service; the UI
uses this to render the Health page (one card per operation with per-target
status, cap-hit state, and next-run info).
"""

from fastapi import APIRouter, Depends, HTTPException

from api.batch_client import BatchServiceError, get_health
from api.dependencies import require_role, resolve_model_dir
from core.rbac import ModelRole

router = APIRouter(prefix="/model/{model}/health", tags=["health"])


@router.get("")
def get_model_health(
    model: str,
    model_dir=Depends(resolve_model_dir),
    _=Depends(require_role(ModelRole.OPERATOR)),
) -> dict:
    try:
        return get_health(model)
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
