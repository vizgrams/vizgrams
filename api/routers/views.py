# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from core.db import BackendUnavailableError

from api.dependencies import resolve_model_dir
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.view import ViewDetail, ViewResult, ViewSummary
from api.services import view_service
from api.services.view_service import ViewValidationError
from core.version_routes import make_version_routes

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/view", tags=["views"])


@router.get("", response_model=list[ViewSummary])
def list_views(model_dir: str = Depends(resolve_model_dir)):
    return view_service.list_views(model_dir)


@router.get("/{view}", response_model=ViewDetail)
def get_view(view: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return view_service.get_view(model_dir, view)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"View '{view}' not found.") from None


@router.post("/{view}/execute", response_model=ViewResult)
def execute_view(
    view: str,
    body: dict[str, Any] | None = Body(None),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    model_dir: str = Depends(resolve_model_dir),
):
    params = (body or {}).get("params") if body else None
    try:
        return view_service.execute_view(model_dir, view, limit=limit, offset=offset, params=params)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        _log.exception("execute_view failed for view '%s'", view)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{view}/validate", response_model=ValidationResult)
def validate_view(view: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return view_service.validate_view(model_dir, view)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"View '{view}' not found.") from None


@router.put("/{view}", response_model=ViewDetail)
def upsert_view(
    view: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    try:
        return view_service.create_or_replace_view(model_dir, view, body.content)
    except ViewValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "View validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


router.include_router(make_version_routes("view", tags=["views"]))
