# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from api.dependencies import (
    author_from_principal,
    get_current_user,
    require_member,
    require_user_or_service_account,
    resolve_model_dir,
)
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.view import ViewDetail, ViewResult, ViewSummary
from api.services import view_service
from api.services.view_service import ViewValidationError
from api.version_routes import make_version_routes
from core import metadata_db
from core.db import BackendUnavailableError

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/view", tags=["views"])


@router.get("", response_model=list[ViewSummary])
def list_views(
    model_dir: str = Depends(resolve_model_dir),
    _principal: dict = Depends(require_user_or_service_account),
):
    return view_service.list_views(model_dir)


@router.get("/{view}", response_model=ViewDetail)
def get_view(
    view: str,
    model_dir: str = Depends(resolve_model_dir),
    _principal: dict = Depends(require_user_or_service_account),
):
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


@router.post("/_execute-inline", response_model=ViewResult)
def execute_inline_view(
    body: dict[str, Any] = Body(...),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    model_dir: str = Depends(resolve_model_dir),
):
    """Execute a transient view YAML (Epic 20 VG-237).

    Body shape: ``{view_yaml: str, query_yaml?: str, params?: dict[str, str]}``.

    Used by the chat to render a card from a freshly-authored view +
    optional freshly-authored query, without saving either to api.db.
    The view's ``query:`` field names the query (saved or transient).
    """
    view_yaml = body.get("view_yaml")
    if not view_yaml:
        raise HTTPException(status_code=400, detail="missing 'view_yaml' in body")
    query_yaml = body.get("query_yaml")
    params = body.get("params")
    try:
        return view_service.execute_inline_view(
            model_dir, view_yaml, query_yaml=query_yaml, params=params,
            limit=limit, offset=offset,
        )
    except BackendUnavailableError:
        raise
    except Exception as exc:
        _log.exception("execute_inline_view failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{view}/validate", response_model=ValidationResult)
def validate_view(view: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return view_service.validate_view(model_dir, view)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"View '{view}' not found.") from None


# VG-258 / VG-259: certification toggle. Creator-gated — same role bar as
# the rest of the artifact write surface.
@router.post("/{view}/certify", response_model=ViewDetail)
def certify_view(
    view: str,
    model_dir: str = Depends(resolve_model_dir),
    user_id: str = Depends(get_current_user),
    _=Depends(require_member),
):
    try:
        view_service.get_view(model_dir, view)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"View '{view}' not found.") from None
    metadata_db.certify(model_dir, "view", view, user_id=user_id)
    return view_service.get_view(model_dir, view)


@router.delete("/{view}/certify", response_model=ViewDetail)
def uncertify_view(
    view: str,
    model_dir: str = Depends(resolve_model_dir),
    _=Depends(require_member),
):
    try:
        view_service.get_view(model_dir, view)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"View '{view}' not found.") from None
    metadata_db.uncertify(model_dir, "view", view)
    return view_service.get_view(model_dir, view)


@router.put("/{view}", response_model=ViewDetail)
def upsert_view(
    view: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
    principal: dict = Depends(require_user_or_service_account),
):
    user_id, via = author_from_principal(principal)
    try:
        return view_service.create_or_replace_view(
            model_dir, view, body.content, user_id=user_id, via=via,
        )
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
