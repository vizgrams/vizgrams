# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

logger = logging.getLogger(__name__)

from api.dependencies import (
    get_base_dir,
    get_current_user_email,
    get_models_dir,
    require_role,
    require_system_admin,
)
from api.schemas.model import (
    AccessRule,
    AccessRulesUpdate,
    ArchiveRequest,
    ModelCreate,
    ModelDetail,
    ModelPatch,
    ModelSummary,
    SetActiveResponse,
)
from api.services import model_service
from core.rbac import ModelRole, get_model_role

router = APIRouter(prefix="/model", tags=["models"])


@router.get("", response_model=list[ModelSummary])
def list_models(
    status: str | None = Query(None),
    tag: list[str] | None = Query(None),
    models_dir=Depends(get_models_dir),
    base_dir=Depends(get_base_dir),
    email: str = Depends(get_current_user_email),
):
    all_models = model_service.list_models(models_dir, base_dir, status=status, tags=tag)
    def _name(m) -> str:
        return m.name if hasattr(m, "name") else m["name"]
    return [m for m in all_models if get_model_role(models_dir / _name(m), email) is not None]


@router.get("/{model}", response_model=ModelDetail)
def get_model(
    model: str,
    audit: bool = Query(False),
    models_dir=Depends(get_models_dir),
    _=Depends(require_role(ModelRole.VIEWER)),
):
    try:
        return model_service.get_model(models_dir, model, full_audit=audit)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model '{model}' not found.") from None


@router.post("", response_model=ModelDetail, status_code=201)
def create_model(
    data: ModelCreate,
    models_dir=Depends(get_models_dir),
    base_dir=Depends(get_base_dir),
    _=Depends(require_system_admin),
):
    try:
        return model_service.create_model(models_dir, base_dir, data.model_dump(mode="json"))
    except (FileExistsError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PermissionError as exc:
        logger.error("Permission denied creating model %r: %s", data.name, exc)
        raise HTTPException(
            status_code=500,
            detail=f"Permission denied: cannot write to models directory. "
            f"Ensure the models volume is writable by the application user.",
        ) from exc
    except Exception:
        logger.exception("Unexpected error creating model %r", data.name)
        raise


@router.patch("/{model}", response_model=ModelDetail)
def update_model(
    model: str,
    data: ModelPatch,
    models_dir=Depends(get_models_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    try:
        return model_service.update_model(models_dir, model, data.model_dump(exclude_none=True))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model '{model}' not found.") from None


@router.post("/{model}/archive", response_model=ModelDetail)
def archive_model(
    model: str,
    body: ArchiveRequest | None = None,
    models_dir=Depends(get_models_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    reason = body.reason if body else None
    try:
        result = model_service.archive_model(models_dir, model, reason=reason)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return result


@router.post("/{model}/set-active", response_model=SetActiveResponse)
def set_active(
    model: str,
    models_dir=Depends(get_models_dir),
    base_dir=Depends(get_base_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    try:
        model_service.set_active(models_dir, base_dir, model)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SetActiveResponse(active=model)


@router.delete("/{model}", status_code=204)
def delete_model(
    model: str,
    delete_files: bool = Query(False),
    models_dir=Depends(get_models_dir),
    _=Depends(require_system_admin),
):
    try:
        model_service.delete_model(models_dir, model, delete_files=delete_files)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{model}/access", response_model=list[AccessRule] | None)
def get_access_rules(
    model: str,
    _=Depends(require_system_admin),
):
    return model_service.get_access_rules(model)


@router.put("/{model}/access", response_model=list[AccessRule] | None)
def set_access_rules(
    model: str,
    body: AccessRulesUpdate,
    models_dir=Depends(get_models_dir),
    _=Depends(require_system_admin),
):
    try:
        return model_service.set_access_rules(models_dir, model, body.rules)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
