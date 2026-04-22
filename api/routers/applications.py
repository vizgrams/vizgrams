# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import logging

from fastapi import APIRouter, Depends, HTTPException

from core.db import BackendUnavailableError

from api.dependencies import resolve_model_dir
from api.schemas.application import ApplicationDetail, ApplicationSummary
from api.schemas.common import ValidationResult, YAMLContent
from api.services import application_service
from api.services.application_service import ApplicationValidationError
from core.version_routes import make_version_routes

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/application", tags=["applications"])


@router.get("", response_model=list[ApplicationSummary])
def list_applications(model_dir: str = Depends(resolve_model_dir)):
    return application_service.list_applications(model_dir)


@router.get("/{app}", response_model=ApplicationDetail)
def get_application(app: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return application_service.get_application(model_dir, app)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Application '{app}' not found.") from None


@router.post("/{app}/validate", response_model=ValidationResult)
def validate_application(app: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return application_service.validate_application(model_dir, app)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Application '{app}' not found.") from None


@router.put("/{app}", response_model=ApplicationDetail)
def upsert_application(
    app: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    try:
        return application_service.create_or_replace_application(model_dir, app, body.content)
    except ApplicationValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Application validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


router.include_router(make_version_routes("application", tags=["applications"]))
