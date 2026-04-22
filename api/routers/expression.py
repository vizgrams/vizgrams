# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import resolve_model_dir
from api.services import expression_service
from core.db import BackendUnavailableError

router = APIRouter(prefix="/model/{model}/expression", tags=["expression"])


class ValidateRequest(BaseModel):
    entity: str
    expr: str
    mode: str = "feature"  # "feature" | "measure" | "filter"


class PreviewRequest(BaseModel):
    entity: str
    expr: str
    entity_id: str | None = None


@router.post("/validate")
def validate_expression(body: ValidateRequest, model_dir: str = Depends(resolve_model_dir)):
    return expression_service.validate_expression(model_dir, body.entity, body.expr, body.mode)


@router.post("/preview")
def preview_expression(body: PreviewRequest, model_dir: str = Depends(resolve_model_dir)):
    try:
        return expression_service.preview_expression(
            model_dir, body.entity, body.expr, body.entity_id
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except BackendUnavailableError:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.get("/functions")
def list_functions(mode: str | None = None):
    return expression_service.list_functions(mode)
