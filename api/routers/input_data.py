# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import resolve_model_dir
from api.schemas.common import YAMLContent
from api.services import input_data_service

router = APIRouter(prefix="/model/{model}/input-data", tags=["input-data"])


@router.get("", response_model=list[dict])
def list_input_files(model_dir: str = Depends(resolve_model_dir)):
    return input_data_service.list_files(model_dir)


@router.post("/{filename}", status_code=201)
def upload_input_file(
    filename: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    """Write text content to input_data/{filename}."""
    try:
        return input_data_service.upload_file(model_dir, filename, body.content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
