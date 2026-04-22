# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException

from api.batch_client import BatchServiceError, submit_mapper_job
from api.dependencies import resolve_entity, resolve_model_dir
from api.routers.jobs import _to_job_out
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.job import JobOut
from api.schemas.mapper import MapperOut
from api.services import mapper_service
from api.services.mapper_service import MapperValidationError
from core.db import BackendUnavailableError
from core.version_routes import make_version_routes

router = APIRouter(prefix="/model/{model}/entity/{entity}/mapper", tags=["mappers"])

# Model-scoped CRUD router (not entity-scoped)
crud_router = APIRouter(prefix="/model/{model}/mapper", tags=["mappers"])


@crud_router.get("", response_model=list[MapperOut])
def list_mappers(model_dir: str = Depends(resolve_model_dir)):
    return mapper_service.list_mappers(model_dir)


@crud_router.get("/{mapper_name}", response_model=MapperOut)
def get_mapper_by_name(mapper_name: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return mapper_service.get_mapper_by_name(model_dir, mapper_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@crud_router.post("/execute-all", response_model=JobOut, status_code=202)
def execute_all_mappers(
    model: str,
    model_dir: str = Depends(resolve_model_dir),
):
    """Run all mappers for this model sequentially in a single background job."""
    mappers = mapper_service.list_mappers(model_dir)
    if not mappers:
        raise HTTPException(status_code=404, detail=f"No mappers found for model '{model}'.")
    try:
        job = submit_mapper_job(model, mapper=None, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@crud_router.put("/{mapper_name}", response_model=MapperOut)
def upsert_mapper(
    mapper_name: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate YAML content and write (create or overwrite) a mapper file."""
    try:
        return mapper_service.create_or_replace_mapper(model_dir, mapper_name, body.content)
    except MapperValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Mapper validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("", response_model=MapperOut)
def get_mapper(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    try:
        return mapper_service.get_mapper(model_dir, entity)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No mapper found for entity '{entity}'.") from None


@router.post("/execute", response_model=JobOut, status_code=202)
def execute_mapper(
    model: str,
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    try:
        mapper_info = mapper_service.get_mapper(model_dir, entity)
        mapper = mapper_info["name"]
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    try:
        job = submit_mapper_job(model, mapper=mapper, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@router.post("/validate", response_model=ValidationResult)
def validate_mapper(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    try:
        return mapper_service.validate_mapper(model_dir, entity)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No mapper found for entity '{entity}'.") from None


crud_router.include_router(make_version_routes("mapper", tags=["mappers"]))
