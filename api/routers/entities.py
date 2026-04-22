# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException

from core.db import BackendUnavailableError

from api.batch_client import BatchServiceError, submit_materialize_job
from api.dependencies import get_job_service, resolve_entity, resolve_model_dir
from api.routers.jobs import _to_job_out
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.entity import EntityCreate, EntityDetail, EntitySummary
from api.schemas.job import JobOut
from api.services import entity_service, feature_service
from api.services.entity_service import EntityValidationError
from api.services.feature_service import FeatureValidationError
from api.services.job_service import JobService
from core.version_routes import make_version_routes

router = APIRouter(prefix="/model/{model}/entity", tags=["entities"])


@router.get("", response_model=list[EntitySummary])
def list_entities(model_dir: str = Depends(resolve_model_dir)):
    return entity_service.list_entities(model_dir)


@router.post("", response_model=JobOut, status_code=202)
def create_entity(
    body: EntityCreate,
    model: str,
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate, write entity YAML, and materialize its DB table in the background."""
    try:
        entity_service.create_entity_write_only(model_dir, body.model_dump())
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EntityValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Entity validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        job = submit_materialize_job(model, entity=body.name, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@router.post("/reconcile-all", response_model=JobOut, status_code=202)
def reconcile_all(
    model: str,
    model_dir: str = Depends(resolve_model_dir),
):
    """Materialize all entities in this model's database in a background job."""
    entities = entity_service.list_entities(model_dir)
    if not entities:
        raise HTTPException(status_code=404, detail=f"No entities found for model '{model}'.")
    try:
        job = submit_materialize_job(model, entity=None, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@router.get("/{entity}", response_model=EntityDetail)
def get_entity(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    try:
        return entity_service.get_entity(model_dir, entity)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Entity '{entity}' not found.") from None


@router.put("/{entity}", response_model=JobOut, status_code=202)
def upsert_entity(
    body: EntityCreate,
    model: str,
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate, write/overwrite entity YAML, and rematerialize its DB table in the background.

    Safe to call on existing entities — additive column changes only (no data loss).
    """
    try:
        entity_service.upsert_entity_write_only(model_dir, entity, body.model_dump())
    except EntityValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Entity validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    entity_name = body.name or entity
    try:
        job = submit_materialize_job(model, entity=entity_name, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@router.post("/{entity}/rematerialize", response_model=JobOut, status_code=202)
def rematerialize_entity(
    model: str,
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    """Rematerialize a single entity's DB table in the background (no YAML change)."""
    entities = {e["name"] for e in entity_service.list_entities(model_dir)}
    if entity not in entities:
        raise HTTPException(status_code=404, detail=f"Entity '{entity}' not found.")
    try:
        job = submit_materialize_job(model, entity=entity, triggered_by="api")
    except BatchServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _to_job_out(job)


@router.get("/{entity}/feature-values")
def get_feature_values(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    """Return all computed feature values for an entity: {entity_id: {feature_id: value}}."""
    return entity_service.get_feature_values_for_entity(model_dir, entity)


@router.put("/{entity}/feature/{feature_id}")
def update_feature(
    feature_id: str,
    body: dict,
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    """Update a feature. Accepts either full YAML content ({"content": "..."}) or
    an expression-only update ({"expr": "..."}) for inline editing from entity detail views."""
    try:
        if "content" in body:
            return feature_service.create_or_replace_feature(
                model_dir, entity, feature_id, body["content"]
            )
        return entity_service.save_feature_expr(model_dir, feature_id, body.get("expr", ""))
    except FeatureValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Feature validation failed.", "errors": exc.errors},
        ) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{entity}/yaml", response_model=EntityDetail)
def save_entity_yaml(
    body: YAMLContent,
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    """Overwrite the entity's ontology YAML directly (no materialization triggered)."""
    try:
        return entity_service.save_entity_yaml(model_dir, entity, body.content)
    except EntityValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Entity validation failed.", "errors": exc.errors},
        ) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{entity}/validate", response_model=ValidationResult)
def validate_entity(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    try:
        return entity_service.validate_entity(model_dir, entity)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Entity '{entity}' not found.") from None


router.include_router(make_version_routes("entity", tags=["entities"]))
