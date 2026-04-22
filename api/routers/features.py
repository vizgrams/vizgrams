# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0


from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Path as PathParam

from api.dependencies import get_job_service, resolve_entity, resolve_model_dir
from api.schemas.common import YAMLContent
from api.schemas.feature import FeatureDetail, FeatureSummary
from api.schemas.job import JobOut
from api.services import feature_service
from api.services.feature_service import FeatureValidationError
from api.services.job_service import JobService
from core.db import BackendUnavailableError
from core.version_routes import make_version_routes

router = APIRouter(prefix="/model/{model}/entity/{entity}/feature", tags=["features"])

# Plural-prefix router for entity-scoped reconcile
reconcile_router = APIRouter(prefix="/model/{model}/entity/{entity}/features", tags=["features"])

# Model-scoped router: list all features + reconcile all
model_feature_router = APIRouter(prefix="/model/{model}/feature", tags=["features"])


@router.get("", response_model=list[FeatureSummary])
def list_features(entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    return feature_service.list_features(model_dir, entity)


@router.get("/{feature}", response_model=FeatureDetail)
def get_feature(feature: str, entity: str = Depends(resolve_entity), model_dir: str = Depends(resolve_model_dir)):
    try:
        return feature_service.get_feature(model_dir, entity, feature)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Feature '{feature}' not found for entity '{entity}'.",
        ) from None


@router.put("/{feature}", response_model=FeatureDetail)
def upsert_feature(
    body: YAMLContent,
    feature: str = PathParam(...),
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate YAML content and write (create or overwrite) a feature file."""
    try:
        return feature_service.create_or_replace_feature(
            model_dir, entity, feature, body.content
        )
    except FeatureValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Feature validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@reconcile_router.post("/{feature}/reconcile", response_model=JobOut, status_code=202)
def reconcile_feature(
    model: str,
    feature: str = PathParam(...),
    entity: str = Depends(resolve_entity),
    model_dir: str = Depends(resolve_model_dir),
    js: JobService = Depends(get_job_service),
):
    """Reconcile (materialise) features for an entity.

    Use ``*`` as the feature name to reconcile all features for the entity.
    """
    try:
        job = feature_service.reconcile_entity_feature(model_dir, entity, feature, js)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JobOut(**job.__dict__)


@model_feature_router.put("/{feature_id}/yaml", response_model=FeatureSummary)
def save_feature_yaml(
    feature_id: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    """Overwrite an existing feature's YAML file by feature_id."""
    try:
        return feature_service.save_feature_by_id(model_dir, feature_id, body.content)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@model_feature_router.get("", response_model=list[FeatureSummary])
def list_all_features(
    model_dir: str = Depends(resolve_model_dir),
    entity: str | None = Query(None, description="Filter by entity name"),
):
    """List all features across all entities, optionally filtered to one entity."""
    features = feature_service.list_all_features(model_dir)
    if entity:
        features = [f for f in features if f.get("entity") == entity]
    return features


@model_feature_router.post("/reconcile", response_model=JobOut, status_code=202)
def reconcile_all_features(
    model: str,
    model_dir: str = Depends(resolve_model_dir),
    js: JobService = Depends(get_job_service),
    entity: str | None = Query(None, description="Scope reconciliation to one entity"),
):
    """Reconcile (materialise) all features in the model, optionally scoped to one entity."""
    try:
        job = feature_service.reconcile_all_features(model_dir, entity, js)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JobOut(**job.__dict__)


model_feature_router.include_router(make_version_routes("feature", tags=["features"]))
