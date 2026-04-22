# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore router: fetch entity records and traverse relationships by ID."""

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import resolve_model_dir
from api.services import explore_service

router = APIRouter(prefix="/model/{model}/explore", tags=["explore"])


@router.get("/{entity}/{id}")
def get_entity_record(
    entity: str,
    id: str,
    model_dir: str = Depends(resolve_model_dir),
):
    """Return a single entity record by primary key, with relationship stubs."""
    try:
        return explore_service.get_entity_record(model_dir, entity, id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{entity}/{id}/related/{relationship}")
def get_related_entities(
    entity: str,
    id: str,
    relationship: str,
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    model_dir: str = Depends(resolve_model_dir),
):
    """Return entities related to a given record via a named relationship."""
    try:
        return explore_service.get_related_entities(
            model_dir, entity, id, relationship, limit=limit, offset=offset
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
