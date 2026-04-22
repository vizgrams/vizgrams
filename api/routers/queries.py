# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import csv as csv_module
import io
import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from core.db import BackendUnavailableError

_log = logging.getLogger(__name__)
from fastapi.responses import StreamingResponse

from api.dependencies import resolve_model_dir
from api.schemas.common import ValidationResult, YAMLContent
from api.schemas.query import QueryDetail, QueryResult, QuerySummary
from api.services import query_service
from api.services.query_service import QueryValidationError
from core.version_routes import make_version_routes

router = APIRouter(prefix="/model/{model}/query", tags=["queries"])


@router.post("/execute-inline", response_model=QueryResult)
def execute_inline(
    body: dict[str, Any] = Body(...),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    model_dir: str = Depends(resolve_model_dir),
):
    """Execute a query defined inline in the request body (no saved query file required)."""
    try:
        result = query_service.execute_inline_query(model_dir, body, limit=limit, offset=offset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        _log.exception("execute-inline failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return QueryResult(**result)


@router.get("", response_model=list[QuerySummary])
def list_queries(model_dir: str = Depends(resolve_model_dir)):
    return query_service.list_queries(model_dir)


@router.get("/{query}", response_model=QueryDetail)
def get_query(query: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return query_service.get_query(model_dir, query)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Query '{query}' not found.") from None


@router.post("/{query}/execute")
def execute_query(
    query: str,
    body: dict[str, Any] | None = Body(None),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    format: str = Query("json"),
    model_dir: str = Depends(resolve_model_dir),
):
    params = (body or {}).get("params") if body else None
    try:
        result = query_service.execute_query(model_dir, query, limit=limit, offset=offset, params=params)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if format == "csv":
        buf = io.StringIO()
        writer = csv_module.writer(buf)
        writer.writerow(result["columns"])
        writer.writerows(result["rows"])
        buf.seek(0)
        filename = f"{query}.csv"
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return QueryResult(**result)


@router.post("/{query}/validate", response_model=ValidationResult)
def validate_query(query: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return query_service.validate_query(model_dir, query)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Query '{query}' not found.") from None


@router.post("/_execute", response_model=QueryResult)
def execute_inline_yaml(
    body: dict = Body(...),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    model_dir: str = Depends(resolve_model_dir),
):
    """Execute a query defined as YAML content (no saved file required)."""
    name = body.get("name", "query")
    content = body.get("content", "")
    try:
        result = query_service.execute_inline_yaml(model_dir, name, content, limit=limit, offset=offset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        _log.exception("execute_inline_yaml failed for query '%s'", name)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return QueryResult(**result)


@router.post("/_validate", response_model=ValidationResult)
def validate_inline(
    body: dict = Body(...),
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate YAML content without saving."""
    name = body.get("name", "query")
    content = body.get("content", "")
    return query_service.validate_inline_query(model_dir, name, content)


@router.put("/{query}", response_model=QueryDetail)
def upsert_query(
    query: str,
    body: YAMLContent,
    model_dir: str = Depends(resolve_model_dir),
):
    """Validate YAML content and write (create or overwrite) a query file."""
    try:
        return query_service.create_or_replace_query(model_dir, query, body.content)
    except QueryValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Query validation failed.", "errors": exc.errors},
        ) from exc
    except BackendUnavailableError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


router.include_router(make_version_routes("query", tags=["queries"]))
