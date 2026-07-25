# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""
Charts router — the unified create/edit surface a "chart" is from the
user's point of view (one query + one visualization, saved together).

Internally a chart still maps to two YAML files: a query and a view.
This router orchestrates writing them in lockstep so the UI can ship a
single Save button. Standalone query / view endpoints remain available
for power users and the chat tooling that compose them independently.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import (
    author_from_principal,
    require_user_or_service_account,
    resolve_model_dir,
)
from api.schemas.query import QueryDetail
from api.schemas.view import ViewDetail
from api.services import chart_service, query_service, view_service
from api.services.query_service import QueryValidationError
from api.services.view_service import ViewValidationError

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/chart", tags=["charts"])


class ChartUpsert(BaseModel):
    """Atomic save of a chart. Two shapes are accepted:

    * ``chart_yaml`` — the modern one-file shape. Server splits it into a
      query + view pair sharing the chart's name.
    * ``query_yaml`` + ``view_yaml`` — the legacy split shape. Preserved
      because the LLM tool loop and standalone editors still compose them
      independently and shouldn't be forced through the split/join round-
      trip.

    Exactly one of the two shapes must be provided.
    """
    chart_yaml: str | None = None
    query_yaml: str | None = None
    view_yaml: str | None = None


class ChartOut(BaseModel):
    query: QueryDetail
    view: ViewDetail
    chart_yaml: str


class ChartGet(BaseModel):
    name: str
    chart_yaml: str


@router.put("/{chart}", response_model=ChartOut)
def upsert_chart(
    chart: str,
    body: ChartUpsert,
    model_dir: str = Depends(resolve_model_dir),
    principal: dict = Depends(require_user_or_service_account),
):
    """Save the query + view that back a chart, atomically.

    On view validation failure, restore the prior query content (or
    delete the query if it didn't exist before) so we don't leave half-
    applied state behind. The chart name is used for both the query and
    the view, keeping the mental model simple ("one thing named X").
    """
    user_id, via = author_from_principal(principal)

    # Resolve the payload shape → (query_yaml, view_yaml). Only one shape
    # may be supplied; otherwise the request is ambiguous.
    if body.chart_yaml is not None:
        if body.query_yaml is not None or body.view_yaml is not None:
            raise HTTPException(
                status_code=422,
                detail="Provide either chart_yaml OR query_yaml+view_yaml, not both.",
            )
        try:
            query_yaml, view_yaml = chart_service.split_chart_yaml(body.chart_yaml, chart)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={"message": str(exc), "stage": "split"},
            ) from exc
    else:
        if body.query_yaml is None or body.view_yaml is None:
            raise HTTPException(
                status_code=422,
                detail="Provide chart_yaml, or both query_yaml and view_yaml.",
            )
        query_yaml, view_yaml = body.query_yaml, body.view_yaml

    # Snapshot the previous query content so we can roll back if the view
    # save fails. None means "didn't exist before — delete on rollback".
    # Service returns a dict — access via subscript, not attribute.
    try:
        prev_query = query_service.get_query(model_dir, chart)
        prev_query_yaml: str | None = prev_query.get("raw_yaml")
    except KeyError:
        prev_query_yaml = None

    # Step 1: write the query. If this fails, nothing has changed yet.
    try:
        query_out = query_service.create_or_replace_query(
            model_dir, chart, query_yaml, user_id=user_id, via=via,
        )
    except QueryValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"message": "Query validation failed.", "errors": exc.errors,
                    "stage": "query"},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail={"message": str(exc), "stage": "query"},
        ) from exc

    # Step 2: write the view. On failure, restore the prior query content
    # (best-effort). If the query is brand new and the view fails, it stays
    # in place — the user fixes the view yaml + retries. Leaving an unused
    # query is recoverable (just delete the standalone artifact); having
    # the user re-enter a multi-line query they thought they saved isn't.
    try:
        view_out = view_service.create_or_replace_view(
            model_dir, chart, view_yaml, user_id=user_id, via=via,
        )
    except ViewValidationError as exc:
        if prev_query_yaml is not None:
            try:
                query_service.create_or_replace_query(
                    model_dir, chart, prev_query_yaml, user_id=user_id, via=via,
                )
            except Exception:  # noqa: BLE001 - best-effort rollback
                _log.exception("Chart %s: query rollback failed after view error", chart)
        raise HTTPException(
            status_code=422,
            detail={"message": "View validation failed.", "errors": exc.errors,
                    "stage": "view"},
        ) from exc
    except Exception as exc:
        if prev_query_yaml is not None:
            try:
                query_service.create_or_replace_query(
                    model_dir, chart, prev_query_yaml, user_id=user_id, via=via,
                )
            except Exception:  # noqa: BLE001 - best-effort rollback
                _log.exception("Chart %s: query rollback failed after view error", chart)
        raise HTTPException(
            status_code=400, detail={"message": str(exc), "stage": "view"},
        ) from exc

    # Compose from what we just wrote so the response reflects the exact
    # bytes on disk, not the caller's input — this catches YAML round-trip
    # quirks that would otherwise only bite on the next GET. Service
    # returns dicts; access via subscript.
    chart_yaml = chart_service.join_chart_yaml(
        query_out.get("raw_yaml"), view_out.get("raw_yaml"),
    )
    return ChartOut(query=query_out, view=view_out, chart_yaml=chart_yaml)


@router.get("/{chart}", response_model=ChartGet)
def get_chart(
    chart: str,
    model_dir: str = Depends(resolve_model_dir),
    _principal: dict = Depends(require_user_or_service_account),
):
    """Return the composed one-file chart YAML for the single-editor UI."""
    from pathlib import Path
    chart_yaml = chart_service.compose_chart_yaml(Path(model_dir), chart)
    # An entirely empty compose means neither the query nor the view exists.
    if not chart_yaml.strip() or chart_yaml.strip() == "{}":
        raise HTTPException(status_code=404, detail=f"Chart '{chart}' not found.")
    return ChartGet(name=chart, chart_yaml=chart_yaml)
