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
    resolve_model_dir,
    require_user_or_service_account,
)
from api.schemas.query import QueryDetail
from api.schemas.view import ViewDetail
from api.services import query_service, view_service
from api.services.query_service import QueryValidationError
from api.services.view_service import ViewValidationError

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/chart", tags=["charts"])


class ChartUpsert(BaseModel):
    """Atomic save of the query + view that compose one chart."""
    query_yaml: str
    view_yaml: str


class ChartOut(BaseModel):
    query: QueryDetail
    view: ViewDetail


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

    # Snapshot the previous query content so we can roll back if the view
    # save fails. None means "didn't exist before — delete on rollback".
    try:
        prev_query = query_service.get_query(model_dir, chart)
        prev_query_yaml: str | None = prev_query.raw_yaml
    except KeyError:
        prev_query_yaml = None

    # Step 1: write the query. If this fails, nothing has changed yet.
    try:
        query_out = query_service.create_or_replace_query(
            model_dir, chart, body.query_yaml, user_id=user_id, via=via,
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
            model_dir, chart, body.view_yaml, user_id=user_id, via=via,
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

    return ChartOut(query=query_out, view=view_out)
