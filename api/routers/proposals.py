# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Propose-change REST surface (Epic 26 VG-295).

Two route families:
- Model-scoped: ``/api/v1/model/{m}/proposals`` — create + list
- Cross-model: ``/api/v1/proposals/{id}/approve`` and
  ``/api/v1/proposals/{id}/reject`` — decisions are stored once,
  fetchable by id alone; an admin / owner doesn't need to know the
  containing model_id to decide.

Plus the bell endpoints on ``/api/v1/me``:
- ``GET /me/notifications`` — list unresolved for the current user
- ``GET /me/notifications/count`` — cheap badge query
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import (
    get_current_user_email,
    require_member,
    resolve_model_dir,
)
from api.schemas.proposal import (
    NotificationOut,
    Proposal,
    ProposalCreate,
    ProposalDecision,
    ProposalRejection,
)
from api.services import proposals_service
from api.services.proposals_service import ProposalAuthError
from core import notifications_db, proposals_db
from core.proposals_db import PROPOSAL_KINDS, ProposalStateError
from core.rbac import is_system_admin

# ---------------------------------------------------------------------------
# Model-scoped: create + list
# ---------------------------------------------------------------------------

model_router = APIRouter(
    prefix="/model/{model}/proposals", tags=["proposals"],
)


@model_router.post("", response_model=Proposal, status_code=201)
def create_proposal(
    body: ProposalCreate,
    model_dir: str = Depends(resolve_model_dir),
    email: str = Depends(require_member),
):
    """Member endpoint — propose a change to a governed artifact."""
    if body.artifact_kind not in PROPOSAL_KINDS:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown artifact_kind {body.artifact_kind!r}",
        )
    try:
        return proposals_service.create(
            model_dir=model_dir,
            proposed_by=email,
            artifact_kind=body.artifact_kind,
            artifact_name=body.artifact_name,
            reason=body.reason,
            entity_name=body.entity_name,
            before_yaml=body.before_yaml,
            after_yaml=body.after_yaml,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@model_router.get("", response_model=list[Proposal])
def list_proposals(
    model_dir: str = Depends(resolve_model_dir),
    _email: str = Depends(require_member),
    entity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    artifact_kind: str | None = Query(default=None),
    artifact_name: str | None = Query(default=None),
):
    """List proposals in this model. Filterable by entity / status /
    artifact. Any authenticated member can read — the audit trail is
    intentionally public to everyone in the model."""
    try:
        from pathlib import Path
        return proposals_db.list_proposals(
            model_id=Path(model_dir).name,
            entity_name=entity,
            status=status,
            artifact_kind=artifact_kind,
            artifact_name=artifact_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Cross-model: decision endpoints (by proposal id)
# ---------------------------------------------------------------------------

decision_router = APIRouter(prefix="/proposals", tags=["proposals"])


def _load_model_dir(proposal_id: str) -> tuple[dict, str]:
    """Resolve the proposal + its model_dir on disk so the service has
    what it needs for ownership checks. Reuses ``get_models_dir`` to
    construct the path so test overrides apply naturally."""
    from pathlib import Path

    from api.dependencies import get_models_dir
    p = proposals_db.get_proposal(proposal_id)
    if p is None:
        raise HTTPException(status_code=404, detail=f"Proposal {proposal_id!r} not found")
    md = get_models_dir() / p["model_id"]
    if not md.is_dir():
        # Most likely a stale proposal pointing at a removed model.
        # Surface as 410 so the UI can prompt to refresh.
        raise HTTPException(status_code=410, detail=f"Model {p['model_id']!r} no longer exists")
    return p, str(md) if isinstance(md, Path) else md


@decision_router.post("/{proposal_id}/approve", response_model=Proposal)
def approve(
    proposal_id: str,
    body: ProposalDecision,
    email: str = Depends(get_current_user_email),
):
    p, md = _load_model_dir(proposal_id)
    admin = is_system_admin(email)
    try:
        return proposals_service.approve(
            model_dir=md,
            proposal_id=proposal_id,
            actor=email,
            is_admin=admin,
            comment=body.comment,
        )
    except ProposalAuthError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ProposalStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@decision_router.post("/{proposal_id}/reject", response_model=Proposal)
def reject(
    proposal_id: str,
    body: ProposalRejection,
    email: str = Depends(get_current_user_email),
):
    p, md = _load_model_dir(proposal_id)
    admin = is_system_admin(email)
    try:
        return proposals_service.reject(
            model_dir=md,
            proposal_id=proposal_id,
            actor=email,
            is_admin=admin,
            comment=body.comment,
        )
    except ProposalAuthError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ProposalStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Bell endpoints on /me
# ---------------------------------------------------------------------------

me_router = APIRouter(prefix="/me/notifications", tags=["proposals"])


@me_router.get("", response_model=list[NotificationOut])
def list_my_notifications(email: str = Depends(get_current_user_email)):
    """Unresolved proposal notifications for the current user, newest first."""
    return notifications_db.list_pending_for_user(email)


@me_router.get("/count")
def count_my_notifications(email: str = Depends(get_current_user_email)) -> dict:
    """Badge count — kept on its own endpoint so the bell can poll
    cheaply without pulling row payloads."""
    return {"count": notifications_db.count_pending_for_user(email)}
