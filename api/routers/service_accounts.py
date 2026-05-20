# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Admin endpoints for managing per-model service accounts.

Service accounts are issued and revoked only by system administrators — these
endpoints are explicitly NOT accepting `X-API-Key` auth themselves (an SA
cannot mint another SA), only OIDC. This keeps the bootstrap chain rooted
in human-controlled credentials.

Endpoints:
  POST   /api/v1/model/{model}/service-accounts          create + return token (once)
  GET    /api/v1/model/{model}/service-accounts          list (no tokens)
  GET    /api/v1/model/{model}/service-accounts/{sa_id}  fetch one (no token)
  DELETE /api/v1/model/{model}/service-accounts/{sa_id}  revoke (soft-delete)
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam
from pydantic import BaseModel, Field

from api.dependencies import (
    get_current_user,
    require_system_admin,
    resolve_model_dir,
)
from core import service_accounts as sa_db

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/model/{model}/service-accounts", tags=["service-accounts"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ServiceAccountCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64,
                      description="Display name unique within the model.")


class ServiceAccountSummary(BaseModel):
    """Service-account metadata without the secret token."""
    id: str
    model_id: str
    name: str
    created_by: str
    created_at: str
    last_used_at: str | None = None
    is_active: bool


class ServiceAccountCreated(ServiceAccountSummary):
    """Returned exactly once at creation time. Includes the plaintext token."""
    token: str = Field(..., description="Plaintext token — shown only here. Store it now.")


# ---------------------------------------------------------------------------
# Endpoints — all admin-only via require_system_admin
# ---------------------------------------------------------------------------

@router.post("", response_model=ServiceAccountCreated, status_code=201)
def create_service_account(
    body: ServiceAccountCreate,
    model: str = PathParam(...),
    _model_dir=Depends(resolve_model_dir),       # 404 if model doesn't exist
    _admin: str = Depends(require_system_admin), # 403 if not an admin
    actor_id: str = Depends(get_current_user),
):
    """Mint a fresh service account for *model*. Returns the plaintext token once."""
    try:
        return sa_db.create_service_account(
            model_id=model, name=body.name, created_by=actor_id,
        )
    except Exception as exc:
        # Unique-name collision → 409
        if "UNIQUE" in str(exc):
            raise HTTPException(
                status_code=409,
                detail=f"A service account named {body.name!r} already exists for this model.",
            ) from exc
        raise


@router.get("", response_model=list[ServiceAccountSummary])
def list_service_accounts(
    model: str = PathParam(...),
    include_inactive: bool = False,
    _model_dir=Depends(resolve_model_dir),
    _admin: str = Depends(require_system_admin),
):
    return sa_db.list_service_accounts(
        model_id=model, include_inactive=include_inactive,
    )


@router.get("/{sa_id}", response_model=ServiceAccountSummary)
def get_service_account(
    sa_id: str,
    model: str = PathParam(...),
    _model_dir=Depends(resolve_model_dir),
    _admin: str = Depends(require_system_admin),
):
    sa = sa_db.get_service_account(sa_id)
    if sa is None or sa.get("model_id") != model:
        raise HTTPException(status_code=404, detail="Service account not found.")
    return sa


@router.delete("/{sa_id}", status_code=204)
def revoke_service_account(
    sa_id: str,
    model: str = PathParam(...),
    _model_dir=Depends(resolve_model_dir),
    _admin: str = Depends(require_system_admin),
):
    sa = sa_db.get_service_account(sa_id)
    if sa is None or sa.get("model_id") != model:
        raise HTTPException(status_code=404, detail="Service account not found.")
    sa_db.revoke_service_account(sa_id)
    # No body for 204; FastAPI returns Response automatically.
