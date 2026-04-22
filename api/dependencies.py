# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""FastAPI dependency factories shared across routers."""

import os
import re
from pathlib import Path

from fastapi import Depends, HTTPException, Request
from fastapi import Path as PathParam

from api.services.job_service import JobService
from api.services.job_service import job_service as _job_service
from core.rbac import ModelRole, get_model_role, is_creator, is_system_admin

_ENTITY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# ---------------------------------------------------------------------------
# User identity resolution
# ---------------------------------------------------------------------------

# In-memory cache: (provider, external_id) -> internal UUID.
# Avoids a DB round-trip on every request for already-seen users.
# Safe for a single-process deployment; cleared on restart (users table is the
# source of truth).
_user_id_cache: dict[tuple[str, str], str] = {}


def _resolve_identity(request: Request) -> tuple[str, str, str | None, str | None]:
    """Extract (provider, external_id, email, display_name) from the request.

    Priority for external_id:
      1. X-Auth-Request-User  (stable sub/username from oauth2-proxy)
      2. X-Auth-Request-Email (fallback for proxies that don't forward User)
      3. DEV_USER env var      (local development only)

    Provider is taken from VZ_AUTH_PROVIDER env var (default "dex").
    DEV_USER requests always use provider "dev".
    """
    external_id = request.headers.get("X-Auth-Request-User")
    email = request.headers.get("X-Auth-Request-Email")
    display_name = request.headers.get("X-Auth-Request-Preferred-Username")

    if not external_id:
        external_id = email  # fallback

    if not external_id:
        dev_user = os.environ.get("DEV_USER")
        if dev_user:
            external_id = dev_user
            email = email or dev_user
            return "dev", external_id, email, display_name

    if not external_id:
        return "", "", None, None

    provider = os.environ.get("VZ_AUTH_PROVIDER", "dex")
    return provider, external_id, email, display_name


def _user_uuid(provider: str, external_id: str, email: str | None, display_name: str | None) -> str:
    """Resolve (provider, external_id) to an internal UUID, with caching."""
    cache_key = (provider, external_id)
    if cache_key in _user_id_cache:
        return _user_id_cache[cache_key]
    from core.vizgrams_db import resolve_user
    # Derive a display name from email if none provided
    if not display_name and email:
        display_name = email.split("@")[0]
    user_id = resolve_user(provider, external_id, email=email, display_name=display_name)
    _user_id_cache[cache_key] = user_id
    return user_id


def get_current_user_email(request: Request) -> str:
    """Return the authenticated user's email (or DEV_USER) for RBAC checks.

    RBAC pattern matching (VZ_SYSTEM_ADMINS, VZ_CREATORS, model access: blocks)
    works on email/domain patterns, so we pass the raw email here rather than
    the internal UUID returned by get_current_user.
    """
    provider, external_id, email, _ = _resolve_identity(request)
    if not external_id:
        raise HTTPException(status_code=401, detail="Unauthenticated")
    # For DEV_USER, external_id IS the dev user value; email may equal it too.
    # For real auth, prefer the email header; fall back to external_id.
    return email or external_id


def get_current_user(request: Request) -> str:
    """Return the authenticated user's internal UUID.

    In production, oauth2-proxy sets X-Auth-Request-User (stable subject) and
    X-Auth-Request-Email on every request.  For local development, set DEV_USER.

    Resolves to a stable internal UUID via the users table so that identity
    changes (email rename, provider swap) don't orphan existing data.
    """
    provider, external_id, email, display_name = _resolve_identity(request)
    if not external_id:
        raise HTTPException(status_code=401, detail="Unauthenticated")
    return _user_uuid(provider, external_id, email, display_name)


def optional_user(request: Request) -> str | None:
    """Return the authenticated user's internal UUID, or None if unauthenticated.

    Use on endpoints that are publicly readable but can personalise for known
    users (e.g. feed endpoints that annotate like/save state).
    """
    provider, external_id, email, display_name = _resolve_identity(request)
    if not external_id:
        return None
    return _user_uuid(provider, external_id, email, display_name)


def get_base_dir() -> Path:
    """Return the workspace base directory."""
    env = os.environ.get("VZ_BASE_DIR")
    return Path(env) if env else Path(__file__).resolve().parents[1]


def get_models_dir(base_dir: Path = Depends(get_base_dir)) -> Path:
    """Return the models directory.

    Reads VZ_MODELS_DIR if set; otherwise falls back to {base_dir}/models.
    Depending on get_base_dir means test overrides of get_base_dir automatically
    propagate here.
    """
    env = os.environ.get("VZ_MODELS_DIR")
    return Path(env) if env else base_dir / "models"


def get_job_service() -> JobService:
    return _job_service


def resolve_entity(entity: str = PathParam(...)) -> str:
    """Validate that the entity path parameter is a safe identifier.

    Rejects anything containing dots, slashes, or other characters that could
    be used for path traversal or injection (e.g. 'FEATURES.md', '../secret').
    """
    if not _ENTITY_NAME_RE.match(entity):
        raise HTTPException(
            status_code=422,
            detail=(
            f"Invalid entity name {entity!r}. Must contain only letters, digits, and underscores, "
            f"and start with a letter."
        ),
        )
    return entity


def resolve_model_dir(
    model: str = PathParam(...),
    models_dir: Path = Depends(get_models_dir),
) -> Path:
    model_dir = models_dir / model
    if not model_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Model '{model}' not found.")
    return model_dir


def get_user_role_for_model(
    model_dir: Path = Depends(resolve_model_dir),
    email: str = Depends(get_current_user_email),
) -> ModelRole:
    """Resolve the current user's role for the model in the request path.

    Raises 403 if the user has no access to the model.
    """
    role = get_model_role(model_dir, email)
    if role is None:
        raise HTTPException(status_code=403, detail="Access denied.")
    return role


def require_role(min_role: ModelRole):
    """Return a dependency that asserts the user has at least *min_role* on the model."""
    def _dep(role: ModelRole = Depends(get_user_role_for_model)) -> ModelRole:
        if role < min_role:
            raise HTTPException(
                status_code=403,
                detail=f"Requires {min_role.name} access on this model.",
            )
        return role
    return _dep


def require_system_admin(email: str = Depends(get_current_user_email)) -> str:
    """Require the user to be a system administrator."""
    if not is_system_admin(email):
        raise HTTPException(status_code=403, detail="Requires system administrator access.")
    return email


def require_creator(email: str = Depends(get_current_user_email)) -> str:
    """Require the user to have Creator platform role or higher."""
    if not is_creator(email):
        raise HTTPException(status_code=403, detail="Requires creator access.")
    return email
