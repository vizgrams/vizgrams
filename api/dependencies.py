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

    provider = os.environ.get("VZ_AUTH_PROVIDER", "auth0")
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


def author_from_principal(principal: dict) -> tuple[str | None, str]:
    """Return ``(user_id, via)`` for the ``created_by`` / ``created_via``
    columns on artifact_versions (VG-251).

    User saves resolve to ``(uuid, 'editor')``; service-account saves
    don't have an owning user so they resolve to ``(None, 'sync')``.
    """
    if principal.get("kind") == "user":
        return principal.get("id"), "editor"
    return None, "sync"


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


# ---------------------------------------------------------------------------
# Service-account auth (machine-to-machine, scoped to one model)
# ---------------------------------------------------------------------------

def get_service_account_from_header(request: Request) -> dict | None:
    """Return the service account behind the X-API-Key header, or None.

    Returns None when the header is absent or the token doesn't match an
    active service account. Side effect: a successful verify updates
    last_used_at via verify_token.
    """
    token = request.headers.get("X-API-Key")
    if not token:
        return None
    from core.service_accounts import verify_token  # noqa: PLC0415
    return verify_token(token)


def require_service_account(
    request: Request,
    model: str = PathParam(...),
) -> dict:
    """Require a valid service-account token scoped to the path's *model*.

    Returns the service-account metadata dict (no token/hash). Raises:
      - 401 if X-API-Key is missing or invalid
      - 403 if the token is valid but scoped to a different model
    """
    sa = get_service_account_from_header(request)
    if sa is None:
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid X-API-Key.",
        )
    if sa.get("model_id") != model:
        raise HTTPException(
            status_code=403,
            detail="Service account is not authorised for this model.",
        )
    return sa


def require_user_or_service_account(
    request: Request,
    model: str = PathParam(...),
) -> dict:
    """Accept either a valid OIDC identity OR a service-account token scoped
    to the path *model*. Returns a normalised principal dict so callers don't
    need to know which auth path was used.

    Principal shape:
      ``{"kind": "user", "id": <uuid>, "email": <email>}``
      ``{"kind": "service_account", "id": <sa_id>, "model_id": <model>}``

    Used on artifact upsert/read endpoints so `vzctl sync` (CI) can call them
    via an X-API-Key while interactive users continue with OIDC.

    Raises 401 if neither auth path is valid; 403 if the SA token is scoped
    to a different model.
    """
    sa = get_service_account_from_header(request)
    if sa is not None:
        if sa.get("model_id") != model:
            raise HTTPException(
                status_code=403,
                detail="Service account is not authorised for this model.",
            )
        return {
            "kind": "service_account",
            "id": sa["id"],
            "model_id": sa["model_id"],
        }
    # Fall back to OIDC identity.
    provider, external_id, email, display_name = _resolve_identity(request)
    if not external_id:
        raise HTTPException(
            status_code=401,
            detail="Unauthenticated — provide a valid OIDC session or X-API-Key.",
        )
    uid = _user_uuid(provider, external_id, email, display_name)
    return {"kind": "user", "id": uid, "email": email}
