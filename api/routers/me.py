# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import os

from fastapi import APIRouter, Request

from core.rbac import is_creator, is_system_admin

router = APIRouter(prefix="/me", tags=["auth"])


@router.get("")
def get_me(request: Request):
    """Return the identity and system role of the current user.

    In auth mode the ForwardAuth middleware sets X-Auth-Request-Email before
    the request reaches FastAPI.  In no-auth / local dev mode, DEV_USER is
    used as a fallback.  Returns ``{"email": null}`` when neither is present.
    """
    email = request.headers.get("X-Auth-Request-Email")
    if not email:
        email = os.environ.get("DEV_USER") or None
    admin = is_system_admin(email) if email else False
    creator = is_creator(email) if email else False
    role = "admin" if admin else "creator" if creator else "viewer"
    return {
        "email": email,
        "is_system_admin": admin,
        "is_creator": creator,
        "role": role,
    }
