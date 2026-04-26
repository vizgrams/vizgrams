# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

import os

from fastapi import APIRouter, Request

from core.rbac import is_creator, is_system_admin

router = APIRouter(prefix="/me", tags=["auth"])


@router.get("")
def get_me(request: Request):
    """Return the identity and system role of the current user.

    In auth mode the ForwardAuth middleware sets X-Auth-Request-Email and
    X-Auth-Request-Preferred-Username before the request reaches FastAPI.
    In no-auth / local dev mode, DEV_USER is used as a fallback.
    Returns ``{"email": null}`` when neither is present.
    """
    email = request.headers.get("X-Auth-Request-Email")
    display_name = request.headers.get("X-Auth-Request-Preferred-Username")
    provider = os.environ.get("VZ_AUTH_PROVIDER", "auth0")

    if not email:
        dev_user = os.environ.get("DEV_USER") or None
        email = dev_user
        provider = "dev" if dev_user else provider

    if not display_name and email:
        display_name = email.split("@")[0]

    admin = is_system_admin(email) if email else False
    creator = is_creator(email) if email else False
    role = "admin" if admin else "creator" if creator else "viewer"
    # VZ_HARD_LOGOUT_URL is the IdP-level logout URL (e.g. Auth0 /v2/logout).
    # The UI uses it to construct /oauth2/sign_out?rd=<encoded> for "sign out
    # of all devices". Empty in local dev — cookie-only sign-out is used instead.
    hard_logout_url = os.environ.get("VZ_HARD_LOGOUT_URL", "")
    return {
        "email": email,
        "display_name": display_name,
        "provider": provider,
        "is_system_admin": admin,
        "is_creator": creator,
        "role": role,
        "hard_logout_url": hard_logout_url,
    }
