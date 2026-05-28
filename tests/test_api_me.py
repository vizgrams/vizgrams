# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for GET /api/v1/me — identity and platform role.

Epic 26 VG-292 collapsed the role enum from three (viewer/creator/admin)
to two (admin/member). Unauthenticated requests still see 'viewer' as a
distinguishable stand-in. ``is_creator`` is gone from the response.
"""

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Unauthenticated
# ---------------------------------------------------------------------------

def test_me_unauthenticated_returns_nulls_and_viewer_role(monkeypatch):
    monkeypatch.delenv("DEV_USER", raising=False)
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    r = client.get("/api/v1/me")
    assert r.status_code == 200
    body = r.json()
    assert body["email"] is None
    assert body["is_system_admin"] is False
    assert body["role"] == "viewer"
    # is_creator was removed in VG-292 — must not reappear.
    assert "is_creator" not in body


# ---------------------------------------------------------------------------
# Member (authenticated, non-admin)
# ---------------------------------------------------------------------------

def test_me_member_role_for_authenticated_user(monkeypatch):
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "user@example.com"})
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "user@example.com"
    assert body["is_system_admin"] is False
    assert body["role"] == "member"


def test_me_vz_creators_env_var_no_longer_promotes_role(monkeypatch):
    """Pre-VG-292, VZ_CREATORS gated the 'creator' role. After the
    collapse, every authenticated user is a member regardless."""
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "alice@example.com")
    for email in ("alice@example.com", "bob@nowhere.com"):
        r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": email})
        assert r.json()["role"] == "member"


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

def test_me_admin_role(monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "admin@example.com"})
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "admin@example.com"
    assert body["is_system_admin"] is True
    assert body["role"] == "admin"


def test_me_admin_via_dev_user(monkeypatch):
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    # No auth header — falls back to DEV_USER
    r = client.get("/api/v1/me")
    body = r.json()
    assert body["email"] == "dev@local.com"
    assert body["is_system_admin"] is True
    assert body["role"] == "admin"


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------

def test_me_response_has_required_fields(monkeypatch):
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    r = client.get("/api/v1/me")
    body = r.json()
    assert {"email", "is_system_admin", "role"} <= set(body.keys())
    # is_creator was removed in VG-292.
    assert "is_creator" not in body
