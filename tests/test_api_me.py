# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for GET /api/v1/me — identity and platform role."""

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app, raise_server_exceptions=True)


def _get_me(email: str | None = None, extra_env: dict | None = None, monkeypatch=None):
    """Call /api/v1/me with optional auth header and env overrides."""
    headers = {"X-Auth-Request-Email": email} if email else {}
    if monkeypatch and extra_env:
        for k, v in extra_env.items():
            monkeypatch.setenv(k, v)
    return client.get("/api/v1/me", headers=headers)


# ---------------------------------------------------------------------------
# Unauthenticated
# ---------------------------------------------------------------------------

def test_me_unauthenticated_returns_nulls(monkeypatch):
    monkeypatch.delenv("DEV_USER", raising=False)
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.delenv("VZ_CREATORS", raising=False)
    r = client.get("/api/v1/me")
    assert r.status_code == 200
    body = r.json()
    assert body["email"] is None
    assert body["is_system_admin"] is False
    assert body["is_creator"] is False
    assert body["role"] == "viewer"


# ---------------------------------------------------------------------------
# Viewer (authenticated, no elevated role)
# ---------------------------------------------------------------------------

def test_me_viewer_role(monkeypatch):
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.delenv("VZ_CREATORS", raising=False)
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "user@example.com"})
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "user@example.com"
    assert body["is_system_admin"] is False
    assert body["is_creator"] is False
    assert body["role"] == "viewer"


# ---------------------------------------------------------------------------
# Creator
# ---------------------------------------------------------------------------

def test_me_creator_role(monkeypatch):
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "creator@example.com")
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "creator@example.com"})
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "creator@example.com"
    assert body["is_system_admin"] is False
    assert body["is_creator"] is True
    assert body["role"] == "creator"


def test_me_creator_domain_wildcard(monkeypatch):
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "*@startup.io")
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "alice@startup.io"})
    body = r.json()
    assert body["role"] == "creator"
    assert body["is_creator"] is True


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

def test_me_admin_role(monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
    monkeypatch.delenv("VZ_CREATORS", raising=False)
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "admin@example.com"})
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "admin@example.com"
    assert body["is_system_admin"] is True
    assert body["is_creator"] is True  # admin implies creator
    assert body["role"] == "admin"


def test_me_admin_via_dev_user(monkeypatch):
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
    monkeypatch.delenv("VZ_CREATORS", raising=False)
    # No auth header — falls back to DEV_USER
    r = client.get("/api/v1/me")
    body = r.json()
    assert body["email"] == "dev@local.com"
    assert body["is_system_admin"] is True
    assert body["role"] == "admin"


def test_me_admin_overrides_creator_role(monkeypatch):
    """When a user is both in VZ_SYSTEM_ADMINS and VZ_CREATORS, role is 'admin'."""
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "boss@example.com")
    monkeypatch.setenv("VZ_CREATORS", "boss@example.com")
    r = client.get("/api/v1/me", headers={"X-Auth-Request-Email": "boss@example.com"})
    body = r.json()
    assert body["role"] == "admin"


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------

def test_me_response_has_required_fields(monkeypatch):
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    r = client.get("/api/v1/me")
    body = r.json()
    assert set(body.keys()) >= {"email", "is_system_admin", "is_creator", "role"}
