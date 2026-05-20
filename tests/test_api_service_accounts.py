# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/routers/service_accounts.py."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app, raise_server_exceptions=True)

ADMIN = "admin@example.com"
VIEWER = "viewer@example.com"

URL_BASE = "/api/v1/model/test_model/service-accounts"


@pytest.fixture
def model_dir(monkeypatch, tmp_path):
    """Create an empty test model directory and point VZ_MODELS_DIR at it."""
    models = tmp_path / "models"
    target = models / "test_model"
    target.mkdir(parents=True)
    monkeypatch.setenv("VZ_MODELS_DIR", str(models))
    return target


@pytest.fixture(autouse=True)
def _admin_env(monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", ADMIN)


def _auth(email: str) -> dict[str, str]:
    return {"X-Auth-Request-Email": email}


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

def test_create_returns_201_with_plaintext_token(model_dir: Path):
    r = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN))
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["model_id"] == "test_model"
    assert body["name"] == "ci-bot"
    assert body["token"].startswith("vzsa_")
    assert body["is_active"] is True


def test_create_rejects_duplicate_name(model_dir: Path):
    client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN))
    r = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN))
    assert r.status_code == 409


def test_create_rejects_non_admin(model_dir: Path):
    r = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(VIEWER))
    assert r.status_code == 403


def test_create_404_when_model_missing():
    r = client.post(
        "/api/v1/model/nonexistent/service-accounts",
        json={"name": "ci-bot"},
        headers=_auth(ADMIN),
    )
    assert r.status_code == 404


def test_create_validates_name(model_dir: Path):
    r = client.post(URL_BASE, json={"name": ""}, headers=_auth(ADMIN))
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def test_list_returns_only_active_by_default(model_dir: Path):
    a = client.post(URL_BASE, json={"name": "a"}, headers=_auth(ADMIN)).json()
    b = client.post(URL_BASE, json={"name": "b"}, headers=_auth(ADMIN)).json()
    client.delete(f"{URL_BASE}/{b['id']}", headers=_auth(ADMIN))

    r = client.get(URL_BASE, headers=_auth(ADMIN))
    assert r.status_code == 200
    ids = {sa["id"] for sa in r.json()}
    assert ids == {a["id"]}


def test_list_include_inactive(model_dir: Path):
    a = client.post(URL_BASE, json={"name": "a"}, headers=_auth(ADMIN)).json()
    b = client.post(URL_BASE, json={"name": "b"}, headers=_auth(ADMIN)).json()
    client.delete(f"{URL_BASE}/{b['id']}", headers=_auth(ADMIN))

    r = client.get(f"{URL_BASE}?include_inactive=true", headers=_auth(ADMIN))
    assert r.status_code == 200
    ids = {sa["id"] for sa in r.json()}
    assert ids == {a["id"], b["id"]}


def test_list_omits_token(model_dir: Path):
    client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN))
    r = client.get(URL_BASE, headers=_auth(ADMIN))
    for sa in r.json():
        assert "token" not in sa
        assert "token_hash" not in sa


def test_list_rejects_non_admin(model_dir: Path):
    r = client.get(URL_BASE, headers=_auth(VIEWER))
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

def test_get_returns_account_without_token(model_dir: Path):
    sa = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN)).json()
    r = client.get(f"{URL_BASE}/{sa['id']}", headers=_auth(ADMIN))
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == sa["id"]
    assert "token" not in body


def test_get_404_for_unknown_id(model_dir: Path):
    r = client.get(f"{URL_BASE}/not-a-real-id", headers=_auth(ADMIN))
    assert r.status_code == 404


def test_get_404_when_scope_mismatches(monkeypatch, tmp_path):
    """An SA from model A cannot be fetched via model B's URL."""
    models = tmp_path / "models"
    (models / "model_a").mkdir(parents=True)
    (models / "model_b").mkdir(parents=True)
    monkeypatch.setenv("VZ_MODELS_DIR", str(models))

    sa = client.post(
        "/api/v1/model/model_a/service-accounts",
        json={"name": "ci-bot"},
        headers=_auth(ADMIN),
    ).json()
    r = client.get(
        f"/api/v1/model/model_b/service-accounts/{sa['id']}",
        headers=_auth(ADMIN),
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# delete (revoke)
# ---------------------------------------------------------------------------

def test_delete_returns_204_and_revokes(model_dir: Path):
    sa = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN)).json()
    r = client.delete(f"{URL_BASE}/{sa['id']}", headers=_auth(ADMIN))
    assert r.status_code == 204
    # Subsequent GET still finds it but with is_active=False
    r = client.get(f"{URL_BASE}/{sa['id']}", headers=_auth(ADMIN))
    assert r.status_code == 200
    assert r.json()["is_active"] is False


def test_delete_404_for_unknown_id(model_dir: Path):
    r = client.delete(f"{URL_BASE}/not-a-real-id", headers=_auth(ADMIN))
    assert r.status_code == 404


def test_delete_rejects_non_admin(model_dir: Path):
    sa = client.post(URL_BASE, json={"name": "ci-bot"}, headers=_auth(ADMIN)).json()
    r = client.delete(f"{URL_BASE}/{sa['id']}", headers=_auth(VIEWER))
    assert r.status_code == 403
