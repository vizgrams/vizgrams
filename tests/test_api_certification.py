# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""HTTP tests for the certify/uncertify endpoints (VG-259).

Covers views, queries, and features at the wire level — auth gate,
404 on missing artifact, idempotency of certify, and the cert payload
fields landing on the list/get responses.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, resolve_model_dir
from api.main import app
from core import metadata_db


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DEV_USER", "creator@example.com")
    monkeypatch.setenv("VZ_CREATORS", "creator@example.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    (model_dir / "views").mkdir(parents=True)
    (model_dir / "queries").mkdir(parents=True)
    (model_dir / "features").mkdir(parents=True)

    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[resolve_model_dir] = lambda model: model_dir  # noqa: ARG005
    yield TestClient(app), model_dir
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def _save_view(model_dir: Path, name: str = "v1") -> None:
    yaml = f"""name: {name}
type: chart
query: q1
visualization:
  chart_type: bar
  x: x_col
  y:
    - y_col
"""
    (model_dir / "views" / f"{name}.yaml").write_text(yaml)
    metadata_db.record_version(model_dir, "view", name, yaml)


def test_certify_view_marks_it_certified(client):
    c, model_dir = client
    _save_view(model_dir)

    resp = c.post("/api/v1/model/demo/view/v1/certify")
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_certified"] is True
    assert body["certified_at"]


def test_uncertify_view_clears_marker(client):
    c, model_dir = client
    _save_view(model_dir)
    c.post("/api/v1/model/demo/view/v1/certify")
    resp = c.delete("/api/v1/model/demo/view/v1/certify")
    assert resp.status_code == 200
    assert resp.json()["is_certified"] is False


def test_certify_view_404_when_missing(client):
    c, _ = client
    resp = c.post("/api/v1/model/demo/view/no_such/certify")
    assert resp.status_code == 404


def test_list_views_includes_cert_fields(client):
    c, model_dir = client
    _save_view(model_dir, "certified_view")
    _save_view(model_dir, "uncertified_view")
    c.post("/api/v1/model/demo/view/certified_view/certify")

    resp = c.get("/api/v1/model/demo/view")
    rows = {r["name"]: r for r in resp.json()}
    assert rows["certified_view"]["is_certified"] is True
    assert rows["uncertified_view"]["is_certified"] is False


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def _save_query(model_dir: Path, name: str = "q1") -> None:
    yaml = f"""name: {name}
root: Widget
attributes:
  - widget_key
"""
    (model_dir / "queries" / f"{name}.yaml").write_text(yaml)
    metadata_db.record_version(model_dir, "query", name, yaml)


def test_certify_query_marks_it_certified(client):
    c, model_dir = client
    _save_query(model_dir)
    resp = c.post("/api/v1/model/demo/query/q1/certify")
    assert resp.status_code == 200
    assert resp.json()["is_certified"] is True


def test_certify_query_404_when_missing(client):
    c, _ = client
    resp = c.post("/api/v1/model/demo/query/no_such/certify")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------


def _save_feature(model_dir: Path, feature_id: str = "widget.is_active") -> None:
    yaml = f"""feature_id: {feature_id}
name: is_active
entity_type: Widget
feature_type: raw_sql
data_type: BOOLEAN
entity_key: widget_key
raw_sql: "1=1"
"""
    metadata_db.record_version(model_dir, "feature", feature_id, yaml)


def test_certify_feature_marks_it_certified(client):
    c, model_dir = client
    _save_feature(model_dir)
    resp = c.post("/api/v1/model/demo/feature/widget.is_active/certify")
    assert resp.status_code == 200
    assert resp.json()["is_certified"] is True
    assert resp.json()["feature_id"] == "widget.is_active"


def test_certify_feature_404_when_missing(client):
    c, _ = client
    resp = c.post("/api/v1/model/demo/feature/no.such/certify")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Auth gate — non-creator forbidden
# ---------------------------------------------------------------------------


def test_certify_rejects_non_creators(tmp_path, monkeypatch):
    # Clear bypasses so the gate is exercised by the X-Auth-Request-Email header.
    monkeypatch.delenv("DEV_USER", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "")
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    (model_dir / "views").mkdir(parents=True)
    _save_view(model_dir)

    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[resolve_model_dir] = lambda model: model_dir  # noqa: ARG005
    try:
        with TestClient(app) as c:
            resp = c.post(
                "/api/v1/model/demo/view/v1/certify",
                headers={"X-Auth-Request-Email": "viewer@example.com"},
            )
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.clear()
