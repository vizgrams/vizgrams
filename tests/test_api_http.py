# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""HTTP integration tests using FastAPI TestClient.

Tests verify routing, status codes, response shapes, and dependency injection.
Business logic is covered by the service-level tests; these tests focus on the
HTTP contract and the router→service wiring.
"""

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, get_job_service
from api.main import app
from api.services.job_service import JobService
from core import metadata_db


def _seed(model_dir: Path, artifact_type: str, name: str, content: str) -> None:
    metadata_db.record_version(model_dir, artifact_type, name, content)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_WIDGET_ENTITY_YAML = """\
entity: Widget

identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
"""

_EXTRACTOR_YAML = """\
tasks:
  - name: fetch_items
    tool: jira
    command: boards
    params: {}
    output:
      table: raw_items
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
"""

_QUERY_YAML = """\
name: widget_count
root: Widget

measures:
  count:
    expr: count(widget_key)
"""


def _write_registry(base_dir: Path, models: dict) -> None:
    models_dir = base_dir / "models"
    models_dir.mkdir(exist_ok=True)
    (models_dir / "registry.yaml").write_text(
        yaml.dump({"models": models}, default_flow_style=False)
    )


def _scaffold_model(base_dir: Path, name: str) -> Path:
    model_dir = base_dir / "models" / name
    for sub in ("extractors", "ontology", "mappers", "features", "queries", "input_data", "data"):
        (model_dir / sub).mkdir(parents=True, exist_ok=True)
    (model_dir / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return model_dir


def _configure_ch(model_dir: Path, ch_backend) -> None:
    """Overwrite model config.yaml to point to the ch_backend test database."""
    (model_dir / "config.yaml").write_text(
        f"database:\n"
        f"  backend: clickhouse\n"
        f"  database: {ch_backend.database}\n"
        f"  host: localhost\n"
        f"  port: 8123\n"
    )


@pytest.fixture
def client(tmp_path, fake_batch_client):
    """TestClient with overridden get_base_dir and a fresh JobService per test."""
    job_svc = JobService()

    def _base_dir():
        return tmp_path

    def _job_service():
        return job_svc

    app.dependency_overrides[get_base_dir] = _base_dir
    app.dependency_overrides[get_job_service] = _job_service
    yield TestClient(app), tmp_path, job_svc
    app.dependency_overrides.clear()


@pytest.fixture
def client_with_model(client):
    test_client, tmp_path, job_svc = client
    _write_registry(tmp_path, {
        "testmodel": {"display_name": "Test", "status": "active", "tags": [], "description": ""},
    })
    model_dir = _scaffold_model(tmp_path, "testmodel")
    return test_client, tmp_path, job_svc, model_dir


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def test_healthz(client):
    test_client, *_ = client
    resp = test_client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def test_list_models_empty(client):
    test_client, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = test_client.get("/api/v1/model")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_models_returns_registered_models(client):
    test_client, tmp_path, _ = client
    _write_registry(tmp_path, {
        "alpha": {
            "display_name": "Alpha",
            "status": "active",
            "tags": [],
            "description": "",
            "owner": "test",
            "created_at": "2026-01-01T00:00:00Z",
        },
    })
    resp = test_client.get("/api/v1/model")
    assert resp.status_code == 200
    names = [m["name"] for m in resp.json()]
    assert "alpha" in names


def test_get_model_not_found(client):
    test_client, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = test_client.get("/api/v1/model/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Model not found → 404 on any sub-resource
# ---------------------------------------------------------------------------

def test_entity_list_404_when_model_not_found(client):
    test_client, *_ = client
    resp = test_client.get("/api/v1/model/nonexistent/entity")
    assert resp.status_code == 404


def test_extractor_list_404_when_model_not_found(client):
    test_client, *_ = client
    resp = test_client.get("/api/v1/model/nonexistent/tool/jira/extract")
    assert resp.status_code == 404


def test_query_list_404_when_model_not_found(client):
    test_client, *_ = client
    resp = test_client.get("/api/v1/model/nonexistent/query")
    assert resp.status_code == 404


def test_job_list_404_when_model_not_found(client):
    test_client, *_ = client
    resp = test_client.get("/api/v1/model/nonexistent/job")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

def test_list_entities_empty_for_new_model(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/entity")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_entity_404_when_not_found(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/entity/NonExistent")
    assert resp.status_code == 404


def test_list_entities_returns_entity_after_write(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = test_client.get("/api/v1/model/testmodel/entity")
    assert resp.status_code == 200
    names = [e["name"] for e in resp.json()]
    assert "Widget" in names


def test_get_entity_200_when_found(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = test_client.get("/api/v1/model/testmodel/entity/Widget")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Widget"


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def test_list_extractors_empty(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/tool")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_extractor_404_when_not_found(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/tool/nonexistent_tool/extract")
    assert resp.status_code == 404


def test_get_extractor_after_file_written(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = test_client.get("/api/v1/model/testmodel/tool/jira/extract")
    assert resp.status_code == 200
    assert resp.json()["tool"] == "jira"


def test_execute_extractor_returns_202(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = test_client.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert data["status"] == "running"


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def test_list_queries_empty(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/query")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_query_404_when_not_found(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/query/nonexistent")
    assert resp.status_code == 404


def test_list_queries_returns_query(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    resp = test_client.get("/api/v1/model/testmodel/query")
    assert resp.status_code == 200
    names = [q["name"] for q in resp.json()]
    assert "widget_count" in names


def test_execute_query_404_when_no_db(client_with_model):
    test_client, _, _, model_dir = client_with_model
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    resp = test_client.post("/api/v1/model/testmodel/query/widget_count/execute")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def test_list_jobs_empty(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/job")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_job_404_when_not_found(client_with_model):
    test_client, *_ = client_with_model
    resp = test_client.get("/api/v1/model/testmodel/job/nonexistent-id")
    assert resp.status_code == 404


def test_job_appears_in_list_after_extractor_execute(client_with_model):
    test_client, _, job_svc, model_dir = client_with_model
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    test_client.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    resp = test_client.get("/api/v1/model/testmodel/job")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


def test_get_job_returns_job_detail(client_with_model):
    test_client, _, job_svc, model_dir = client_with_model
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    execute_resp = test_client.post(
        "/api/v1/model/testmodel/tool/jira/extract/execute"
    )
    job_id = execute_resp.json()["job_id"]
    resp = test_client.get(f"/api/v1/model/testmodel/job/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id


# ---------------------------------------------------------------------------
# Query CSV format
# ---------------------------------------------------------------------------

def test_execute_query_csv_content_type(client_with_model, ch_backend):
    """CSV format returns text/csv Content-Type."""
    test_client, _, _, model_dir = client_with_model
    _configure_ch(model_dir, ch_backend)
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    ch_backend.create_table(
        "widget",
        {"widget_key": "String", "name": "Nullable(String)", "score": "Nullable(Float64)"},
        primary_keys=["widget_key"],
    )
    ch_backend.bulk_upsert("widget", [{"widget_key": "w1", "name": "foo", "score": 1.0}])
    resp = test_client.post(
        "/api/v1/model/testmodel/query/widget_count/execute?format=csv"
    )
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
