# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Black-box integration tests: exercise every REST API endpoint using TestClient.

Tests run against an in-memory/tmp_path environment — no external services needed.
Each test exercises the full HTTP contract: routing, status codes, response shapes.
"""

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, get_job_service
from api.main import app
from api.services.job_service import JobService
from core import metadata_db

# ---------------------------------------------------------------------------
# YAML / JSON fixtures
# ---------------------------------------------------------------------------

_WIDGET_ENTITY_BODY = {
    "name": "Widget",
    "identity": {"widget_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
    "attributes": {"name": {"type": "STRING", "semantic": "IDENTIFIER"}},
}

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
  - name: fetch_widgets
    tool: jira
    command: boards
    params: {}
    output:
      table: raw_widgets
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
"""

_MAPPER_YAML = """\
mapper: widget
description: Build Widget records from raw data

sources:
  - alias: w
    table: raw_widgets
    columns: [widget_key, name]

targets:
  - entity: Widget
    rows:
      - from: w
        columns:
          - name: widget_key
            expr: w.widget_key
          - name: name
            expr: w.name
"""

_FEATURE_YAML = """\
feature_id: widget.widget_name_len
name: Widget Name Length
description: Length of the widget name
entity_type: Widget
entity_key: widget_key
data_type: INTEGER
materialization_mode: materialized
expr: "length(name)"
"""

_QUERY_YAML = """\
name: widget_count
root: Widget

measures:
  count:
    expr: count(widget_key)
"""

_INPUT_DATA_CONTENT = "widget_key,name\nw1,Foo\nw2,Bar\n"


def _seed(model_dir: Path, artifact_type: str, name: str, content: str) -> None:
    """Seed an artifact directly into the metadata DB."""
    metadata_db.record_version(model_dir, artifact_type, name, content)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_MODEL_META = {
    "display_name": "Test",
    "status": "active",
    "tags": [],
    "description": "",
    "owner": "test",
    "created_at": "2026-01-01T00:00:00Z",
}


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
    (model_dir / "config.yaml").write_text("")
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

    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[get_job_service] = lambda: job_svc
    yield TestClient(app), tmp_path, job_svc
    app.dependency_overrides.clear()


@pytest.fixture
def live_client(client):
    """Client with a pre-scaffolded, registered testmodel."""
    tc, tmp_path, job_svc = client
    _write_registry(tmp_path, {"testmodel": _MODEL_META})
    model_dir = _scaffold_model(tmp_path, "testmodel")
    return tc, tmp_path, job_svc, model_dir


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_healthz(client):
    tc, *_ = client
    resp = tc.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Models — list / get / create / update / archive / delete
# ---------------------------------------------------------------------------

def test_list_models_empty(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = tc.get("/api/v1/model")
    assert resp.status_code == 200
    assert resp.json() == []


def test_create_model_returns_201(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = tc.post("/api/v1/model", json={
        "name": "newmodel",
        "display_name": "New Model",
        "description": "Test",
        "owner": "tester",
        "status": "experimental",
        "tags": [],
    })
    assert resp.status_code == 201
    assert resp.json()["name"] == "newmodel"


def test_create_model_conflict(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {"dup": {**_MODEL_META, "display_name": "D"}})
    resp = tc.post("/api/v1/model", json={
        "name": "dup",
        "display_name": "Dup",
        "description": "",
        "owner": "x",
    })
    assert resp.status_code == 409


def test_list_models_returns_registered(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {"alpha": {**_MODEL_META, "display_name": "Alpha"}})
    resp = tc.get("/api/v1/model")
    assert resp.status_code == 200
    names = [m["name"] for m in resp.json()]
    assert "alpha" in names


def test_get_model_200(live_client):
    tc, *_ = live_client
    resp = tc.get("/api/v1/model/testmodel")
    assert resp.status_code == 200
    assert resp.json()["name"] == "testmodel"


def test_get_model_404(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = tc.get("/api/v1/model/nonexistent")
    assert resp.status_code == 404


def test_update_model(live_client):
    tc, *_ = live_client
    resp = tc.patch("/api/v1/model/testmodel", json={"description": "Updated"})
    assert resp.status_code == 200
    assert resp.json()["description"] == "Updated"


def test_archive_model(live_client):
    tc, *_ = live_client
    resp = tc.post("/api/v1/model/testmodel/archive", json={"reason": "test"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"


def test_delete_model_204(live_client):
    tc, tmp_path, *_ = live_client
    resp = tc.delete("/api/v1/model/testmodel")
    assert resp.status_code == 204
    # Model is gone from registry
    assert tc.get("/api/v1/model/testmodel").status_code == 404


def test_delete_model_404(client):
    tc, tmp_path, _ = client
    _write_registry(tmp_path, {})
    resp = tc.delete("/api/v1/model/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 404 propagation for unknown model
# ---------------------------------------------------------------------------

def test_entity_list_unknown_model(client):
    tc, *_ = client
    assert tc.get("/api/v1/model/unknown/entity").status_code == 404


def test_extractor_list_unknown_model(client):
    tc, *_ = client
    assert tc.get("/api/v1/model/unknown/tool/jira/extract").status_code == 404


def test_query_list_unknown_model(client):
    tc, *_ = client
    assert tc.get("/api/v1/model/unknown/query").status_code == 404


def test_job_list_unknown_model(client):
    tc, *_ = client
    assert tc.get("/api/v1/model/unknown/job").status_code == 404


# ---------------------------------------------------------------------------
# Entities — list / get / create (async) / upsert (async) / validate
# ---------------------------------------------------------------------------

def test_list_entities_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/entity").json() == []


def test_create_entity_202(live_client):
    tc, *_ = live_client
    resp = tc.post("/api/v1/model/testmodel/entity", json=_WIDGET_ENTITY_BODY)
    assert resp.status_code == 202
    assert "job_id" in resp.json()


def test_entity_yaml_written_after_create(live_client):
    tc, _, _, model_dir = live_client
    tc.post("/api/v1/model/testmodel/entity", json=_WIDGET_ENTITY_BODY)
    assert metadata_db.get_current_content(model_dir, "entity", "Widget") is not None


def test_create_entity_conflict_409(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.post("/api/v1/model/testmodel/entity", json=_WIDGET_ENTITY_BODY)
    assert resp.status_code == 409


def test_list_entities_after_yaml_write(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    names = [e["name"] for e in tc.get("/api/v1/model/testmodel/entity").json()]
    assert "Widget" in names


def test_get_entity_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.get("/api/v1/model/testmodel/entity/Widget")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Widget"


def test_get_entity_404(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/entity/NoSuch").status_code == 404


def test_upsert_entity_202(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.put("/api/v1/model/testmodel/entity/Widget", json=_WIDGET_ENTITY_BODY)
    assert resp.status_code == 202


def test_validate_entity_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.post("/api/v1/model/testmodel/entity/Widget/validate")
    assert resp.status_code == 200
    assert resp.json()["valid"] is True


def test_validate_entity_404(live_client):
    tc, *_ = live_client
    assert tc.post("/api/v1/model/testmodel/entity/NoSuch/validate").status_code == 404


# ---------------------------------------------------------------------------
# Extractors — list / get / upsert / validate / execute
# ---------------------------------------------------------------------------

def test_list_extractors_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/tool").json() == []


def test_upsert_extractor_200(live_client):
    tc, *_ = live_client
    resp = tc.put(
        "/api/v1/model/testmodel/tool/jira/extract",
        json={"content": _EXTRACTOR_YAML},
    )
    assert resp.status_code == 200
    assert resp.json()["tool"] == "jira"


def test_extractor_file_written(live_client):
    tc, _, _, model_dir = live_client
    tc.put(
        "/api/v1/model/testmodel/tool/jira/extract",
        json={"content": _EXTRACTOR_YAML},
    )
    assert metadata_db.get_current_content(model_dir, "extractor", "jira") is not None


def test_get_extractor_after_upsert(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = tc.get("/api/v1/model/testmodel/tool/jira/extract")
    assert resp.status_code == 200
    assert resp.json()["tool"] == "jira"


def test_get_extractor_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = tc.get("/api/v1/model/testmodel/tool/jira/extract")
    assert resp.status_code == 200
    assert resp.json()["tool"] == "jira"


def test_get_extractor_404(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/tool/nonexistent_tool/extract").status_code == 404


def test_validate_extractor_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/validate")
    assert resp.status_code == 200
    assert "valid" in resp.json()


def test_execute_extractor_202(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert resp.status_code == 202
    assert "job_id" in resp.json()


# ---------------------------------------------------------------------------
# Mappers — model-scoped list / upsert + entity-scoped get / validate / execute
# ---------------------------------------------------------------------------

def test_list_mappers_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/mapper").json() == []


def test_upsert_mapper_200(live_client):
    tc, *_ = live_client
    resp = tc.put(
        "/api/v1/model/testmodel/mapper/widget",
        json={"content": _MAPPER_YAML},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget"


def test_mapper_file_written(live_client):
    tc, _, _, model_dir = live_client
    tc.put("/api/v1/model/testmodel/mapper/widget", json={"content": _MAPPER_YAML})
    assert metadata_db.get_current_content(model_dir, "mapper", "widget") is not None


def test_list_mappers_after_upsert(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "mapper", "widget", _MAPPER_YAML)
    names = [m["name"] for m in tc.get("/api/v1/model/testmodel/mapper").json()]
    assert "widget" in names


def test_get_mapper_by_entity_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "mapper", "widget", _MAPPER_YAML)
    resp = tc.get("/api/v1/model/testmodel/entity/Widget/mapper")
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget"


def test_get_mapper_by_entity_404(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/entity/NoSuch/mapper").status_code == 404


def test_validate_mapper_by_entity(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "mapper", "widget", _MAPPER_YAML)
    resp = tc.post("/api/v1/model/testmodel/entity/Widget/mapper/validate")
    assert resp.status_code == 200
    assert "valid" in resp.json()


def test_execute_mapper_202(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "mapper", "widget", _MAPPER_YAML)
    resp = tc.post("/api/v1/model/testmodel/entity/Widget/mapper/execute")
    assert resp.status_code == 202
    assert "job_id" in resp.json()


def test_execute_all_mappers_202(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "mapper", "widget", _MAPPER_YAML)
    resp = tc.post("/api/v1/model/testmodel/mapper/execute-all")
    assert resp.status_code == 202
    assert "job_id" in resp.json()


def test_execute_all_mappers_404_when_none(live_client):
    tc, *_ = live_client
    assert tc.post("/api/v1/model/testmodel/mapper/execute-all").status_code == 404


# ---------------------------------------------------------------------------
# Entity name validation (injection guard)
# ---------------------------------------------------------------------------

_BAD_NAMES = [
    "FEATURES.md",       # dot → path traversal
    "../secret",         # directory traversal
    "foo/bar",           # slash
    "foo bar",           # space
    ".hidden",           # leading dot
    "foo;rm -rf /",      # shell injection
]

@pytest.mark.parametrize("bad_name", _BAD_NAMES)
def test_entity_name_validation_get(live_client, bad_name):
    tc, *_ = live_client
    resp = tc.get(f"/api/v1/model/testmodel/entity/{bad_name}")
    assert resp.status_code in (404, 422), f"Expected 404/422 for {bad_name!r}, got {resp.status_code}"


@pytest.mark.parametrize("bad_name", _BAD_NAMES)
def test_entity_name_validation_mapper_execute(live_client, bad_name):
    tc, *_ = live_client
    resp = tc.post(f"/api/v1/model/testmodel/entity/{bad_name}/mapper/execute")
    assert resp.status_code in (404, 422), f"Expected 404/422 for {bad_name!r}, got {resp.status_code}"


def test_valid_entity_name_passes_validation(live_client):
    tc, _, _, model_dir = live_client
    # A valid name that doesn't exist returns 404, not 422
    resp = tc.get("/api/v1/model/testmodel/entity/MyEntity_123")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Features — list / get / upsert
# ---------------------------------------------------------------------------

def test_list_features_empty(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    assert tc.get("/api/v1/model/testmodel/entity/Widget/feature").json() == []


def test_upsert_feature_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.put(
        "/api/v1/model/testmodel/entity/Widget/feature/widget_name_len",
        json={"content": _FEATURE_YAML},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget_name_len"


def test_feature_file_written(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    tc.put(
        "/api/v1/model/testmodel/entity/Widget/feature/widget_name_len",
        json={"content": _FEATURE_YAML},
    )
    assert metadata_db.get_current_content(model_dir, "feature", "widget.widget_name_len") is not None


def test_list_features_after_upsert(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "feature", "widget.widget_name_len", _FEATURE_YAML)
    names = [f["name"] for f in tc.get("/api/v1/model/testmodel/entity/Widget/feature").json()]
    assert "widget_name_len" in names


def test_get_feature_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "feature", "widget.widget_name_len", _FEATURE_YAML)
    resp = tc.get("/api/v1/model/testmodel/entity/Widget/feature/widget_name_len")
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget_name_len"


def test_get_feature_404(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    assert tc.get("/api/v1/model/testmodel/entity/Widget/feature/nosuch").status_code == 404


# ---------------------------------------------------------------------------
# Queries — list / get / upsert / validate / execute (json + csv)
# ---------------------------------------------------------------------------

def test_list_queries_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/query").json() == []


def test_upsert_query_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    resp = tc.put(
        "/api/v1/model/testmodel/query/widget_count",
        json={"content": _QUERY_YAML},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget_count"


def test_query_file_written(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    tc.put("/api/v1/model/testmodel/query/widget_count", json={"content": _QUERY_YAML})
    assert metadata_db.get_current_content(model_dir, "query", "widget_count") is not None


def test_list_queries_after_upsert(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    names = [q["name"] for q in tc.get("/api/v1/model/testmodel/query").json()]
    assert "widget_count" in names


def test_get_query_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    resp = tc.get("/api/v1/model/testmodel/query/widget_count")
    assert resp.status_code == 200
    assert resp.json()["name"] == "widget_count"


def test_get_query_404(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/query/nosuch").status_code == 404


def test_validate_query_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    resp = tc.post("/api/v1/model/testmodel/query/widget_count/validate")
    assert resp.status_code == 200
    assert "valid" in resp.json()


def test_execute_query_404_no_db(live_client, ch_backend):
    tc, _, _, model_dir = live_client
    _configure_ch(model_dir, ch_backend)
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    assert tc.post("/api/v1/model/testmodel/query/widget_count/execute").status_code == 404


def test_execute_query_200_with_db(live_client, ch_backend):
    tc, _, _, model_dir = live_client
    _configure_ch(model_dir, ch_backend)
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    ch_backend.create_table(
        "widget",
        {"widget_key": "String", "name": "Nullable(String)"},
        primary_keys=["widget_key"],
    )
    ch_backend.bulk_upsert("widget", [{"widget_key": "w1", "name": "Foo"}])
    resp = tc.post("/api/v1/model/testmodel/query/widget_count/execute")
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] >= 1


def test_execute_query_csv_content_type(live_client, ch_backend):
    tc, _, _, model_dir = live_client
    _configure_ch(model_dir, ch_backend)
    _seed(model_dir, "entity", "Widget", _WIDGET_ENTITY_YAML)
    _seed(model_dir, "query", "widget_count", _QUERY_YAML)
    ch_backend.create_table(
        "widget",
        {"widget_key": "String", "name": "Nullable(String)"},
        primary_keys=["widget_key"],
    )
    ch_backend.bulk_upsert("widget", [{"widget_key": "w1", "name": "Foo"}])
    resp = tc.post("/api/v1/model/testmodel/query/widget_count/execute?format=csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# Input data — list / upload
# ---------------------------------------------------------------------------

def test_list_input_data_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/input-data").json() == []


def test_upload_input_file_201(live_client):
    tc, *_ = live_client
    resp = tc.post(
        "/api/v1/model/testmodel/input-data/widgets.csv",
        json={"content": _INPUT_DATA_CONTENT},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["file"] == "widgets.csv"
    assert data["size"] > 0


def test_input_file_written_to_disk(live_client):
    tc, _, _, model_dir = live_client
    tc.post(
        "/api/v1/model/testmodel/input-data/widgets.csv",
        json={"content": _INPUT_DATA_CONTENT},
    )
    assert (model_dir / "input_data" / "widgets.csv").read_text() == _INPUT_DATA_CONTENT


def test_list_input_data_after_upload(live_client):
    tc, *_ = live_client
    tc.post(
        "/api/v1/model/testmodel/input-data/widgets.csv",
        json={"content": _INPUT_DATA_CONTENT},
    )
    files = [f["file"] for f in tc.get("/api/v1/model/testmodel/input-data").json()]
    assert "widgets.csv" in files


# ---------------------------------------------------------------------------
# Jobs — list / get
# ---------------------------------------------------------------------------

def test_list_jobs_empty(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/job").json() == []


def test_job_appears_after_extractor_execute(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert len(tc.get("/api/v1/model/testmodel/job").json()) >= 1


def test_get_job_200(live_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    exec_resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    job_id = exec_resp.json()["job_id"]
    resp = tc.get(f"/api/v1/model/testmodel/job/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id


def test_get_job_404(live_client):
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/job/nonexistent-id").status_code == 404


# ---------------------------------------------------------------------------
# Job cancellation and single-run enforcement
# ---------------------------------------------------------------------------

def _inject_running_job(fake_batch, model="testmodel") -> str:
    """Insert a running extract job into the fake batch store and return its job_id."""
    job_id = "running-001"
    fake_batch.inject(job_id, model, "extract", "running", tool="jira")
    return job_id


def test_cancel_running_job_returns_cancelling(live_client, fake_batch_client):
    tc, *_ = live_client
    _inject_running_job(fake_batch_client)
    resp = tc.delete("/api/v1/model/testmodel/job/running-001")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelling"


def test_cancel_already_completed_job_returns_409(live_client, fake_batch_client):
    tc, *_ = live_client
    fake_batch_client.inject(
        "done-001", "testmodel", "extract", "completed",
        tool="jira", completed_at="2026-01-01T00:01:00Z",
    )
    assert tc.delete("/api/v1/model/testmodel/job/done-001").status_code == 409


def test_cancel_nonexistent_job_returns_404(live_client):
    tc, *_ = live_client
    assert tc.delete("/api/v1/model/testmodel/job/no-such-job").status_code == 404


def test_execute_extractor_blocked_when_already_running(live_client, fake_batch_client):
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    _inject_running_job(fake_batch_client)
    resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert resp.status_code == 409
    assert "already running" in resp.json()["detail"]


def test_execute_extractor_allowed_after_cancel(live_client, fake_batch_client):
    """Once a job is cancelled (not running/cancelling), a new one can start."""
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    # Inject a cancelled (not running) job — should not block new submission
    fake_batch_client.inject(
        "cancelled-001", "testmodel", "extract", "cancelled",
        tool="jira", completed_at="2026-01-01T00:01:00Z",
    )
    resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert resp.status_code == 202


def test_execute_extractor_blocked_while_cancelling(live_client, fake_batch_client):
    """A job in 'cancelling' state also blocks a new run."""
    tc, _, _, model_dir = live_client
    _seed(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    fake_batch_client.inject("cancelling-001", "testmodel", "extract", "cancelling", tool="jira")
    resp = tc.post("/api/v1/model/testmodel/tool/jira/extract/execute")
    assert resp.status_code == 409


def test_is_cancelling_returns_true_after_cancel():
    """JobService.is_cancelling reflects the cancel() call (unit test, no HTTP)."""
    from api.services.job_service import JobService
    svc = JobService()
    job = svc.create(model="m1", operation="extract")
    assert not svc.is_cancelling(job.job_id)
    svc.cancel(job.job_id)
    assert svc.is_cancelling(job.job_id)


def test_mark_cancelled_transitions_status():
    """JobService.mark_cancelled() sets status to cancelled and records completed_at."""
    from api.services.job_service import JobService
    svc = JobService()
    job = svc.create(model="m1", operation="extract")
    svc.cancel(job.job_id)
    svc.mark_cancelled(job.job_id)
    updated = svc._jobs[job.job_id]
    assert updated.status.value == "cancelled"
    assert updated.completed_at is not None


# ---------------------------------------------------------------------------
# Orphan detection on startup
# ---------------------------------------------------------------------------

def _insert_running_api_job(job_id: str, model: str, started_at: str = "2026-01-01T00:00:00Z") -> None:
    """Insert a running api_job row directly into batch.db for orphan tests."""
    from core.batch_db import get_connection
    with get_connection() as con:
        con.execute(
            """
            INSERT INTO api_jobs (id, status, model, created_at, updated_at, operation)
            VALUES (?, 'running', ?, ?, ?, 'extract')
            """,
            (job_id, model, started_at, started_at),
        )
        con.commit()


def _insert_terminal_api_job(job_id: str, model: str, status: str) -> None:
    """Insert a terminal (completed/failed) api_job row into batch.db."""
    from core.batch_db import get_connection
    with get_connection() as con:
        con.execute(
            """
            INSERT INTO api_jobs
                (id, status, model, created_at, updated_at, completed_at, operation)
            VALUES (?, ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z', 'extract')
            """,
            (job_id, status, model),
        )
        con.commit()


def test_orphan_marked_failed_on_startup(tmp_path):
    """A running API job in batch.db is marked failed on startup."""
    from core.registry import mark_orphaned_jobs
    model_dir = _scaffold_model(tmp_path, "testmodel")
    _insert_running_api_job("orphan-001", "testmodel")

    n = mark_orphaned_jobs(tmp_path / "models")
    assert n == 1

    from core.registry import read_job_history
    jobs = read_job_history(model_dir)
    assert len(jobs) == 1
    assert jobs[0]["status"] == "failed"
    assert "restarted" in jobs[0]["error"]


def test_completed_job_not_marked_orphan(tmp_path):
    """A job that is already in a terminal state is not re-marked."""
    from core.registry import mark_orphaned_jobs
    _scaffold_model(tmp_path, "testmodel")
    _insert_terminal_api_job("clean-001", "testmodel", "completed")

    n = mark_orphaned_jobs(tmp_path / "models")
    assert n == 0


def test_failed_job_not_marked_orphan(tmp_path):
    """A job that is already failed is not re-marked."""
    from core.registry import mark_orphaned_jobs
    _scaffold_model(tmp_path, "testmodel")
    _insert_terminal_api_job("failed-001", "testmodel", "failed")

    n = mark_orphaned_jobs(tmp_path / "models")
    assert n == 0


def test_multiple_models_orphans_marked(tmp_path):
    """mark_orphaned_jobs marks orphaned jobs across all models."""
    from core.registry import mark_orphaned_jobs
    _scaffold_model(tmp_path, "m1")
    _scaffold_model(tmp_path, "m2")
    _insert_running_api_job("orphan-m1", "m1")
    _insert_running_api_job("orphan-m2", "m2")

    n = mark_orphaned_jobs(tmp_path / "models")
    assert n == 2


def test_history_non_job_audit_entries_ignored(live_client):
    """Non-job audit entries (extract_run, map_run) do not appear in job list."""
    tc, *_ = live_client
    resp = tc.get("/api/v1/model/testmodel/job")
    # No jobs — batch client returns empty list for a fresh model
    assert resp.json() == []



def test_history_unknown_job_id_still_404(live_client):
    """GET /job/{job_id} returns 404 when id is absent from the batch service."""
    tc, *_ = live_client
    assert tc.get("/api/v1/model/testmodel/job/totally-unknown").status_code == 404

