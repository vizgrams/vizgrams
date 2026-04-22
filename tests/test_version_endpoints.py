# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""HTTP integration tests for version endpoints (resource-embedded pattern).

Tests cover:
  - GET /api/v1/model/{model}/{artifact}/{name}/versions        → list
  - GET /api/v1/model/{model}/{artifact}/{name}/versions/{id}   → get
  - 404 on missing version
  - saving an artifact records a version
  - deduplication: same content twice → version_num increments only once
  - entity and query as representative artifact types; mapper, feature, extractor spot-checked
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
# Shared YAML fixtures
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

_WIDGET_ENTITY_YAML_V2 = """\
entity: Widget

identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
  score:
    type: FLOAT
    semantic: MEASURE
"""

_WIDGET_QUERY_YAML = """\
name: widget_count
root: Widget

measures:
  count:
    expr: count(widget_key)
"""

_WIDGET_QUERY_YAML_V2 = """\
name: widget_count
root: Widget

measures:
  count:
    expr: count(widget_key)
  name_count:
    expr: count(name)
"""

_MAPPER_YAML = """\
mapper: widget
description: "Build Widget records from raw data"

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
feature: widget_name_length
entity: Widget
expr: len(name)
type: INTEGER
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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
    return model_dir


@pytest.fixture
def client(tmp_path):
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
# Helper
# ---------------------------------------------------------------------------

def _upsert_entity(test_client, content: str) -> None:
    resp = test_client.put(
        "/api/v1/model/testmodel/entity/Widget/yaml",
        json={"content": content},
    )
    assert resp.status_code == 200, resp.text


def _upsert_query(test_client, content: str) -> None:
    resp = test_client.put(
        "/api/v1/model/testmodel/query/widget_count",
        json={"content": content},
    )
    assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# Entity version endpoints
# ---------------------------------------------------------------------------

class TestEntityVersionList:
    def test_list_versions_returns_200_and_list(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_list_versions_empty_before_any_save(self, client_with_model):
        test_client, *_ = client_with_model
        # No entity has been saved yet; versions list is empty.
        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_versions_includes_expected_fields(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        version = resp.json()[0]
        assert "id" in version
        assert "version_num" in version
        assert "checksum" in version
        assert "created_at" in version
        assert "is_current" in version
        # list endpoint omits content
        assert "content" not in version


class TestEntityVersionGet:
    def test_get_version_returns_200_with_content(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        list_resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        version_id = list_resp.json()[0]["id"]

        resp = test_client.get(f"/api/v1/model/testmodel/entity/Widget/versions/{version_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert "content" in data
        assert data["content"] == _WIDGET_ENTITY_YAML

    def test_get_nonexistent_version_returns_404(self, client_with_model):
        test_client, *_ = client_with_model
        resp = test_client.get(
            "/api/v1/model/testmodel/entity/Widget/versions/00000000-0000-0000-0000-000000000000"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Query version endpoints
# ---------------------------------------------------------------------------

class TestQueryVersionList:
    def test_list_versions_returns_200_and_list(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)
        resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_list_versions_includes_expected_fields(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)
        resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        version = resp.json()[0]
        assert "id" in version
        assert "version_num" in version


class TestQueryVersionGet:
    def test_get_version_returns_200_with_content(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)
        list_resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        version_id = list_resp.json()[0]["id"]

        resp = test_client.get(
            f"/api/v1/model/testmodel/query/widget_count/versions/{version_id}"
        )
        assert resp.status_code == 200
        assert resp.json()["content"] == _WIDGET_QUERY_YAML

    def test_get_nonexistent_version_returns_404(self, client_with_model):
        test_client, *_ = client_with_model
        resp = test_client.get(
            "/api/v1/model/testmodel/query/widget_count/versions/00000000-0000-0000-0000-000000000000"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Save records a version
# ---------------------------------------------------------------------------

class TestSaveRecordsVersion:
    def test_saving_entity_records_version(self, client_with_model):
        test_client, *_ = client_with_model
        # Initially no versions
        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        assert resp.json() == []

        # Save via endpoint
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)

        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        assert len(resp.json()) == 1
        assert resp.json()[0]["version_num"] == 1

    def test_saving_query_records_version(self, client_with_model):
        test_client, *_ = client_with_model
        resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        assert resp.json() == []

        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)

        resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        assert len(resp.json()) == 1

    def test_saving_new_content_creates_second_version(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML_V2)

        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        versions = resp.json()
        assert len(versions) == 2
        # Newest first
        assert versions[0]["version_num"] == 2
        assert versions[1]["version_num"] == 1

    def test_only_one_version_is_current(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML_V2)

        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        versions = resp.json()
        current_count = sum(1 for v in versions if v["is_current"])
        assert current_count == 1
        assert versions[0]["is_current"] == 1  # newest is current


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_saving_same_content_twice_does_not_create_second_version(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)  # identical content

        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        versions = resp.json()
        assert len(versions) == 1
        assert versions[0]["version_num"] == 1

    def test_saving_same_query_twice_does_not_duplicate(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)
        _upsert_query(test_client, _WIDGET_QUERY_YAML)

        resp = test_client.get("/api/v1/model/testmodel/query/widget_count/versions")
        assert len(resp.json()) == 1

    def test_version_num_increments_only_once_for_duplicate(self, client_with_model):
        test_client, *_ = client_with_model
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML)
        _upsert_entity(test_client, _WIDGET_ENTITY_YAML_V2)

        resp = test_client.get("/api/v1/model/testmodel/entity/Widget/versions")
        versions = resp.json()
        # First save → v1, duplicate → no-op, second distinct save → v2
        assert len(versions) == 2
        nums = {v["version_num"] for v in versions}
        assert nums == {1, 2}


# ---------------------------------------------------------------------------
# Mapper versions (spot-check)
# ---------------------------------------------------------------------------

class TestMapperVersions:
    def test_mapper_list_versions_returns_200(self, client_with_model):
        test_client, *_ = client_with_model
        # Save via mapper crud endpoint
        resp = test_client.put(
            "/api/v1/model/testmodel/mapper/widget",
            json={"content": _MAPPER_YAML},
        )
        assert resp.status_code == 200

        resp = test_client.get("/api/v1/model/testmodel/mapper/widget/versions")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
        assert len(resp.json()) >= 1

    def test_mapper_get_version_returns_content(self, client_with_model):
        test_client, *_ = client_with_model
        test_client.put(
            "/api/v1/model/testmodel/mapper/widget",
            json={"content": _MAPPER_YAML},
        )
        list_resp = test_client.get("/api/v1/model/testmodel/mapper/widget/versions")
        version_id = list_resp.json()[0]["id"]

        resp = test_client.get(f"/api/v1/model/testmodel/mapper/widget/versions/{version_id}")
        assert resp.status_code == 200
        assert "content" in resp.json()

    def test_mapper_missing_version_returns_404(self, client_with_model):
        test_client, *_ = client_with_model
        resp = test_client.get(
            "/api/v1/model/testmodel/mapper/widget/versions/00000000-0000-0000-0000-000000000000"
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Extractor versions (spot-check)
# ---------------------------------------------------------------------------

class TestExtractorVersions:
    def test_extractor_list_versions_returns_200(self, client_with_model):
        test_client, *_ = client_with_model
        resp = test_client.put(
            "/api/v1/model/testmodel/tool/jira/extract",
            json={"content": _EXTRACTOR_YAML},
        )
        assert resp.status_code == 200

        resp = test_client.get("/api/v1/model/testmodel/tool/jira/versions")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
        assert len(resp.json()) >= 1

    def test_extractor_get_version_returns_content(self, client_with_model):
        test_client, *_ = client_with_model
        test_client.put(
            "/api/v1/model/testmodel/tool/jira/extract",
            json={"content": _EXTRACTOR_YAML},
        )
        list_resp = test_client.get("/api/v1/model/testmodel/tool/jira/versions")
        version_id = list_resp.json()[0]["id"]

        resp = test_client.get(f"/api/v1/model/testmodel/tool/jira/versions/{version_id}")
        assert resp.status_code == 200
        assert "content" in resp.json()
