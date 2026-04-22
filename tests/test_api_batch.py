# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/routers/batch.py — batch schedule and trigger endpoints.

The batch router proxies to the batch microservice via api.batch_client.
Tests use the fake_batch_client fixture (from conftest.py) to avoid needing
the batch service running.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, get_job_service
from api.main import app
from api.services.job_service import JobService

HOURLY = "0 * * * *"
DAILY = "0 6 * * *"


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


def _write_extractor(model_dir: Path, tool: str, cron: str | None = None) -> None:
    from core import metadata_db
    data: dict = {
        "tasks": [{"name": f"{tool}_task", "tool": tool, "command": "load", "params": {}}]
    }
    if cron is not None:
        data["schedule"] = {"cron": cron}
    content = yaml.dump(data)
    (model_dir / "extractors" / f"extractor_{tool}.yaml").write_text(content)
    metadata_db.record_version(model_dir, "extractor", tool, content)


def _write_audit_completed(model_dir: Path, tool: str, completed_at: str) -> None:
    """Record a completed extraction run in the batch-service DB (used by _last_success_time)."""
    from batch_service import db as jobdb
    with jobdb.get_connection(model_dir) as con:
        job_id = f"test-job-{tool}"
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=model_dir.name,
            operation="extract",
            tool=tool,
            status="running",
            started_at=completed_at,
            triggered_by="test",
        )
        jobdb.update_job(con, job_id, status="completed", completed_at=completed_at)


@pytest.fixture
def client(tmp_path, fake_batch_client):
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
        "mymodel": {"display_name": "My Model", "status": "active", "tags": [], "description": ""},
    })
    model_dir = _scaffold_model(tmp_path, "mymodel")
    return test_client, tmp_path, job_svc, model_dir


# ---------------------------------------------------------------------------
# GET /api/v1/model/{model}/batch/schedule
# ---------------------------------------------------------------------------


class TestGetSchedule:
    def test_returns_empty_when_no_scheduled_extractors(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file")  # no cron
        resp = tc.get("/api/v1/model/mymodel/batch/schedule")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_schedule_entry_for_scheduled_extractor(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file", cron=HOURLY)
        resp = tc.get("/api/v1/model/mymodel/batch/schedule")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        entry = data[0]
        assert entry["tool"] == "file"
        assert entry["cron"] == HOURLY
        assert entry["last_success"] is None
        assert entry["next_run"] is not None
        assert entry["due"] is False  # never run → waits for next scheduled time

    def test_returns_multiple_entries(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file", cron=HOURLY)
        _write_extractor(model_dir, "jira", cron=DAILY)
        resp = tc.get("/api/v1/model/mymodel/batch/schedule")
        assert resp.status_code == 200
        tools = {e["tool"] for e in resp.json()}
        assert tools == {"file", "jira"}

    def test_not_due_when_recently_completed(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "jira", cron=DAILY)
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_audit_completed(model_dir, "jira", recent)
        resp = tc.get("/api/v1/model/mymodel/batch/schedule")
        assert resp.status_code == 200
        entry = resp.json()[0]
        assert entry["due"] is False
        assert entry["last_success"] == recent

    def test_404_for_unknown_model(self, client):
        tc, *_ = client
        resp = tc.get("/api/v1/model/nosuchmodel/batch/schedule")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/v1/model/{model}/batch/trigger
# ---------------------------------------------------------------------------


class TestTrigger:
    def test_404_for_unknown_model(self, client):
        tc, *_ = client
        resp = tc.post("/api/v1/model/nosuchmodel/batch/trigger?tool=file")
        assert resp.status_code == 404

    def test_422_when_tool_not_provided(self, client_with_model):
        tc, *_ = client_with_model
        resp = tc.post("/api/v1/model/mymodel/batch/trigger")
        assert resp.status_code == 422

    def test_404_when_specific_tool_not_found(self, client_with_model):
        tc, *_ = client_with_model
        resp = tc.post("/api/v1/model/mymodel/batch/trigger?tool=nosuch&force=true")
        assert resp.status_code == 404

    def test_409_when_tool_not_due_without_force(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "jira", cron=DAILY)
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_audit_completed(model_dir, "jira", recent)
        resp = tc.post("/api/v1/model/mymodel/batch/trigger?tool=jira")
        assert resp.status_code == 409

    def test_force_bypasses_schedule_check(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file", cron=HOURLY)
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_audit_completed(model_dir, "file", recent)

        resp = tc.post("/api/v1/model/mymodel/batch/trigger?tool=file&force=true")
        assert resp.status_code == 202
        body = resp.json()
        assert body["tool"] == "file"
        assert body["model"] == "mymodel"
        assert "job_id" in body
        assert body["status"] == "running"

    def test_response_shape(self, client_with_model):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file", cron=HOURLY)

        resp = tc.post("/api/v1/model/mymodel/batch/trigger?tool=file&force=true")
        assert resp.status_code == 202
        body = resp.json()
        assert "job_id" in body
        assert "model" in body
        assert "tool" in body
        assert "status" in body
        assert "started_at" in body

    def test_409_when_already_running(self, client_with_model, fake_batch_client):
        tc, _, _, model_dir = client_with_model
        _write_extractor(model_dir, "file", cron=HOURLY)
        # Inject an already-running job for this tool
        fake_batch_client.inject("running-001", "mymodel", "extract", "running", tool="file")
        resp = tc.post("/api/v1/model/mymodel/batch/trigger?tool=file&force=true")
        assert resp.status_code == 409
