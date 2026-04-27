# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Shared pytest fixtures."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_vz_models_dir(monkeypatch):
    """Ensure VZ_MODELS_DIR from a .env file never leaks into tests.

    Tests that need a real models dir use absolute paths directly; tests that
    use the FastAPI DI layer override get_base_dir via dependency_overrides,
    which cascades into get_models_dir. Either way, the env var should not
    interfere.
    """
    monkeypatch.delenv("VZ_MODELS_DIR", raising=False)
    monkeypatch.delenv("VZ_DATABASE_BACKEND", raising=False)


@pytest.fixture(autouse=True)
def _isolate_api_db(monkeypatch, tmp_path):
    """Route all metadata_db and vizgrams_db access to a per-test api.db.

    Both modules read API_DB_PATH to locate the central DB. Setting it to a
    tmp_path ensures tests never share state and don't touch the real data dir.
    Tests that need a custom DB path can pass db_path= directly to each
    function, which takes precedence over the env var.
    """
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))
    # Clear the in-process UUID cache so user identities don't leak between tests
    from api.dependencies import _user_id_cache
    _user_id_cache.clear()


@pytest.fixture(autouse=True)
def _isolate_batch_db(monkeypatch, tmp_path):
    """Route all batch_db access (api_jobs, audit_events, batch jobs) to a per-test batch.db.

    Sets BATCH_DB_PATH so every batch_db.get_connection() call uses the test's
    tmp_path, preventing cross-test state leakage.
    """
    monkeypatch.setenv("BATCH_DB_PATH", str(tmp_path / "batch.db"))


@pytest.fixture(autouse=True)
def _set_dev_user(monkeypatch):
    """Set DEV_USER for all tests so authenticated endpoints don't return 401.

    DEV_USER is treated as a system admin by the RBAC layer, so integration
    tests can exercise all endpoints without mocking the auth stack.  Tests
    that specifically test role behaviour override or delete this env var via
    their own monkeypatch calls (function-scope overrides win).
    """
    monkeypatch.setenv("DEV_USER", "test@example.com")


# ---------------------------------------------------------------------------
# Fake batch client — replaces HTTP calls to the batch microservice in tests
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class FakeBatchStore:
    """In-memory stand-in for the batch service, used to patch api.batch_client."""

    def __init__(self, models_dir: Path | None = None) -> None:
        self._jobs: dict[str, dict] = {}
        self._models_dir = models_dir

    def _model_dir(self, model: str) -> Path:
        if self._models_dir is None:
            raise KeyError(f"Model '{model}' not found.")
        d = self._models_dir / model
        if not d.is_dir():
            raise KeyError(f"Model '{model}' not found.")
        return d

    def inject(
        self,
        job_id: str,
        model: str,
        operation: str,
        status: str,
        tool: str | None = None,
        **kwargs,
    ) -> None:
        """Insert a job directly into the store for test set-up."""
        self._jobs[job_id] = {
            "job_id": job_id,
            "model": model,
            "operation": operation,
            "tool": tool,
            "status": status,
            "started_at": kwargs.get("started_at", "2026-01-01T00:00:00Z"),
            "completed_at": kwargs.get("completed_at"),
            "records": None,
            "duration_s": None,
            "error": kwargs.get("error"),
            "triggered_by": "test",
            "progress": [],
        }

    # ------------------------------------------------------------------
    # Methods that replace the module-level functions in api.batch_client
    # ------------------------------------------------------------------

    def submit_job_fn(
        self,
        model: str,
        tool: str,
        operation: str = "extract",
        task: str | None = None,
        full_refresh: bool = False,
        since: str | None = None,
        triggered_by: str = "api",
    ) -> dict:
        from api.batch_client import BatchServiceError

        # Conflict check
        running = [
            j for j in self._jobs.values()
            if j["model"] == model and j["tool"] == tool
            and j["status"] in ("running", "cancelling")
        ]
        if running:
            raise BatchServiceError(
                f"Job '{running[0]['job_id']}' is already running for "
                f"model '{model}' tool '{tool}'. Cancel it first.",
                status_code=409,
            )
        job_id = str(uuid.uuid4())
        job: dict = {
            "job_id": job_id,
            "model": model,
            "operation": operation,
            "tool": tool,
            "status": "running",
            "started_at": _now(),
            "completed_at": None,
            "records": None,
            "duration_s": None,
            "error": None,
            "triggered_by": triggered_by,
            "progress": [],
        }
        self._jobs[job_id] = job
        return dict(job)

    def get_job_fn(self, job_id: str, model: str) -> dict:
        from api.batch_client import BatchServiceError

        job = self._jobs.get(job_id)
        if job is None or job["model"] != model:
            raise BatchServiceError(f"Job '{job_id}' not found.", status_code=404)
        return dict(job)

    def list_jobs_fn(
        self,
        model: str,
        status: str | None = None,
        operation: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        jobs = [j for j in self._jobs.values() if j["model"] == model]
        if status:
            jobs = [j for j in jobs if j["status"] == status]
        if operation:
            jobs = [j for j in jobs if j["operation"] == operation]
        return [dict(j) for j in jobs[:limit]]

    def cancel_job_fn(self, job_id: str, model: str) -> dict:
        from api.batch_client import BatchServiceError

        job = self._jobs.get(job_id)
        if job is None or job["model"] != model:
            raise BatchServiceError(f"Job '{job_id}' not found.", status_code=404)
        if job["status"] != "running":
            raise BatchServiceError(
                f"Job '{job_id}' is not running (status: {job['status']}).",
                status_code=409,
            )
        job["status"] = "cancelling"
        return dict(job)

    def get_schedules_fn(self, model: str) -> list[dict]:
        """Delegate directly to batch.schedule.next_run_times (reads YAML files)."""
        from api.batch_client import BatchServiceError
        from batch.schedule import next_run_times

        try:
            model_dir = self._model_dir(model)
        except KeyError as exc:
            raise BatchServiceError(str(exc), status_code=404) from exc
        entries = next_run_times(model_dir)
        return [
            {
                "model": model,
                "tool": e["tool"],
                "cron": e["cron"],
                "enabled": True,
                "last_success": e["last_success"],
                "next_run": e["next_run"],
                "due": e["due"],
            }
            for e in entries
        ]

    def submit_materialize_job_fn(
        self,
        model: str,
        entity: str | None = None,
        triggered_by: str = "api",
    ) -> dict:
        from api.batch_client import BatchServiceError

        entity_key = entity or "__all__"
        running = [
            j for j in self._jobs.values()
            if j["model"] == model and j["operation"] == "materialize"
            and j["tool"] == entity_key
            and j["status"] in ("running", "cancelling")
        ]
        if running:
            raise BatchServiceError(
                f"Materialize job '{running[0]['job_id']}' is already running for "
                f"model '{model}'. Cancel it first.",
                status_code=409,
            )
        job_id = str(uuid.uuid4())
        job: dict = {
            "job_id": job_id,
            "model": model,
            "operation": "materialize",
            "tool": entity_key,
            "status": "running",
            "started_at": _now(),
            "completed_at": None,
            "records": None,
            "duration_s": None,
            "error": None,
            "triggered_by": triggered_by,
            "progress": [],
        }
        self._jobs[job_id] = job
        return dict(job)

    def submit_mapper_job_fn(
        self,
        model: str,
        mapper: str | None = None,
        triggered_by: str = "api",
    ) -> dict:
        from api.batch_client import BatchServiceError

        mapper_key = mapper or "__all__"
        running = [
            j for j in self._jobs.values()
            if j["model"] == model and j["operation"] == "map"
            and j["tool"] == mapper_key
            and j["status"] in ("running", "cancelling")
        ]
        if running:
            raise BatchServiceError(
                f"Mapper job '{running[0]['job_id']}' is already running for "
                f"model '{model}'. Cancel it first.",
                status_code=409,
            )
        job_id = str(uuid.uuid4())
        job: dict = {
            "job_id": job_id,
            "model": model,
            "operation": "map",
            "tool": mapper_key,
            "status": "running",
            "started_at": _now(),
            "completed_at": None,
            "records": None,
            "duration_s": None,
            "error": None,
            "triggered_by": triggered_by,
            "progress": [],
        }
        self._jobs[job_id] = job
        return dict(job)

    def trigger_fn(
        self,
        model: str,
        tool: str,
        force: bool = False,
        full_refresh: bool = False,
    ) -> dict:
        from api.batch_client import BatchServiceError
        from engine.extractor import find_extractor

        try:
            model_dir = self._model_dir(model)
        except KeyError as exc:
            raise BatchServiceError(str(exc), status_code=404) from exc
        try:
            find_extractor(model_dir, tool)
        except KeyError as exc:
            raise BatchServiceError(str(exc), status_code=404) from exc
        if not force:
            from batch.schedule import extractors_due
            due = extractors_due(model_dir)
            if tool not in due:
                raise BatchServiceError(
                    f"Extractor '{tool}' is not due yet. Use force=true to run it anyway.",
                    status_code=409,
                )
        return self.submit_job_fn(model, tool, triggered_by="api")


# ---------------------------------------------------------------------------
# ClickHouse test fixture — isolated database per test
# ---------------------------------------------------------------------------


def _make_ch_backend(always_final: bool):
    """Create and yield an isolated ClickHouse test backend, then drop the DB."""
    try:
        import clickhouse_connect  # noqa: F401
    except ImportError:
        pytest.skip("clickhouse-connect not installed")

    from core.db import ClickHouseBackend

    db_name = f"test_{uuid.uuid4().hex}"
    try:
        root = ClickHouseBackend(host="localhost", port=8123, database="default")
        root.connect()
        root._client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        root.close()
    except Exception as exc:
        pytest.skip(f"ClickHouse not reachable: {exc}")

    backend = ClickHouseBackend(
        host="localhost",
        port=8123,
        database=db_name,
        username="default",
        password="",
        always_final=always_final,
    )
    backend.connect()
    yield backend
    backend.close()

    try:
        root = ClickHouseBackend(host="localhost", port=8123, database="default")
        root.connect()
        root._client.command(f"DROP DATABASE IF EXISTS `{db_name}`")
        root.close()
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def ch_backend():
    """Function-scoped ClickHouseBackend with an isolated test database (always_final=True).

    Creates a uniquely-named database (``test_<uuid>``) at setup and drops
    it on teardown so tests are fully isolated.  Skips automatically when
    ClickHouse is not reachable or clickhouse-connect is not installed.

    Usage::

        def test_something(ch_backend):
            ch_backend.upsert("widget", {"widget_key": "w1"}, pk="widget_key")
            rows = ch_backend.execute("SELECT * FROM widget FINAL")
            assert len(rows) == 1
    """
    yield from _make_ch_backend(always_final=True)


@pytest.fixture
def ch_backend_mapper():
    """Function-scoped ClickHouseBackend for mapper tests (always_final=False).

    Uses always_final=False so that ``raw_``-prefixed source tables get FINAL
    injected (legacy mode) but entity tables (no prefix) are read without FINAL.
    This matches the mapper's single-database pattern where raw and entity tables
    share one ClickHouse database.
    """
    yield from _make_ch_backend(always_final=False)


# ---------------------------------------------------------------------------
# DB seeding helpers — used by tests that work with service/API layers
# ---------------------------------------------------------------------------


def seed_artifact(model_dir, artifact_type: str, name: str, content: str) -> None:
    """Seed a single artifact into the model's metadata DB."""
    from core.metadata_db import record_version
    record_version(model_dir, artifact_type, name, content)


@pytest.fixture
def seed_db():
    """Fixture that returns a callable to seed artifacts into model DB.

    Usage in tests::

        def test_something(model_dir, seed_db):
            seed_db(model_dir, "entity", "widget", _WIDGET_YAML)
    """
    from core.metadata_db import record_version

    def _seed(model_dir, artifact_type: str, name: str, content: str) -> None:
        record_version(model_dir, artifact_type, name, content)

    return _seed


@pytest.fixture
def fake_batch_client(monkeypatch, tmp_path):
    """Patch api.batch_client functions with an in-memory FakeBatchStore.

    Fixtures and tests that need to inspect or inject jobs should also
    request this fixture — pytest shares the same instance within a test.

    Patching happens both on the api.batch_client module AND in the specific
    router/service modules that import the functions at the top level.
    """
    store = FakeBatchStore(models_dir=tmp_path / "models")

    # Patch the source module (catches lazy imports inside function bodies)
    import api.batch_client as bc
    monkeypatch.setattr(bc, "submit_job", store.submit_job_fn)
    monkeypatch.setattr(bc, "submit_mapper_job", store.submit_mapper_job_fn)
    monkeypatch.setattr(bc, "submit_materialize_job", store.submit_materialize_job_fn)
    monkeypatch.setattr(bc, "get_job", store.get_job_fn)
    monkeypatch.setattr(bc, "list_jobs", store.list_jobs_fn)
    monkeypatch.setattr(bc, "cancel_job", store.cancel_job_fn)
    monkeypatch.setattr(bc, "get_schedules", store.get_schedules_fn)
    monkeypatch.setattr(bc, "trigger", store.trigger_fn)

    # Patch in the routers that import functions at the top level
    import api.routers.batch as batch_router
    import api.routers.jobs as jobs_router
    import api.routers.mappers as mappers_router

    monkeypatch.setattr(batch_router, "get_schedules", store.get_schedules_fn)
    monkeypatch.setattr(batch_router, "trigger", store.trigger_fn)
    monkeypatch.setattr(jobs_router, "get_job", store.get_job_fn)
    monkeypatch.setattr(jobs_router, "list_jobs", store.list_jobs_fn)
    monkeypatch.setattr(jobs_router, "cancel_job", store.cancel_job_fn)
    monkeypatch.setattr(mappers_router, "submit_mapper_job", store.submit_mapper_job_fn)
    import api.routers.entities as entities_router
    monkeypatch.setattr(entities_router, "submit_materialize_job", store.submit_materialize_job_fn)

    return store
