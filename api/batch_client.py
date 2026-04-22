# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""HTTP client for the main API to communicate with the batch service.

The batch service URL is read from BATCH_SERVICE_URL (default: http://localhost:8001).
All methods raise ``BatchServiceError`` when the batch service is unreachable or
returns an unexpected error, so callers can translate to the appropriate HTTP response.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

_log = logging.getLogger(__name__)

_TIMEOUT = 10.0  # seconds for connect + read

# Persistent client — reused across all requests so the process does not
# exhaust its file-descriptor budget when the UI polls for job updates.
# Re-created if the base URL changes (e.g. env var set after import).
_http_client: httpx.Client | None = None
_http_client_url: str | None = None


class BatchServiceError(Exception):
    """Raised when the batch service returns an error or is unreachable."""

    def __init__(self, message: str, status_code: int = 503):
        super().__init__(message)
        self.status_code = status_code


def _batch_url() -> str:
    return os.environ.get("BATCH_SERVICE_URL", "http://localhost:8001")


def _headers() -> dict[str, str]:
    secret = os.environ.get("BATCH_SERVICE_SECRET")
    return {"X-Batch-Secret": secret} if secret else {}


def _client() -> httpx.Client:
    global _http_client, _http_client_url
    url = _batch_url()
    if _http_client is None or _http_client_url != url:
        if _http_client is not None:
            _http_client.close()
        transport = httpx.HTTPTransport(retries=2)
        _http_client = httpx.Client(
            base_url=url, timeout=_TIMEOUT, headers=_headers(), transport=transport,
        )
        _http_client_url = url
    return _http_client


def _raise_for_status(resp: httpx.Response) -> dict:
    """Raise BatchServiceError for non-2xx responses."""
    if resp.is_success:
        return resp.json()
    try:
        detail = resp.json().get("detail", resp.text)
    except Exception:
        detail = resp.text
    raise BatchServiceError(str(detail), status_code=resp.status_code)


# ---------------------------------------------------------------------------
# Job operations — mirrors batch service /api/v1/jobs
# ---------------------------------------------------------------------------


def submit_job(
    model: str,
    tool: str,
    operation: str = "extract",
    task: str | None = None,
    full_refresh: bool = False,
    since: str | None = None,
    triggered_by: str = "api",
) -> dict:
    """Submit a job and return the job dict (status 'running')."""
    try:
        resp = _client().post(
            "/api/v1/jobs",
            json={
                "model": model,
                "tool": tool,
                "operation": operation,
                "task": task,
                "full_refresh": full_refresh,
                "since": since,
                "triggered_by": triggered_by,
            },
        )
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def submit_materialize_job(
    model: str,
    entity: str | None = None,
    triggered_by: str = "api",
) -> dict:
    """Submit a materialize job and return the job dict (status 'running')."""
    try:
        resp = _client().post(
            "/api/v1/jobs",
            json={
                "model": model,
                "operation": "materialize",
                "entity": entity,
                "triggered_by": triggered_by,
            },
        )
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def submit_mapper_job(
    model: str,
    mapper: str | None = None,
    triggered_by: str = "api",
) -> dict:
    """Submit a mapper job and return the job dict (status 'running')."""
    try:
        resp = _client().post(
            "/api/v1/jobs",
            json={
                "model": model,
                "operation": "map",
                "mapper": mapper,
                "triggered_by": triggered_by,
            },
        )
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def get_job(job_id: str, model: str) -> dict:
    """Get a job by ID, including live progress if running."""
    try:
        resp = _client().get(f"/api/v1/jobs/{job_id}", params={"model": model})
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def list_jobs(
    model: str,
    status: str | None = None,
    operation: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """List jobs for a model."""
    params: dict[str, Any] = {"model": model, "limit": limit}
    if status:
        params["status"] = status
    if operation:
        params["operation"] = operation
    try:
        resp = _client().get("/api/v1/jobs", params=params)
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def cancel_job(job_id: str, model: str) -> dict:
    """Request cancellation of a running job."""
    try:
        resp = _client().delete(f"/api/v1/jobs/{job_id}", params={"model": model})
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


# ---------------------------------------------------------------------------
# Schedule operations — mirrors batch service /api/v1/schedules
# ---------------------------------------------------------------------------


def get_schedules(model: str) -> list[dict]:
    """Return schedule status for every scheduled extractor in the model."""
    try:
        resp = _client().get("/api/v1/schedules", params={"model": model})
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc


def trigger(
    model: str,
    tool: str,
    force: bool = False,
    full_refresh: bool = False,
) -> dict:
    """Manually trigger an extractor. Returns a job dict."""
    params: dict[str, Any] = {"force": force, "full_refresh": full_refresh}
    try:
        resp = _client().post(f"/api/v1/schedules/{model}/{tool}/trigger", params=params)
        return _raise_for_status(resp)
    except BatchServiceError:
        raise
    except Exception as exc:
        raise BatchServiceError(f"Batch service unreachable: {exc}") from exc
