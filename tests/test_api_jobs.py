# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/job_service.py — in-memory job store."""

import time

import pytest

from api.services.job_service import JobService, JobStatus


@pytest.fixture
def svc():
    return JobService()


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

def test_create_returns_running_job(svc):
    job = svc.create(model="m1", operation="extract")
    assert job.status == JobStatus.running
    assert job.model == "m1"
    assert job.operation == "extract"
    assert job.job_id


def test_create_unique_ids(svc):
    j1 = svc.create(model="m1", operation="extract")
    j2 = svc.create(model="m1", operation="extract")
    assert j1.job_id != j2.job_id


def test_create_optional_kwargs(svc):
    job = svc.create(model="m1", operation="extract", extractor="ext_jira", task="fetch_issues")
    assert job.extractor == "ext_jira"
    assert job.task == "fetch_issues"


def test_create_started_at_is_set(svc):
    job = svc.create(model="m1", operation="extract")
    assert job.started_at is not None
    assert "T" in job.started_at  # ISO format


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

def test_get_returns_job(svc):
    job = svc.create(model="m1", operation="extract")
    found = svc.get("m1", job.job_id)
    assert found is not None
    assert found.job_id == job.job_id


def test_get_returns_none_for_wrong_model(svc):
    job = svc.create(model="m1", operation="extract")
    assert svc.get("m2", job.job_id) is None


def test_get_returns_none_for_unknown_id(svc):
    assert svc.get("m1", "does-not-exist") is None


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def test_list_filters_by_model(svc):
    svc.create(model="m1", operation="extract")
    svc.create(model="m2", operation="extract")
    result = svc.list("m1")
    assert len(result) == 1
    assert result[0].model == "m1"


def test_list_filters_by_status(svc):
    j1 = svc.create(model="m1", operation="extract")
    _j2 = svc.create(model="m1", operation="extract")
    svc.complete(j1.job_id, {"rows": 10})
    result = svc.list("m1", status=JobStatus.completed)
    assert len(result) == 1
    assert result[0].job_id == j1.job_id


def test_list_filters_by_operation(svc):
    svc.create(model="m1", operation="extract")
    svc.create(model="m1", operation="materialize")
    result = svc.list("m1", operation="extract")
    assert len(result) == 1
    assert result[0].operation == "extract"


def test_list_respects_limit(svc):
    for _ in range(5):
        svc.create(model="m1", operation="extract")
    result = svc.list("m1", limit=3)
    assert len(result) == 3


def test_list_sorted_newest_first(svc):
    """Jobs are sorted newest-first by started_at timestamp."""
    from unittest.mock import patch

    timestamps = ["2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z"]
    call_count = 0

    def fake_now(self):
        nonlocal call_count
        ts = timestamps[min(call_count, len(timestamps) - 1)]
        call_count += 1
        return ts

    with patch.object(type(svc), "_now", fake_now):
        _j1 = svc.create(model="m1", operation="extract")
        j2 = svc.create(model="m1", operation="extract")

    result = svc.list("m1")
    assert result[0].job_id == j2.job_id


# ---------------------------------------------------------------------------
# complete / fail
# ---------------------------------------------------------------------------

def test_complete_updates_status_and_result(svc):
    job = svc.create(model="m1", operation="extract")
    svc.complete(job.job_id, {"records_written": 42})
    updated = svc.get("m1", job.job_id)
    assert updated.status == JobStatus.completed
    assert updated.result == {"records_written": 42}
    assert updated.completed_at is not None


def test_fail_updates_status_and_error(svc):
    job = svc.create(model="m1", operation="extract")
    svc.fail(job.job_id, "something went wrong")
    updated = svc.get("m1", job.job_id)
    assert updated.status == JobStatus.failed
    assert updated.error == "something went wrong"
    assert updated.completed_at is not None


def test_complete_noop_for_unknown_id(svc):
    # Should not raise
    svc.complete("nonexistent", {"rows": 1})


def test_fail_noop_for_unknown_id(svc):
    svc.fail("nonexistent", "err")


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

def test_submit_runs_function(svc):
    results = []

    def _fn():
        results.append("done")

    svc.submit(_fn)
    # Give the daemon thread a moment to run
    for _ in range(20):
        if results:
            break
        time.sleep(0.01)

    assert results == ["done"]
