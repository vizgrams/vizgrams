# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``batch_service/scheduler.py`` — the schedule-tick loop.

Focused on ``_any_writer_running``, the shared gate that stops the
scheduler from stacking a fresh writer job on top of one already queued /
running. DuckDB is single-writer + all four batch operations serialise on
the fcntl model lock, so submitting a second writer only creates a queue
that will time out at 300 s and fail — the "everything in running status"
report the user hit twice.
"""

from batch_service import db as jobdb
from batch_service.scheduler import _any_writer_running


def _insert_running(job_id: str, model: str, operation: str, tool: str = "__all__") -> None:
    with jobdb.get_connection() as con:
        jobdb.insert_job(
            con,
            job_id=job_id, model=model, operation=operation, tool=tool,
            status="running", started_at="2026-07-02T05:36:09Z",
            triggered_by="schedule",
        )


def test_reports_no_writer_when_none_running():
    """Empty DB — nothing to gate on. The scheduler should proceed to
    submit whatever is due."""
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is False
    assert other is None


def test_extract_blocks_further_extract_submission():
    """The recurring symptom: scheduler ticks at :00, extract git is
    already grinding, but the scheduler was previously happy to also fire
    file / git_codeowners / jira / iagai_metadata — all of which then sat
    behind git's fcntl lock and failed at t=300s. New gate must catch this
    same-operation case."""
    _insert_running("extract-git-1", "iagai", "extract", tool="git")
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is True
    assert other == "extract-git-1"


def test_extract_blocks_map_submission():
    """Cross-operation case: extract is running so a scheduled ``map
    __all__`` must be skipped, not queued. Old scheduler split this into
    per-operation checks and let map through — which is how the "all six
    running" state happened."""
    _insert_running("extract-git-1", "iagai", "extract", tool="git")
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is True
    assert other == "extract-git-1"


def test_map_blocks_materialize_submission():
    """Same in reverse — a live mapper must gate an incoming
    materialize."""
    _insert_running("map-1", "iagai", "map")
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is True
    assert other == "map-1"


def test_reconcile_blocks_further_submissions():
    """Reconcile takes the same fcntl lock and must gate too."""
    _insert_running("reconcile-1", "iagai", "reconcile")
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is True


def test_completed_and_failed_jobs_dont_block():
    """Only ``running`` jobs count. Yesterday's failed extract and
    completed materialize must NOT gate a fresh scheduler pass — otherwise
    a single crash permanently disables the schedule."""
    with jobdb.get_connection() as con:
        jobdb.insert_job(
            con, job_id="old-failed", model="iagai", operation="extract",
            tool="git", status="failed", started_at="2026-07-01T00:00:00Z",
            triggered_by="schedule",
        )
        jobdb.insert_job(
            con, job_id="old-done", model="iagai", operation="map",
            tool="__all__", status="completed",
            started_at="2026-07-01T00:00:00Z", triggered_by="schedule",
        )
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is False
    assert other is None


def test_writer_on_other_model_doesnt_block():
    """Serialisation is per-model. A running extract on ``openflights``
    must not gate a fresh submit on ``iagai`` — different fcntl lock file,
    different DuckDB."""
    _insert_running("other-model-extract", "openflights", "extract", tool="git")
    with jobdb.get_connection() as con:
        is_busy, other = _any_writer_running(con, "iagai")
    assert is_busy is False
