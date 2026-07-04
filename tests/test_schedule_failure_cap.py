# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for the scheduler's consecutive-failure cap.

Motivating incident: on prod, a scheduled Garmin extract failed once
(tool config bug — credentials didn't stick). Then the scheduler polled
every 60 s and re-fired the same extract every tick, because a failed
job never advances ``last_success`` and the "next_run_after(cron,
last_success=None)" fallback of ``now - 24h`` produces a time that's
always in the past. ~700 failed jobs per hour piled up in ``jobs.db``
until the schedule was manually torn out of the YAML.

The cap enforces a sane stop: after N consecutive failed
``triggered_by='schedule'`` runs since the most recent success, the
scheduler stops firing. A manual API trigger (``triggered_by`` != 'schedule')
is not counted and, if it succeeds, resets the counter for the next tick.
"""

from pathlib import Path

import pytest

from batch.schedule import (
    _consecutive_failures_since_last_success,
    _hit_failure_cap,
    _max_consecutive_failures,
)

_job_counter = 0


def _write_job(
    model_dir: Path,
    operation: str,
    tool: str,
    status: str,
    completed_at: str,
    triggered_by: str = "schedule",
) -> None:
    """Insert a job row directly into the batch-service SQLite DB.

    All the parameters are relevant for the cap tests — most existing
    ``_write_db_entry`` helpers default ``triggered_by="test"``, which
    would exclude the row from the "counted" set and make cap tests
    silently pass.
    """
    global _job_counter
    from batch_service import db as jobdb

    _job_counter += 1
    job_id = f"cap-test-{_job_counter}"
    with jobdb.get_connection(model_dir) as con:
        jobdb.insert_job(
            con,
            job_id=job_id,
            model=model_dir.name,
            operation=operation,
            tool=tool,
            status="running",
            started_at=completed_at,
            triggered_by=triggered_by,
        )
        jobdb.update_job(con, job_id, status=status, completed_at=completed_at)


# ---------------------------------------------------------------------------
# _max_consecutive_failures — env-var driven default
# ---------------------------------------------------------------------------


class TestMaxConsecutiveFailures:
    def test_default_is_five(self, monkeypatch):
        """Ops shouldn't need to set an env var for the sane default."""
        monkeypatch.delenv("VZ_SCHEDULE_MAX_FAILURES", raising=False)
        assert _max_consecutive_failures() == 5

    def test_env_var_overrides(self, monkeypatch):
        """Ops can tighten to 3 or loosen to 10 without a redeploy."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "3")
        assert _max_consecutive_failures() == 3

    def test_negative_falls_back_to_default(self, monkeypatch):
        """A negative or zero cap is nonsensical (schedule would never
        fire). Fall back to the default rather than accidentally disabling
        all scheduled runs."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "0")
        assert _max_consecutive_failures() == 5
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "-1")
        assert _max_consecutive_failures() == 5

    def test_non_int_falls_back_to_default(self, monkeypatch):
        """A typo in the env var mustn't silently disable scheduling."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "not-a-number")
        assert _max_consecutive_failures() == 5


# ---------------------------------------------------------------------------
# _consecutive_failures_since_last_success — the counting logic
# ---------------------------------------------------------------------------


class TestConsecutiveFailures:
    def test_zero_when_no_jobs(self, tmp_path):
        """Empty jobs table → 0 failures. Guards against a fresh install
        being blocked by the cap when there's nothing to fail on yet."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        n = _consecutive_failures_since_last_success(model_dir, "extract", "garmin")
        assert n == 0

    def test_counts_all_failures_when_never_succeeded(self, tmp_path):
        """The original incident case — no successful run, all scheduled
        runs failed. Should count them all."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(7):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        n = _consecutive_failures_since_last_success(model_dir, "extract", "garmin")
        assert n == 7

    def test_resets_after_success(self, tmp_path):
        """A successful run mid-stream resets the count. This is the
        "user fixed the bug, ran a manual extract that succeeded" case —
        next scheduler tick sees zero failures since success."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        _write_job(model_dir, "extract", "garmin", "failed",
                   "2026-07-04T05:00:00Z")
        _write_job(model_dir, "extract", "garmin", "failed",
                   "2026-07-04T05:01:00Z")
        _write_job(model_dir, "extract", "garmin", "completed",
                   "2026-07-04T05:02:00Z", triggered_by="manual")
        # Two later failures should count post-reset — but zero for now.
        n = _consecutive_failures_since_last_success(model_dir, "extract", "garmin")
        assert n == 0

    def test_counts_only_failures_after_last_success(self, tmp_path):
        """Failures BEFORE the last success shouldn't count. Only the
        gap between last success and now matters — that's the "run of
        failures we're worried about"."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(4):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T04:{i:02d}:00Z")
        _write_job(model_dir, "extract", "garmin", "completed",
                   "2026-07-04T04:30:00Z", triggered_by="manual")
        _write_job(model_dir, "extract", "garmin", "failed",
                   "2026-07-04T05:00:00Z")
        _write_job(model_dir, "extract", "garmin", "failed",
                   "2026-07-04T05:01:00Z")
        n = _consecutive_failures_since_last_success(model_dir, "extract", "garmin")
        assert n == 2

    def test_ignores_manual_failures(self, tmp_path):
        """Only ``triggered_by='schedule'`` counts. A user manually
        retrying an extract five times to debug shouldn't push the
        scheduler over the cap."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(6):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z", triggered_by="manual")
        n = _consecutive_failures_since_last_success(model_dir, "extract", "garmin")
        assert n == 0

    def test_isolates_by_tool(self, tmp_path):
        """A failing Garmin extract doesn't block Jira. Each (operation,
        tool) tracks its own counter."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(6):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        # Jira has zero failures
        assert _consecutive_failures_since_last_success(model_dir, "extract", "jira") == 0
        # Garmin is over the cap
        assert _consecutive_failures_since_last_success(model_dir, "extract", "garmin") == 6

    def test_isolates_by_operation(self, tmp_path):
        """Failing extract doesn't block mapper runs. Each operation
        tracks separately."""
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(6):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        # Mapper failures are counted under operation='map' + tool='__all__'
        assert _consecutive_failures_since_last_success(model_dir, "map", "__all__") == 0


# ---------------------------------------------------------------------------
# _hit_failure_cap — the "does this scheduler tick skip" check
# ---------------------------------------------------------------------------


class TestHitFailureCap:
    def test_below_cap_returns_false(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "5")
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(4):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        assert _hit_failure_cap(model_dir, "extract", "garmin") is False

    def test_at_cap_returns_true(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "5")
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(5):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        assert _hit_failure_cap(model_dir, "extract", "garmin") is True

    def test_above_cap_returns_true(self, tmp_path, monkeypatch):
        """The original prod state — 82 failures, cap of 5."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "5")
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(82):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i // 60:02d}:{i % 60:02d}:00Z")
        assert _hit_failure_cap(model_dir, "extract", "garmin") is True

    def test_custom_cap_via_env(self, tmp_path, monkeypatch):
        """Tightened to 3 — trips earlier."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "3")
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(3):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        assert _hit_failure_cap(model_dir, "extract", "garmin") is True

    def test_logs_at_warning_level_when_capped(
        self, tmp_path, monkeypatch, caplog,
    ):
        """First-time trip should log at WARNING so ops sees a clear
        signal in the scheduler log alongside the silent skip. If we
        ever regress and log at DEBUG or INFO, ops will notice failed
        runs stopping with no explanation."""
        import logging
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "3")
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        for i in range(3):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        with caplog.at_level(logging.WARNING, logger="batch.schedule"):
            _hit_failure_cap(model_dir, "extract", "garmin")
        assert any(
            "Skipping scheduled" in r.message and "consecutive failed runs" in r.message
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# Integration — cap actually gates extractors_due / mappers_due / entities_due
# ---------------------------------------------------------------------------


class TestExtractorsDueRespectCap:
    def test_extractors_due_skips_when_capped(self, tmp_path, monkeypatch):
        """The whole point of the cap — a broken extractor with a valid
        cron and 5+ failed runs disappears from the due list. The
        scheduler's next tick submits nothing, jobs.db stops filling up."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "5")

        from core import metadata_db
        model_dir = tmp_path / "mymodel"
        (model_dir / "extractors").mkdir(parents=True)
        content = """schedule:
  cron: "* * * * *"
tasks:
  - name: garmin_task
    tool: garmin
    command: activities
    params: {}
"""
        metadata_db.record_version(model_dir, "extractor", "garmin", content)

        # Below cap → due
        for i in range(4):
            _write_job(model_dir, "extract", "garmin", "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        from batch.schedule import extractors_due
        assert "garmin" in extractors_due(model_dir)

        # At cap → NOT due
        _write_job(model_dir, "extract", "garmin", "failed",
                   "2026-07-04T05:05:00Z")
        assert "garmin" not in extractors_due(model_dir)


@pytest.mark.parametrize("operation,tool,func_name,cron_yaml_key,artifact_type", [
    ("map", "__all__", "mappers_due", "schedule", "mapper"),
])
class TestMappersDueRespectCap:
    """Materialize has the same shape — one job per model, tool='__all__'."""

    def test_gates_when_capped(
        self, tmp_path, monkeypatch, operation, tool, func_name, cron_yaml_key, artifact_type,
    ):
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "5")

        from core import metadata_db
        model_dir = tmp_path / "mymodel"
        (model_dir / f"{artifact_type}s").mkdir(parents=True)

        # Seed one mapper with an ever-due cron
        content = (
            'schedule:\n  cron: "* * * * *"\n'
            "mapper: activity\n"
            "sources: [{alias: a, table: raw, columns: [x]}]\n"
            "grain: a\n"
            "targets: [{entity: Activity, columns: [{name: c, expr: a.x}]}]\n"
        )
        metadata_db.record_version(model_dir, "mapper", "activity", content)

        from batch.schedule import mappers_due
        # Below cap → due
        assert mappers_due(model_dir) != []
        for i in range(5):
            _write_job(model_dir, operation, tool, "failed",
                       f"2026-07-04T05:{i:02d}:00Z")
        # At cap → NOT due — the whole pass is skipped
        assert mappers_due(model_dir) == []
