# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for batch.schedule — cron-based extraction scheduling."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from batch.schedule import _is_due, _last_success_time, _next_run_after, _read_schedule, extractors_due, next_run_times

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HOURLY = "0 * * * *"
DAILY = "0 6 * * *"


def _write_extractor(extractors_dir: Path, tool: str, cron: str | None = None) -> Path:
    """Write a minimal extractor YAML, with or without a schedule block.

    Also seeds the metadata DB so schedule functions that read from DB work.
    """
    path = extractors_dir / f"extractor_{tool}.yaml"
    data: dict = {"tasks": [{"name": f"{tool}_task", "tool": tool, "command": "load", "params": {}}]}
    if cron is not None:
        data["schedule"] = {"cron": cron}
    content = yaml.dump(data)
    path.write_text(content)
    model_dir = extractors_dir.parent
    if model_dir.exists():
        from core import metadata_db
        metadata_db.record_version(model_dir, "extractor", tool, content)
    return path


_job_counter = 0


def _write_db_entry(model_dir: Path, tool: str, status: str, completed_at: str) -> None:
    """Insert a job row directly into the batch-service SQLite DB."""
    global _job_counter
    from batch_service import db as jobdb

    _job_counter += 1
    job_id = f"test-job-{_job_counter}"
    with jobdb.get_connection(model_dir) as con:
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
        jobdb.update_job(con, job_id, status=status, completed_at=completed_at)


# ---------------------------------------------------------------------------
# _read_schedule
# ---------------------------------------------------------------------------


class TestReadSchedule:
    def test_returns_cron_when_present(self, tmp_path):
        path = _write_extractor(tmp_path, "file", cron=HOURLY)
        tool, cron = _read_schedule(path)
        assert tool == "file"
        assert cron == HOURLY

    def test_returns_none_when_no_schedule_block(self, tmp_path):
        path = _write_extractor(tmp_path, "file", cron=None)
        _, cron = _read_schedule(path)
        assert cron is None

    def test_returns_none_for_invalid_cron(self, tmp_path):
        path = _write_extractor(tmp_path, "file")
        # Manually overwrite with bad cron
        data = yaml.safe_load(path.read_text())
        data["schedule"] = {"cron": "not-a-cron"}
        path.write_text(yaml.dump(data))
        _, cron = _read_schedule(path)
        assert cron is None

    def test_tool_name_from_task(self, tmp_path):
        path = _write_extractor(tmp_path, "jira", cron=DAILY)
        tool, _ = _read_schedule(path)
        assert tool == "jira"

    def test_tool_name_from_task_when_filename_differs(self, tmp_path):
        """Filename stem is 'github' but task tool is 'git' — task wins."""
        path = tmp_path / "extractor_github.yaml"
        data = {"schedule": {"cron": DAILY}, "tasks": [{"name": "t", "tool": "git", "command": "c", "params": {}}]}
        path.write_text(yaml.dump(data))
        tool, _ = _read_schedule(path)
        assert tool == "git"

    def test_returns_none_for_unreadable_yaml(self, tmp_path):
        path = tmp_path / "extractor_broken.yaml"
        path.write_text(": : invalid: yaml: {{")
        tool, cron = _read_schedule(path)
        assert tool == "broken"
        assert cron is None


# ---------------------------------------------------------------------------
# _next_run_after
# ---------------------------------------------------------------------------


class TestNextRunAfter:
    def test_returns_next_run_after_given_time(self):
        # hourly cron; base is 10:30 → next should be 11:00
        base = datetime(2026, 3, 30, 10, 30, tzinfo=UTC)
        nxt = _next_run_after(HOURLY, base)
        assert nxt == datetime(2026, 3, 30, 11, 0, tzinfo=UTC)

    def test_returns_next_run_after_a_given_datetime(self):
        base = datetime(2026, 3, 30, 14, 0, tzinfo=UTC)
        nxt = _next_run_after(HOURLY, base)
        assert nxt == datetime(2026, 3, 30, 15, 0, tzinfo=UTC)

    def test_returns_none_for_bad_cron(self):
        result = _next_run_after("not-valid", None)
        assert result is None


# ---------------------------------------------------------------------------
# _is_due
# ---------------------------------------------------------------------------


class TestIsDue:
    def test_due_when_next_run_in_past(self):
        last = datetime(2026, 3, 30, 5, 0, tzinfo=UTC)
        now = datetime(2026, 3, 30, 7, 0, tzinfo=UTC)
        assert _is_due(DAILY, last, now, "file") is True

    def test_not_due_when_next_run_in_future(self):
        last = datetime(2026, 3, 30, 7, 0, tzinfo=UTC)
        now = datetime(2026, 3, 30, 7, 30, tzinfo=UTC)
        assert _is_due(DAILY, last, now, "file") is False

    def test_not_due_when_never_run_and_next_occurrence_in_future(self):
        # never run: next run computed relative to now, so it's always in the future
        now = datetime(2026, 3, 30, 12, 30, tzinfo=UTC)
        assert _is_due(HOURLY, None, now, "file") is False

    def test_due_when_never_run_and_exactly_at_cron_time(self):
        # now IS exactly a cron tick — next_run after now is the *following* tick, still not due
        now = datetime(2026, 3, 30, 13, 0, tzinfo=UTC)  # exactly on the hour
        assert _is_due(HOURLY, None, now, "file") is False

    def test_not_due_exactly_at_next_run_minus_one_second(self):
        # Last run was at daily-6am; next is tomorrow 6am; now is just before it
        last = datetime(2026, 3, 30, 6, 0, tzinfo=UTC)
        now = datetime(2026, 3, 31, 5, 59, 59, tzinfo=UTC)
        assert _is_due(DAILY, last, now, "file") is False

    def test_due_exactly_at_next_run_time(self):
        last = datetime(2026, 3, 30, 6, 0, tzinfo=UTC)
        now = datetime(2026, 3, 31, 6, 0, tzinfo=UTC)
        assert _is_due(DAILY, last, now, "file") is True


# ---------------------------------------------------------------------------
# _last_success_time
# ---------------------------------------------------------------------------


class TestLastSuccessTime:
    def test_returns_none_when_no_audit_log(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        result = _last_success_time(model_dir, "file")
        assert result is None

    def test_returns_none_when_no_completed_run(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        _write_db_entry(model_dir, "file", "failed", "2026-03-30T05:00:00Z")
        result = _last_success_time(model_dir, "file")
        assert result is None

    def test_returns_completed_at_timestamp(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        _write_db_entry(model_dir, "file", "completed", "2026-03-30T06:00:00Z")
        result = _last_success_time(model_dir, "file")
        assert result == datetime(2026, 3, 30, 6, 0, tzinfo=UTC)

    def test_ignores_other_tools(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        _write_db_entry(model_dir, "jira", "completed", "2026-03-30T06:00:00Z")
        result = _last_success_time(model_dir, "file")
        assert result is None

    def test_returns_most_recent_success(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        _write_db_entry(model_dir, "file", "completed", "2026-03-28T06:00:00Z")
        _write_db_entry(model_dir, "file", "completed", "2026-03-30T06:00:00Z")
        result = _last_success_time(model_dir, "file")
        # Should return the most recent (read_job_history sorts newest-first)
        assert result == datetime(2026, 3, 30, 6, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# extractors_due
# ---------------------------------------------------------------------------


class TestExtractorsDue:
    def _setup_model(self, tmp_path) -> Path:
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        (model_dir / "extractors").mkdir()
        return model_dir

    def test_returns_empty_when_no_extractors(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        assert extractors_due(model_dir) == []

    def test_skips_extractors_without_schedule(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file")  # no cron
        assert extractors_due(model_dir) == []

    def test_due_when_never_run(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        # Never run → 24h lookback base, so a cron that has fired today is due
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        assert extractors_due(model_dir) == ["file"]

    def test_due_when_overdue(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        # Last success was 2 hours ago → overdue
        old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "file", "completed", old)
        assert extractors_due(model_dir) == ["file"]

    def test_skips_extractor_not_yet_due(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        # Daily at 06:00; last run was a moment ago → not yet due
        _write_extractor(model_dir / "extractors", "file", cron=DAILY)
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "file", "completed", recent)
        assert extractors_due(model_dir) == []

    def test_returns_only_due_extractors(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        _write_extractor(model_dir / "extractors", "jira", cron=DAILY)
        # file: last ran 2 hours ago → overdue
        old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "file", "completed", old)
        # jira: just ran → not due
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "jira", "completed", recent)
        result = extractors_due(model_dir)
        assert "file" in result
        assert "jira" not in result

    def test_missing_extractors_dir_returns_empty(self, tmp_path):
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        # extractors/ subdir does not exist — glob returns empty, no error raised
        assert extractors_due(model_dir) == []


# ---------------------------------------------------------------------------
# next_run_times
# ---------------------------------------------------------------------------


class TestNextRunTimes:
    def _setup_model(self, tmp_path) -> Path:
        model_dir = tmp_path / "mymodel"
        model_dir.mkdir()
        (model_dir / "extractors").mkdir()
        return model_dir

    def test_returns_empty_when_no_scheduled_extractors(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file")
        assert next_run_times(model_dir) == []

    def test_returns_entry_for_each_scheduled_extractor(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        _write_extractor(model_dir / "extractors", "jira", cron=DAILY)
        result = next_run_times(model_dir)
        assert len(result) == 2

    def test_entry_fields(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        result = next_run_times(model_dir)
        entry = result[0]
        assert entry["tool"] == "file"
        assert entry["cron"] == HOURLY
        assert entry["last_success"] is None
        assert entry["next_run"] is not None
        assert entry["due"] is True  # never run → 24h lookback, hourly cron is due

    def test_not_due_when_recently_completed(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "jira", cron=DAILY)
        recent = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "jira", "completed", recent)
        result = next_run_times(model_dir)
        assert result[0]["due"] is False
        assert result[0]["last_success"] == recent

    def test_next_run_is_valid_iso_timestamp(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        entry = next_run_times(model_dir)[0]
        # Should parse without error and match expected format
        assert entry["next_run"].endswith("Z")
        datetime.fromisoformat(entry["next_run"].rstrip("Z")).replace(tzinfo=UTC)  # no exception

    def test_due_when_overdue_by_many_hours(self, tmp_path):
        model_dir = self._setup_model(tmp_path)
        _write_extractor(model_dir / "extractors", "file", cron=HOURLY)
        old = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_db_entry(model_dir, "file", "completed", old)
        result = next_run_times(model_dir)
        assert result[0]["due"] is True
