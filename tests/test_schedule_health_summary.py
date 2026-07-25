# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``batch.schedule.health_summary`` — the aggregate snapshot
that powers the Health page. It's the shape of the payload that matters
most, since the UI colours on ``cap_hit`` and the timestamps and would
show a silently-broken pipeline as green if those signals were missing.
"""

from pathlib import Path

from batch.schedule import health_summary
from tests.test_schedule_failure_cap import _write_job


def _write_extractor_yaml(model_dir: Path, name: str, cron: str, tool: str | None = None) -> None:
    """Write an extractor YAML into the model's metadata store the same way
    the artifact APIs would — so ``health_summary`` reads them exactly the
    same way as production."""
    from core import metadata_db

    tool = tool or name
    yaml_content = (
        f"schedule:\n  cron: {cron!r}\n"
        f"tasks:\n  - name: {name}\n    tool: {tool}\n"
        "    command: noop\n    params: {}\n"
        f"    output:\n      table: {name}\n      write_mode: UPSERT\n"
        "      primary_keys: [id]\n      columns:\n        - name: id\n          json_path: $.id\n"
    )
    metadata_db.record_version(
        model_dir, "extractor", name, yaml_content, via="system",
    )


def _write_mapper_yaml(model_dir: Path, name: str, cron: str | None) -> None:
    from core import metadata_db

    schedule_block = f"schedule:\n  cron: {cron!r}\n" if cron else ""
    yaml_content = (
        f"{schedule_block}"
        f"mapper: {name}\ngrain: {name}\nsources: []\ntargets: []\n"
    )
    metadata_db.record_version(
        model_dir, "mapper", name, yaml_content, via="system",
    )


class TestHealthSummary:
    def test_contains_all_four_sections(self, tmp_path):
        """UI expects exactly extract/map/materialize/reconcile so a fresh
        model without any artifacts still renders a full page."""
        model_dir = tmp_path / "fresh"
        model_dir.mkdir()
        result = health_summary(model_dir)
        assert [s["operation"] for s in result["sections"]] == [
            "extract", "map", "materialize", "reconcile",
        ]

    def test_extract_row_per_tool(self, tmp_path):
        """Each extractor with a schedule gets its own row so per-tool
        health is visible independently (not aggregated into one wave)."""
        model_dir = tmp_path / "m1"
        model_dir.mkdir()
        _write_extractor_yaml(model_dir, "git", "0 23 * * *")
        _write_extractor_yaml(model_dir, "jira", "40 22 * * *")

        result = health_summary(model_dir)
        extract = next(s for s in result["sections"] if s["operation"] == "extract")
        names = {t["name"] for t in extract["targets"]}
        assert names == {"git", "jira"}

    def test_cap_hit_surfaces_on_extract_target(self, tmp_path, monkeypatch):
        """The DORA-null-cliff scenario in reverse: 5 consecutive failures
        without a manual reset should show cap_hit=True so the UI banner
        fires. This is the load-bearing signal — a green row over 5 red
        jobs would defeat the point of the page."""
        monkeypatch.setenv("VZ_SCHEDULE_MAX_FAILURES", "3")
        model_dir = tmp_path / "capped"
        model_dir.mkdir()
        _write_extractor_yaml(model_dir, "git", "0 23 * * *")
        # 3 scheduled failures, no successes since — cap tripped
        for i in range(3):
            _write_job(model_dir, "extract", "git", "failed",
                       f"2026-07-{20 + i}T23:00:00Z", triggered_by="schedule")

        result = health_summary(model_dir)
        row = next(t for s in result["sections"] if s["operation"] == "extract"
                   for t in s["targets"] if t["name"] == "git")
        assert row["cap_hit"] is True
        assert row["failures_since_success"] >= 3

    def test_reconcile_row_present_even_without_history(self, tmp_path):
        """Reconcile is manual-only, so no cron and no schedule discovery.
        But the row must still render — the DORA case turned on a
        never-scheduled reconcile going 27 days stale."""
        model_dir = tmp_path / "no_reconcile_ever"
        model_dir.mkdir()

        result = health_summary(model_dir)
        rec = next(s for s in result["sections"] if s["operation"] == "reconcile")
        assert len(rec["targets"]) == 1
        assert rec["targets"][0]["name"] == "__all__"
        assert rec["targets"][0]["cron"] is None

    def test_wave_row_lists_scheduled_children(self, tmp_path):
        """Map/materialize sections show one wave row plus the list of
        children — the list is what tells an operator *which* mapper
        added a cron slot when the wave next_run shifts."""
        model_dir = tmp_path / "wave"
        model_dir.mkdir()
        _write_mapper_yaml(model_dir, "team",        cron="0 2 * * *")
        _write_mapper_yaml(model_dir, "pull_request", cron="0 3 * * *")
        _write_mapper_yaml(model_dir, "ad_hoc",      cron=None)  # no schedule

        result = health_summary(model_dir)
        map_section = next(s for s in result["sections"] if s["operation"] == "map")
        row = map_section["targets"][0]
        assert row["cron"] == "wave"
        # Only scheduled children — the unscheduled ad_hoc mapper is excluded
        assert set(row["scheduled_children"]) == {"team", "pull_request"}
