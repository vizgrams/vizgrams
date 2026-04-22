# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Schedule evaluation: determine which extractors and mappers are due to run.

Both extractor and mapper YAMLs may include an optional ``schedule`` block::

    schedule:
      cron: "0 6 * * *"   # daily at 06:00 UTC

For extractors this sits alongside ``tasks``; for mappers it sits at the top
level.  If no ``schedule`` block is present the artifact is considered
*unscheduled* and will never be started automatically.

A tool is *due* when all of the following hold:

  1. Its YAML has a ``schedule.cron`` expression.
  2. Either it has never completed successfully, OR the croniter next-run
     timestamp after the last successful completion is ≤ now (UTC).

Usage::

    from batch.schedule import extractors_due, mappers_due
    for tool_name in extractors_due(model_dir):
        ...  # trigger extraction
    if mappers_due(model_dir):
        ...  # trigger wave-based mapper run
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import yaml
from croniter import CroniterBadCronError, croniter

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extractors_due(model_dir: Path) -> list[str]:
    """Return the tool names of all extractors that are due to run now.

    An extractor is included if it carries a ``schedule.cron`` block in its
    YAML and its next scheduled run (relative to its last successful
    completion) is on or before the current UTC time.

    Extractors with no schedule block are silently skipped.
    Extractors with an invalid cron expression are logged and skipped.

    Args:
        model_dir: Root directory of the model.

    Returns:
        Tool names (strings) in discovery order.
    """
    from core import metadata_db
    now = datetime.now(UTC)
    due: list[str] = []

    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        tool_name, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue

        last_success = _last_success_time(model_dir, tool_name)
        if _is_due(cron_expr, last_success, now, tool_name):
            due.append(tool_name)

    return due


def next_run_times(model_dir: Path) -> list[dict]:
    """Return schedule status for every scheduled extractor and mapper.

    Returns a list of dicts with keys:

    * ``tool``         — tool name
    * ``type``         — ``'extractor'`` or ``'mapper'``
    * ``cron``         — cron expression string
    * ``last_success`` — ISO 8601 UTC string, or ``null`` if never run
    * ``next_run``     — ISO 8601 UTC string of the next scheduled run
    * ``due``          — bool, True if the next run is ≤ now

    Extractors and mappers without a schedule block are omitted.
    """
    from core import metadata_db
    now = datetime.now(UTC)
    from datetime import timedelta
    result = []

    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        tool_name, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue

        last_success = _last_success_time(model_dir, tool_name)
        base = last_success if last_success is not None else now
        next_run = _next_run_after(cron_expr, base)

        result.append({
            "tool": tool_name,
            "type": "extractor",
            "cron": cron_expr,
            "last_success": last_success.strftime("%Y-%m-%dT%H:%M:%SZ") if last_success else None,
            "next_run": next_run.strftime("%Y-%m-%dT%H:%M:%SZ") if next_run else None,
            "due": next_run is not None and next_run <= now,
        })

    # Per-entity schedules — read from each entity's ontology YAML
    last_materialize_success = _last_materialize_success(model_dir)
    _entity_base = last_materialize_success if last_materialize_success is not None else (now - timedelta(hours=24))
    for name in metadata_db.list_artifact_names(model_dir, "entity"):
        content = metadata_db.get_current_content(model_dir, "entity", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        next_run = _next_run_after(cron_expr, _entity_base)
        result.append({
            "tool": name,
            "type": "entity",
            "cron": cron_expr,
            "last_success": (
                last_materialize_success.strftime("%Y-%m-%dT%H:%M:%SZ")
                if last_materialize_success else None
            ),
            "next_run": next_run.strftime("%Y-%m-%dT%H:%M:%SZ") if next_run else None,
            "due": next_run is not None and next_run <= now,
        })

    # Per-mapper schedules — read from each mapper's YAML
    last_mapper_success = _last_mapper_success(model_dir)
    _mapper_base = last_mapper_success if last_mapper_success is not None else (now - timedelta(hours=24))
    for name in metadata_db.list_artifact_names(model_dir, "mapper"):
        content = metadata_db.get_current_content(model_dir, "mapper", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        next_run = _next_run_after(cron_expr, _mapper_base)
        result.append({
            "tool": name,
            "type": "mapper",
            "cron": cron_expr,
            "last_success": last_mapper_success.strftime("%Y-%m-%dT%H:%M:%SZ") if last_mapper_success else None,
            "next_run": next_run.strftime("%Y-%m-%dT%H:%M:%SZ") if next_run else None,
            "due": next_run is not None and next_run <= now,
        })

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_schedule_from_content(content: str, fallback_name: str) -> tuple[str, str | None]:
    """Return (tool_name, cron_expr | None) from extractor YAML content.

    ``tool_name`` is taken from the first task's tool field; falls back to
    ``fallback_name`` (the DB artifact name, i.e. filename stem without prefix).
    ``cron_expr`` is the value of ``schedule.cron``, or None if absent.
    """
    tool_name = fallback_name
    try:
        data = yaml.safe_load(content) or {}
    except Exception:
        _log.warning("Could not parse extractor YAML for %r", fallback_name)
        return tool_name, None

    tasks = data.get("tasks") or []
    if tasks and isinstance(tasks[0], dict) and tasks[0].get("tool"):
        tool_name = tasks[0]["tool"]

    schedule = data.get("schedule") or {}
    cron_expr: str | None = schedule.get("cron")
    if cron_expr is None:
        return tool_name, None

    try:
        croniter(cron_expr)
    except CroniterBadCronError:
        _log.error(
            "Invalid cron expression %r for extractor %r — will not be scheduled",
            cron_expr,
            fallback_name,
        )
        return tool_name, None

    return tool_name, cron_expr


def entities_due(model_dir: Path) -> list[str]:
    """Return entity names whose materialization schedule is due.

    Each entity ontology YAML may include an optional ``schedule`` block::

        schedule:
          cron: "0 4 * * *"  # daily at 04:00 UTC

    Entities without a ``schedule`` block are skipped.  The last successful
    run-all-entities materialize job time is used as the shared
    ``last_success`` reference (all entities are materialized together).

    When no successful run exists yet, a 24-hour lookback is used so a
    freshly-scheduled entity fires on its first upcoming cron slot.

    Returns entity names in discovery order.
    """
    from datetime import timedelta

    from core import metadata_db

    now = datetime.now(UTC)
    last_success = _last_materialize_success(model_dir)
    base = last_success if last_success is not None else (now - timedelta(hours=24))
    due: list[str] = []

    for name in metadata_db.list_artifact_names(model_dir, "entity"):
        content = metadata_db.get_current_content(model_dir, "entity", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        next_run = _next_run_after(cron_expr, base)
        if next_run is not None and next_run <= now:
            due.append(name)

    return due


def _last_materialize_success(model_dir: Path) -> datetime | None:
    """Return UTC datetime of the most recent successful run-all-entities materialize job."""
    try:
        from batch_service import db as jobdb

        with jobdb.get_connection(model_dir) as con:
            row = con.execute(
                """
                SELECT completed_at FROM jobs
                WHERE model = ? AND operation = 'materialize' AND tool = '__all__' AND status = 'completed'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (model_dir.name,),
            ).fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0].rstrip("Z")).replace(tzinfo=UTC)
    except Exception:
        pass
    return None


def mappers_due(model_dir: Path) -> list[str]:
    """Return the names of all mappers whose schedule is due.

    Each mapper YAML may include an optional ``schedule`` block::

        schedule:
          cron: "0 2 * * *"  # daily at 02:00 UTC

    Mappers without a ``schedule`` block are skipped.  The last successful
    run-all-mappers job time is used as the shared ``last_success`` reference
    (mappers are run together in dependency-ordered waves).

    When no successful run exists yet, a 24-hour lookback is used as the base
    so that a freshly-scheduled mapper fires on its first upcoming cron slot
    rather than silently waiting an extra full period.

    Returns mapper names in discovery order.
    """
    from datetime import timedelta

    from core import metadata_db

    now = datetime.now(UTC)
    last_success = _last_mapper_success(model_dir)
    # If mappers have never run, look back 24h so we catch a cron slot that
    # fired today even though the service/schedule was set up earlier.
    base = last_success if last_success is not None else (now - timedelta(hours=24))
    due: list[str] = []

    for name in metadata_db.list_artifact_names(model_dir, "mapper"):
        content = metadata_db.get_current_content(model_dir, "mapper", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        next_run = _next_run_after(cron_expr, base)
        if next_run is not None and next_run <= now:
            due.append(name)

    return due


def _last_mapper_success(model_dir: Path) -> datetime | None:
    """Return UTC datetime of the most recent successful run-all-mappers job."""
    try:
        from batch_service import db as jobdb

        with jobdb.get_connection(model_dir) as con:
            row = con.execute(
                """
                SELECT completed_at FROM jobs
                WHERE model = ? AND operation = 'map' AND tool = '__all__' AND status = 'completed'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (model_dir.name,),
            ).fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0].rstrip("Z")).replace(tzinfo=UTC)
    except Exception:
        pass
    return None


def _read_schedule(path: Path) -> tuple[str, str | None]:
    """Compatibility shim: read schedule from a YAML file path."""
    tool_name = path.stem.removeprefix("extractor_")
    try:
        content = path.read_text()
    except Exception:
        _log.warning("Could not read extractor YAML: %s", path)
        return tool_name, None
    return _read_schedule_from_content(content, tool_name)


def _last_success_time(model_dir: Path, tool_name: str) -> datetime | None:
    """Return the UTC datetime of the most recent successful extraction run.

    Reads the batch-service SQLite DB — the authoritative store written by
    ``batch_service.executor`` on job completion.  Returns None if no
    successful run exists or the DB is not accessible.
    """
    try:
        from batch_service import db as jobdb

        with jobdb.get_connection(model_dir) as con:
            row = con.execute(
                """
                SELECT completed_at FROM jobs
                WHERE model = ? AND tool = ? AND operation = 'extract' AND status = 'completed'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (model_dir.name, tool_name),
            ).fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0].rstrip("Z")).replace(tzinfo=UTC)
    except Exception:
        pass
    return None


def _next_run_after(cron_expr: str, after: datetime) -> datetime | None:
    """Return the next UTC datetime that the cron fires after *after*."""
    try:
        return croniter(cron_expr, after).get_next(datetime)
    except CroniterBadCronError:
        return None


def _is_due(cron_expr: str, last_success: datetime | None, now: datetime, tool_name: str) -> bool:
    """Return True if the extractor's next scheduled run is on or before *now*.

    When an extractor has never run (last_success is None), the next run is
    computed relative to *now* — so a fresh install waits for the next
    scheduled occurrence rather than running immediately.
    """
    base = last_success if last_success is not None else now
    next_run = _next_run_after(cron_expr, base)
    if next_run is None:
        return False
    due = next_run <= now
    _log.debug(
        "Schedule check: tool=%s cron=%r last_success=%s next_run=%s due=%s",
        tool_name,
        cron_expr,
        last_success,
        next_run,
        due,
    )
    return due
