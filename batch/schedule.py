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
  3. The count of consecutive failed runs since the last successful run
     is below ``VZ_SCHEDULE_MAX_FAILURES`` (default 5). Once the cap is
     hit, the scheduler stops firing this artifact until a manual trigger
     produces a successful run and resets the counter.

The failure cap exists because a broken extractor with a cron of
``0 5 * * *`` will otherwise re-fire on every 60 s scheduler tick — the
"last success" advancement mechanism can't move past a failure, so
``next_run(cron, last_success=None) → yesterday 05:00 → past → due``
holds forever. That produced ~700 failed extract jobs per hour on
prod when Garmin auth first broke. The cap enforces "sane stop"
behaviour: N attempts, then silence until a human clears the block.

Usage::

    from batch.schedule import extractors_due, mappers_due
    for tool_name in extractors_due(model_dir):
        ...  # trigger extraction
    if mappers_due(model_dir):
        ...  # trigger wave-based mapper run
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import yaml
from croniter import CroniterBadCronError, croniter

_log = logging.getLogger(__name__)

# Consecutive failures since the last successful run at which the scheduler
# stops firing this artifact. A manual trigger via the API bypasses this
# check; if it succeeds, the counter resets automatically (the ordering of
# ``ORDER BY completed_at DESC`` in ``_consecutive_failures_since_last_success``
# means "since last success" naturally shifts forward). Env-var override so
# ops can tighten to 3 or loosen to 10 without a redeploy.
_DEFAULT_MAX_FAILURES = 5


def _max_consecutive_failures() -> int:
    raw = os.environ.get("VZ_SCHEDULE_MAX_FAILURES")
    if not raw:
        return _DEFAULT_MAX_FAILURES
    try:
        n = int(raw)
        return n if n > 0 else _DEFAULT_MAX_FAILURES
    except ValueError:
        _log.warning(
            "VZ_SCHEDULE_MAX_FAILURES=%r not an int; using default %d",
            raw, _DEFAULT_MAX_FAILURES,
        )
        return _DEFAULT_MAX_FAILURES

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
    from datetime import timedelta

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
        # When an extractor has never run, look back 24 h so the next scheduled
        # slot that has already passed today is treated as due.  Using `now` as
        # the base instead would compute next_run as tomorrow and miss forever.
        base = last_success if last_success is not None else now - timedelta(hours=24)
        next_run = _next_run_after(cron_expr, base)
        if next_run is not None and next_run <= now:
            if _hit_failure_cap(model_dir, "extract", tool_name):
                continue
            _log.debug(
                "Extractor due: tool=%s cron=%r last_success=%s next_run=%s",
                tool_name, cron_expr, last_success, next_run,
            )
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
        base = last_success if last_success is not None else now - timedelta(hours=24)
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

    # Materialize is submitted once for all entities together (tool='__all__'),
    # so the failure cap is checked once here rather than per entity. If the
    # scheduler is above the cap, we skip the whole materialize pass — no
    # entity's schedule triggers a submit until a manual run succeeds.
    if _hit_failure_cap(model_dir, "materialize", "__all__"):
        return []

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

    # Mapper jobs run in dependency-ordered waves as a single submit (tool='__all__').
    # Same treatment as materialize — check the cap once, skip the whole pass.
    if _hit_failure_cap(model_dir, "map", "__all__"):
        return []

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


def _consecutive_failures_since_last_success(
    model_dir: Path,
    operation: str,
    tool: str,
) -> int:
    """Count consecutive failed jobs since the most recent successful one
    for the given (model, operation, tool) tuple.

    - operation is one of ``'extract'`` / ``'map'`` / ``'materialize'``.
    - tool is the per-tool name for extractors (``'garmin'``, ``'jira'``),
      or ``'__all__'`` for run-all-mappers / materialize jobs.

    Ordering: fetch the most recent success time; count how many failed
    jobs completed *after* that. If no success exists, count all failed
    jobs for this tuple. Zero means "the last completed job succeeded".

    Only looks at scheduler-triggered jobs (``triggered_by = 'schedule'``).
    A manual API trigger is not counted as a scheduler failure — that
    matches the user model: "I want to debug and retry without waiting
    for the cap to reset".
    """
    try:
        from batch_service import db as jobdb

        with jobdb.get_connection(model_dir) as con:
            row = con.execute(
                """
                SELECT completed_at FROM jobs
                WHERE model = ? AND operation = ? AND tool = ? AND status = 'completed'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (model_dir.name, operation, tool),
            ).fetchone()
            since = row[0] if row and row[0] else '1970-01-01T00:00:00Z'
            failed = con.execute(
                """
                SELECT COUNT(*) FROM jobs
                WHERE model = ? AND operation = ? AND tool = ?
                  AND status = 'failed' AND triggered_by = 'schedule'
                  AND completed_at IS NOT NULL AND completed_at > ?
                """,
                (model_dir.name, operation, tool, since),
            ).fetchone()
            return failed[0] if failed else 0
    except Exception:
        # DB unreadable — err on the side of allowing the tick. The scheduler
        # already tolerates DB flakiness in other helpers with the same
        # bare-except pattern.
        return 0


def _hit_failure_cap(model_dir: Path, operation: str, tool: str) -> bool:
    """Convenience wrapper around ``_consecutive_failures_since_last_success``
    that logs at WARNING level the first time a tool trips the cap on a
    tick, so ops sees a clear signal in the scheduler log.

    Returns True when the cap is reached and scheduling should skip.
    """
    n = _consecutive_failures_since_last_success(model_dir, operation, tool)
    cap = _max_consecutive_failures()
    if n >= cap:
        _log.warning(
            "Skipping scheduled %s/%s for %s — %d consecutive failed runs since last success "
            "(cap=%d). Fix the underlying issue and trigger a manual run to reset the counter.",
            operation, tool, model_dir.name, n, cap,
        )
        return True
    return False


def _last_job(
    model_dir: Path, operation: str, tool: str,
) -> tuple[str | None, str | None]:
    """Return (completed_at, status) of the most recent job of any status
    for this (op, tool). None/None if no jobs exist.
    """
    try:
        from batch_service import db as jobdb

        with jobdb.get_connection(model_dir) as con:
            row = con.execute(
                """
                SELECT completed_at, status FROM jobs
                WHERE model = ? AND operation = ? AND tool = ?
                  AND completed_at IS NOT NULL
                ORDER BY completed_at DESC LIMIT 1
                """,
                (model_dir.name, operation, tool),
            ).fetchone()
            if row:
                return (row[0], row[1])
    except Exception:
        pass
    return (None, None)


def _iso_or_none(dt: datetime | None) -> str | None:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _target_health(
    model_dir: Path,
    operation: str,
    tool: str,
    cron: str | None,
    last_success: datetime | None,
    base: datetime | None,
) -> dict:
    """Build a health-summary row for a single (operation, tool) target.

    ``base`` is what the scheduler would use as the reference for the next-run
    calculation (usually last_success, or a 24h lookback fallback). Callers pass
    the exact same value the scheduler uses so ``next_run`` matches reality.
    """
    now = datetime.now(UTC)
    last_attempt_at, last_attempt_status = _last_job(model_dir, operation, tool)
    failures = _consecutive_failures_since_last_success(model_dir, operation, tool)
    cap = _max_consecutive_failures()
    next_run = _next_run_after(cron, base or now) if cron else None
    return {
        "name": tool,
        "cron": cron,
        "last_success": _iso_or_none(last_success),
        "last_attempt_at": last_attempt_at,
        "last_attempt_status": last_attempt_status,
        "next_run": _iso_or_none(next_run),
        "failures_since_success": failures,
        "failure_cap": cap,
        "cap_hit": failures >= cap,
    }


def health_summary(model_dir: Path) -> dict:
    """Return an aggregated health snapshot for a model — one row per
    operational target (extract per tool, one row for the map/materialize
    waves, one row for reconcile). The UI colours based on cap_hit,
    last_attempt_status, and the timestamps; the backend returns raw
    signals only.
    """
    from datetime import timedelta

    from core import metadata_db

    now = datetime.now(UTC)
    fallback_base = now - timedelta(hours=24)

    # --- Extract: per-tool rows ---
    extract_targets: list[dict] = []
    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        tool_name, cron_expr = _read_schedule_from_content(content, name)
        last_success = _last_success_time(model_dir, tool_name)
        base = last_success if last_success is not None else fallback_base
        extract_targets.append(
            _target_health(model_dir, "extract", tool_name, cron_expr, last_success, base)
        )

    # --- Map: one wave row (tool='__all__'), plus scheduled mapper list ---
    # The scheduler fires the wave when any scheduled mapper's cron slot
    # is due, so the wave's "next_run" is the earliest child next_run.
    scheduled_mappers: list[str] = []
    map_next_runs: list[datetime] = []
    map_last_success = _last_mapper_success(model_dir)
    map_base = map_last_success if map_last_success is not None else fallback_base
    for name in metadata_db.list_artifact_names(model_dir, "mapper"):
        content = metadata_db.get_current_content(model_dir, "mapper", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        scheduled_mappers.append(name)
        nxt = _next_run_after(cron_expr, map_base)
        if nxt is not None:
            map_next_runs.append(nxt)
    map_row = _target_health(
        model_dir, "map", "__all__",
        "wave" if scheduled_mappers else None,
        map_last_success, map_base,
    )
    map_row["next_run"] = _iso_or_none(min(map_next_runs) if map_next_runs else None)
    map_row["scheduled_children"] = scheduled_mappers

    # --- Materialize: one wave row + scheduled entity list ---
    scheduled_entities: list[str] = []
    mat_next_runs: list[datetime] = []
    mat_last_success = _last_materialize_success(model_dir)
    mat_base = mat_last_success if mat_last_success is not None else fallback_base
    for name in metadata_db.list_artifact_names(model_dir, "entity"):
        content = metadata_db.get_current_content(model_dir, "entity", name)
        if not content:
            continue
        _, cron_expr = _read_schedule_from_content(content, name)
        if cron_expr is None:
            continue
        scheduled_entities.append(name)
        nxt = _next_run_after(cron_expr, mat_base)
        if nxt is not None:
            mat_next_runs.append(nxt)
    mat_row = _target_health(
        model_dir, "materialize", "__all__",
        "wave" if scheduled_entities else None,
        mat_last_success, mat_base,
    )
    mat_row["next_run"] = _iso_or_none(min(mat_next_runs) if mat_next_runs else None)
    mat_row["scheduled_children"] = scheduled_entities

    # --- Reconcile: manual-only, single row ---
    rec_last_success_at, _ = _last_job(model_dir, "reconcile", "__all__")
    reconcile_row = {
        "name": "__all__",
        "cron": None,
        "last_success": rec_last_success_at,
        "last_attempt_at": _last_job(model_dir, "reconcile", "__all__")[0],
        "last_attempt_status": _last_job(model_dir, "reconcile", "__all__")[1],
        "next_run": None,
        "failures_since_success": 0,
        "failure_cap": _max_consecutive_failures(),
        "cap_hit": False,
        "scheduled_children": [],
    }

    return {
        "model": model_dir.name,
        "sections": [
            {"operation": "extract",     "targets": extract_targets},
            {"operation": "map",         "targets": [map_row]},
            {"operation": "materialize", "targets": [mat_row]},
            {"operation": "reconcile",   "targets": [reconcile_row]},
        ],
    }


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
