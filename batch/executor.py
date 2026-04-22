# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Unified extraction executor — the single source of truth for running extractors.

Both the API server (``api/services/extractor_service.py``) and the batch runner
(``batch/runner.py``) delegate here.  Callers supply callbacks so this module
stays free of job-service and audit-log concerns:

* ``progress_cb(msg)`` — called for each progress line (task start/done/warning).
  The API passes ``job_service.update_progress``; the batch runner passes a
  file-writer so the API can read progress from disk while the job is running.
* ``cancel_check()`` — returns True if the job should be cancelled.
  The API passes ``job_service.is_cancelling``; the batch runner passes None
  (batch jobs are not cancellable mid-run).
* ``warning_cb(msg)`` — called when a WARNING is emitted.
  The API passes ``job_service.add_warning``; the batch runner passes None.

Return value: ``ExtractionResult`` dataclass with ``success``, ``records``,
``elapsed``, ``errors``, ``cancelled`` fields.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ExtractionResult:
    success: bool
    records: int = 0
    elapsed: float = 0.0
    errors: list[str] = field(default_factory=list)
    cancelled: bool = False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_extractor(
    model_dir: Path,
    tool_name: str,
    task_name: str | None = None,
    since_override: str | None = None,
    progress_cb: Callable[[str], None] | None = None,
    warning_cb: Callable[[str], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> ExtractionResult:
    """Execute an extractor and return the result.

    This function is called *after* the write lock has been acquired and job
    tracking (audit log / in-memory) has been set up by the caller.

    Args:
        model_dir:      Root directory of the model.
        tool_name:      Task tool name (e.g. ``"jira"``, ``"github"``).
        task_name:      If set, run only the named task within the extractor.
        since_override: ISO date string to override the incremental checkpoint,
                        or ``None`` to use the stored checkpoint.
        progress_cb:    Called with a human-readable progress string for each
                        task start, completion, or per-record message.
        warning_cb:     Called with warning text whenever a task emits a WARNING.
        cancel_check:   Called before each task; if it returns True the run is
                        cancelled and ``ExtractionResult.cancelled`` is set.

    Returns:
        ``ExtractionResult`` — always returns, never raises.
    """
    from core.db import get_backend
    from core.tool_service import get_tool_instance
    from engine.extractor import JobCancelledError, find_extractor, parse_yaml_config_from_content, run_task

    def _progress(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        if warning_cb and msg.startswith("  WARNING"):
            warning_cb(msg[len("  WARNING: "):])

    try:
        content = find_extractor(model_dir, tool_name)
        tasks = parse_yaml_config_from_content(content)
        if task_name:
            tasks = [t for t in tasks if t.name == task_name]

        db = get_backend(model_dir, namespace="raw")
        db.connect()
        db.ensure_meta_table()

        total = 0
        errors: list[str] = []
        n_tasks = len(tasks)
        cancelled = False
        t0 = time.time()

        try:
            for i, task in enumerate(tasks, 1):
                if cancel_check and cancel_check():
                    _progress(f"task {i}/{n_tasks}: cancellation requested")
                    cancelled = True
                    break

                tool = get_tool_instance(task.tool, model_dir)
                _progress(f"task {i}/{n_tasks}: {task.name} — starting")
                t_task = time.time()

                try:
                    count = run_task(
                        task,
                        tool,
                        db,
                        since_override=since_override,
                        progress_cb=_progress,
                        cancel_check=cancel_check,
                    )
                    total += count
                    elapsed_task = round(time.time() - t_task, 1)
                    _progress(f"task {i}/{n_tasks}: {task.name} — done  {count} records  ({elapsed_task}s)")
                    _log.info(
                        "Task complete",
                        extra={
                            "model": model_dir.name,
                            "tool": tool_name,
                            "task": task.name,
                            "records": count,
                            "elapsed_s": elapsed_task,
                        },
                    )
                except JobCancelledError:
                    cancelled = True
                    break
                except Exception as exc:
                    msg = f"{task.name}: {exc}"
                    errors.append(msg)
                    _progress(f"task {i}/{n_tasks}: {task.name} — FAILED: {exc}")
                    _log.error(
                        "Task failed: %s",
                        msg,
                        extra={"model": model_dir.name, "task": task.name},
                        exc_info=True,
                    )
        finally:
            db.close()

        elapsed = round(time.time() - t0, 1)
        return ExtractionResult(
            success=not cancelled and not errors,
            records=total,
            elapsed=elapsed,
            errors=errors,
            cancelled=cancelled,
        )

    except Exception as exc:
        _log.exception(
            "Unhandled error in extraction",
            extra={"model": model_dir.name, "tool": tool_name},
        )
        return ExtractionResult(success=False, errors=[f"Unexpected error: {exc}"])
