# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Background scheduler thread for the batch service.

Runs a loop that periodically checks all registered models for due extractors
and submits jobs to the thread pool executor.

The scheduler is started as a daemon thread in the batch service lifespan.
It reads VZ_MODELS_DIR to discover models and uses ``batch.schedule`` (the same
schedule evaluation logic used by the CLI runner) to determine which extractors
are due.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

_log = logging.getLogger(__name__)

_POLL_INTERVAL = 60  # seconds between schedule checks


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _schedule_tick(models_dir: Path) -> None:
    """One pass: check all models for due extractors, mapper runs, and entity materializations."""
    from batch.schedule import entities_due, extractors_due, mappers_due
    from batch_service import db as jobdb
    from batch_service import executor as jobexec
    from core.registry import load_registry

    try:
        registry = load_registry(models_dir)
    except Exception:
        _log.warning("Could not load model registry from %s", models_dir)
        return

    for model_name in registry:
        model_dir = models_dir / model_name
        if not model_dir.is_dir():
            continue

        # --- Extractors ---
        try:
            due = extractors_due(model_dir)
        except Exception:
            _log.exception("Error checking extractor schedule for model %s", model_name)
            due = []

        if due:
            _log.info("Extractors due for %s: %s", model_name, due)

        for tool in due:
            try:
                with jobdb.get_connection(model_dir) as con:
                    running = [
                        j for j in jobdb.list_jobs(con, model_name, status="running", limit=50)
                        if j.get("tool") == tool
                    ]
                    if running:
                        _log.info(
                            "Skipping %s/%s — already running (job %s)",
                            model_name, tool, running[0]["job_id"],
                        )
                        continue

                    job_id = str(uuid.uuid4())
                    jobdb.insert_job(
                        con,
                        job_id=job_id,
                        model=model_name,
                        operation="extract",
                        tool=tool,
                        status="running",
                        started_at=_now(),
                        triggered_by="schedule",
                    )

                jobexec.submit(model_dir, job_id, tool, None, None)
                _log.info(
                    "Scheduled extractor job submitted",
                    extra={"job_id": job_id, "model": model_name, "tool": tool},
                )
            except Exception:
                _log.exception("Error submitting scheduled extractor job for %s/%s", model_name, tool)

        # --- Mappers ---
        try:
            due_mappers = mappers_due(model_dir)
        except Exception:
            _log.exception("Error checking mapper schedule for model %s", model_name)
            continue

        if not due_mappers:
            continue

        _log.info("Mappers due for %s: %s", model_name, due_mappers)

        try:
            with jobdb.get_connection(model_dir) as con:
                running = [
                    j for j in jobdb.list_jobs(con, model_name, status="running", limit=50)
                    if j.get("operation") == "map"
                ]
                if running:
                    _log.info(
                        "Skipping mapper run for %s — already running (job %s)",
                        model_name, running[0]["job_id"],
                    )
                    continue

                job_id = str(uuid.uuid4())
                jobdb.insert_job(
                    con,
                    job_id=job_id,
                    model=model_name,
                    operation="map",
                    tool="__all__",
                    status="running",
                    started_at=_now(),
                    triggered_by="schedule",
                )

            jobexec.submit_mapper(model_dir, job_id, None)
            _log.info(
                "Scheduled mapper job submitted",
                extra={"job_id": job_id, "model": model_name, "due_mappers": due_mappers},
            )
        except Exception:
            _log.exception("Error submitting scheduled mapper job for %s", model_name)

        # --- Entities (materialize + feature reconcile) ---
        try:
            due_entities = entities_due(model_dir)
        except Exception:
            _log.exception("Error checking entity schedule for model %s", model_name)
            continue

        if not due_entities:
            continue

        _log.info("Entities due for %s: %s", model_name, due_entities)

        try:
            with jobdb.get_connection(model_dir) as con:
                running = [
                    j for j in jobdb.list_jobs(con, model_name, status="running", limit=50)
                    if j.get("operation") == "materialize"
                ]
                if running:
                    _log.info(
                        "Skipping materialize for %s — already running (job %s)",
                        model_name, running[0]["job_id"],
                    )
                    continue

                job_id = str(uuid.uuid4())
                jobdb.insert_job(
                    con,
                    job_id=job_id,
                    model=model_name,
                    operation="materialize",
                    tool="__all__",
                    status="running",
                    started_at=_now(),
                    triggered_by="schedule",
                )

            jobexec.submit_materialize(model_dir, job_id, None)
            _log.info(
                "Scheduled materialize job submitted",
                extra={"job_id": job_id, "model": model_name, "due_entities": due_entities},
            )
        except Exception:
            _log.exception("Error submitting scheduled materialize job for %s", model_name)


def start_scheduler(models_dir: Path) -> threading.Thread:
    """Start the background scheduler daemon thread and return it."""

    def _loop():
        _log.info("Scheduler started, polling every %ds", _POLL_INTERVAL)
        # Wait one full interval before the first tick so that orphan cleanup
        # in the lifespan completes and the service is fully ready.
        time.sleep(_POLL_INTERVAL)
        while True:
            try:
                _schedule_tick(models_dir)
            except Exception:
                _log.exception("Unhandled error in scheduler tick")
            time.sleep(_POLL_INTERVAL)

    t = threading.Thread(target=_loop, name="batch-scheduler", daemon=True)
    t.start()
    return t
