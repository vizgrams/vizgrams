# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Inline thread-pool executor for the batch service (Phase 1).

Submits extraction jobs to a bounded thread pool. Each running job writes
progress to the model's ``vizgrams-batch.db`` so callers can poll for
live updates via ``GET /api/v1/jobs/{job_id}``.

The execution mechanism is an internal implementation detail of the batch
service. Future phases may swap this for Celery + Redis or k8s Job manifests
without changing the HTTP contract.
"""

from __future__ import annotations

import logging
import os as _os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

_log = logging.getLogger(__name__)

# Bounded thread pool — configurable via BATCH_WORKERS env var
_pool = ThreadPoolExecutor(max_workers=int(_os.environ.get("BATCH_WORKERS", "4")))

# Max parallel mappers per wave — configurable via MAPPER_WAVE_WORKERS env var
_MAPPER_WAVE_WORKERS = int(_os.environ.get("MAPPER_WAVE_WORKERS", "8"))


def submit(
    model_dir: Path,
    job_id: str,
    tool: str,
    task: str | None,
    since_override: str | None,
) -> None:
    """Schedule a job on the thread pool and return immediately."""
    _pool.submit(_run_job, model_dir, job_id, tool, task, since_override)


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_job(
    model_dir: Path,
    job_id: str,
    tool: str,
    task: str | None,
    since_override: str | None,
) -> None:
    """Execute the extraction and update job status in the DB."""
    from batch.executor import run_extractor
    from batch_service import db as jobdb

    _log.info(
        "Job starting",
        extra={"job_id": job_id, "model": model_dir.name, "tool": tool},
    )

    def _progress(msg: str) -> None:
        try:
            with jobdb.get_connection(model_dir) as con:
                jobdb.append_progress(con, job_id, _now_utc(), msg)
        except Exception:
            _log.debug("Progress write failed for job %s", job_id, exc_info=True)

    def _cancel_check() -> bool:
        try:
            with jobdb.get_connection(model_dir) as con:
                job = jobdb.get_job(con, job_id)
                return job is not None and job["status"] == "cancelling"
        except Exception:
            return False

    try:
        result = run_extractor(
            model_dir,
            tool,
            task_name=task,
            since_override=since_override,
            progress_cb=_progress,
            cancel_check=_cancel_check,
        )
    except Exception as exc:
        _log.exception("Unexpected error in job %s", job_id)
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return

    now = _now_utc()
    with jobdb.get_connection(model_dir) as con:
        if result.cancelled:
            jobdb.update_job(con, job_id, status="cancelled", completed_at=now)
            _log.info("Job cancelled", extra={"job_id": job_id, "model": model_dir.name})
        elif result.errors:
            error_str = "; ".join(result.errors)
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=now, error=error_str)
            _log.error(
                "Job failed",
                extra={"job_id": job_id, "model": model_dir.name, "error": error_str},
            )
        else:
            jobdb.update_job(con, job_id, status="completed", completed_at=now,
                             records=result.records, duration_s=result.elapsed)
            _log.info(
                "Job completed",
                extra={
                    "job_id": job_id,
                    "model": model_dir.name,
                    "records": result.records,
                    "duration_s": result.elapsed,
                },
            )


# ---------------------------------------------------------------------------
# Mapper jobs
# ---------------------------------------------------------------------------


def submit_mapper(model_dir: Path, job_id: str, mapper_name: str | None) -> None:
    """Schedule a mapper job on the thread pool and return immediately."""
    _pool.submit(_run_mapper_job, model_dir, job_id, mapper_name)


def _run_mapper_job(model_dir: Path, job_id: str, mapper_name: str | None) -> None:
    """Execute mappers and update job status in the DB.

    When running all mappers, uses wave-based parallel execution: mappers that
    share no dependency path run concurrently within the same wave.  Each mapper
    gets its own backend connections so ClickHouse handles concurrent writes
    on independent sem_ tables without contention.
    """
    from batch_service import db as jobdb

    _log.info(
        "Mapper job starting",
        extra={"job_id": job_id, "model": model_dir.name, "mapper": mapper_name or "all"},
    )

    def _progress(msg: str) -> None:
        try:
            with jobdb.get_connection(model_dir) as con:
                jobdb.append_progress(con, job_id, _now_utc(), msg)
        except Exception:
            _log.debug("Progress write failed for job %s", job_id, exc_info=True)

    t0 = time.time()
    total_rows = 0

    try:
        from core.db import get_backend
        from engine.mapper import build_execution_waves, run_mapper
        from semantic.yaml_adapter import YAMLAdapter

        ontology_entities = YAMLAdapter.load_entities(model_dir / "ontology")
        all_mappers = YAMLAdapter.load_mappers(model_dir / "mappers")

        if mapper_name:
            target = next((mc for mc in all_mappers if mc.name == mapper_name), None)
            if target is None:
                raise KeyError(f"Mapper '{mapper_name}' not found in model '{model_dir.name}'")
            waves = [[target]]
        else:
            waves = build_execution_waves(all_mappers)

        n_waves = len(waves)

        def _run_one(mc):
            backend = get_backend(model_dir, namespace="sem")
            source_backend = get_backend(model_dir, namespace="raw")
            backend.connect()
            source_backend.connect()
            try:
                result = run_mapper(mc, ontology_entities, backend, source_backend=source_backend)
                return result.total_grain_rows
            finally:
                backend.close()
                source_backend.close()

        for wave_idx, wave in enumerate(waves, 1):
            if len(wave) == 1:
                mc = wave[0]
                _progress(f"mapper {mc.name} — starting")
                rows = _run_one(mc)
                total_rows += rows
                _progress(f"mapper {mc.name} — done  {rows} rows")
                _log.info("Mapper complete", extra={
                    "job_id": job_id, "model": model_dir.name,
                    "mapper": mc.name, "rows": rows,
                })
            else:
                names = ", ".join(mc.name for mc in wave)
                _progress(f"wave {wave_idx}/{n_waves}: {names} — starting ({len(wave)} parallel)")
                wave_rows = 0
                workers = min(len(wave), _MAPPER_WAVE_WORKERS)
                with ThreadPoolExecutor(max_workers=workers) as wave_pool:
                    futures = {wave_pool.submit(_run_one, mc): mc for mc in wave}
                    for fut in as_completed(futures):
                        mc = futures[fut]
                        rows = fut.result()  # propagates exception → fails the job
                        wave_rows += rows
                        _progress(f"mapper {mc.name} — done  {rows} rows")
                        _log.info("Mapper complete", extra={
                            "job_id": job_id, "model": model_dir.name,
                            "mapper": mc.name, "rows": rows,
                        })
                total_rows += wave_rows
                _progress(f"wave {wave_idx}/{n_waves}: done  {wave_rows} rows")

    except Exception as exc:
        _log.exception("Unexpected error in mapper job %s", job_id)
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return

    elapsed = round(time.time() - t0, 1)
    with jobdb.get_connection(model_dir) as con:
        jobdb.update_job(con, job_id, status="completed", completed_at=_now_utc(),
                         records=total_rows, duration_s=elapsed)
    _log.info(
        "Mapper job completed",
        extra={"job_id": job_id, "model": model_dir.name,
               "rows": total_rows, "duration_s": elapsed},
    )


# ---------------------------------------------------------------------------
# Materialize jobs
# ---------------------------------------------------------------------------


def submit_materialize(model_dir: Path, job_id: str, entity_name: str | None) -> None:
    """Schedule a materialize job on the thread pool and return immediately."""
    _pool.submit(_run_materialize_job, model_dir, job_id, entity_name)


def _run_materialize_job(model_dir: Path, job_id: str, entity_name: str | None) -> None:
    """Create or update entity tables in the sem backend and update job status in the DB."""
    from batch_service import db as jobdb

    _log.info(
        "Materialize job starting",
        extra={"job_id": job_id, "model": model_dir.name, "entity": entity_name or "all"},
    )

    def _progress(msg: str) -> None:
        try:
            with jobdb.get_connection(model_dir) as con:
                jobdb.append_progress(con, job_id, _now_utc(), msg)
        except Exception:
            _log.debug("Progress write failed for job %s", job_id, exc_info=True)

    t0 = time.time()
    tables: list[str] = []

    try:
        from batch.lock import LockTimeoutError, model_write_lock
        with model_write_lock(model_dir):
            from core.db import get_backend
            from semantic.materialize import materialize_with_backend
            from semantic.yaml_adapter import YAMLAdapter

            all_entities = YAMLAdapter.load_entities(model_dir / "ontology")
            if entity_name:
                targets = [e for e in all_entities if e.name == entity_name]
                if not targets:
                    raise KeyError(f"Entity '{entity_name}' not found in model '{model_dir.name}'")
            else:
                targets = all_entities

            _progress(f"materializing {len(targets)} entit{'y' if len(targets) == 1 else 'ies'}")
            backend = get_backend(model_dir, namespace="sem")
            backend.connect()
            try:
                tables = materialize_with_backend(targets, backend)
                _progress(f"done — tables: {', '.join(tables)}")

                # Reconcile features into the same backend after entity tables are ready
                from semantic.feature import reconcile_with_backend
                all_entities_map = {e.name: e for e in all_entities}
                feature_defs = YAMLAdapter.load_features(model_dir / "features")
                if feature_defs:
                    _progress(f"reconciling {len(feature_defs)} feature(s)")
                    reconcile_with_backend(feature_defs, all_entities_map, backend)
                    _progress("features reconciled")
            finally:
                backend.close()

    except LockTimeoutError as exc:
        _log.error("Write lock timeout for materialize job %s: %s", job_id, exc,
                   extra={"job_id": job_id, "model": model_dir.name})
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return
    except Exception as exc:
        _log.exception("Unexpected error in materialize job %s", job_id)
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return

    elapsed = round(time.time() - t0, 1)
    completed_at = _now_utc()
    with jobdb.get_connection(model_dir) as con:
        jobdb.update_job(con, job_id, status="completed", completed_at=completed_at,
                         records=len(tables), duration_s=elapsed)
    _log.info(
        "Materialize job completed",
        extra={"job_id": job_id, "model": model_dir.name,
               "tables": len(tables), "duration_s": elapsed},
    )

    # Write a pipeline_checkpoint entry to the audit log so external backup
    # tooling can identify a consistent point-in-time across both stores.
    try:
        from core.registry import append_audit
        append_audit(
            model_dir,
            event="pipeline_checkpoint",
            detail={
                "consistent_at": completed_at,
                "clickhouse_databases": [model_dir.name, f"{model_dir.name}_raw"],
                "sqlite_files": [
                    "data/api.db",
                    "data/batch.db",
                ],
            },
            actor="schedule",
        )
    except Exception:
        _log.debug("pipeline_checkpoint audit skipped (registry unavailable)")
