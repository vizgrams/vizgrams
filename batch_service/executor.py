# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Subprocess-per-job executor for the batch service.

Each job (extract / map / materialize / reconcile) runs in its own child
process spawned via ``python -m batch_service.runner <subcmd>``. A
background monitor thread reaps exited children and marks any job whose
subprocess died without self-reporting a terminal status as ``failed``.

Why not the previous ThreadPoolExecutor design: DuckDB 1.5.x has an
internal assertion that, once triggered, invalidates the connection AND
poisons the whole Python process for future DuckDB use. In-thread, one
crash wedged every subsequent job in the batch_service for as long as it
stayed up — we saw 8-day bad windows. Crash-in-child only kills that
child; the next job spawns a fresh interpreter with a clean DuckDB state.

The ``_run_*_job`` functions below are still called from the child (via
``batch_service.runner``); they self-report progress + status to the
batch DB (SQLite, multi-process safe), so no IPC between parent and child
is needed beyond the child's exit code.
"""

from __future__ import annotations

import logging
import os as _os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

_log = logging.getLogger(__name__)

# Max parallel mappers per wave — configurable via MAPPER_WAVE_WORKERS env var.
# Still used inside a child running ``_run_mapper_job`` to run same-wave
# mappers in parallel threads within the one subprocess.
_MAPPER_WAVE_WORKERS = int(_os.environ.get("MAPPER_WAVE_WORKERS", "8"))

# Poll interval for the child-reaper thread. Small enough to notice a
# crashed job within a couple of seconds; large enough not to burn CPU
# checking exit statuses in a tight loop.
_MONITOR_POLL_S = 1.0

# Tracks live child processes so the monitor can reap them and the API
# can hard-kill on cancel. Keyed by job_id; value is (Popen, model_dir).
_children_lock = threading.Lock()
_children: dict[str, tuple[subprocess.Popen, Path]] = {}

_monitor_lock = threading.Lock()
_monitor_started = False


def _ensure_monitor() -> None:
    """Start the child-reaper thread the first time a job is submitted."""
    global _monitor_started
    with _monitor_lock:
        if _monitor_started:
            return
        threading.Thread(
            target=_monitor_children, name="batch-child-reaper", daemon=True,
        ).start()
        _monitor_started = True


def _monitor_children() -> None:
    """Poll for exited children; if a child died without self-reporting a
    terminal status, mark the job ``failed`` with the child's stderr tail
    as the error. Runs forever as a daemon thread.
    """
    from batch_service import db as jobdb
    while True:
        time.sleep(_MONITOR_POLL_S)
        # Snapshot state under the lock; do the actual work outside it so
        # we don't hold the lock during jobdb writes or process signals.
        with _children_lock:
            dead = [
                (jid, proc, mdir)
                for jid, (proc, mdir) in _children.items()
                if proc.poll() is not None
            ]
            for jid, _proc, _mdir in dead:
                _children.pop(jid, None)
            live = [
                (jid, proc, mdir)
                for jid, (proc, mdir) in _children.items()
            ]
        for jid, proc, mdir in dead:
            try:
                _finalize_dead_child(jid, proc, mdir, jobdb)
            except Exception:
                _log.exception("Reaper failed for job %s", jid)
        for jid, proc, mdir in live:
            try:
                _kill_zombie_if_job_terminal(jid, proc, mdir, jobdb)
            except Exception:
                _log.exception("Zombie-check failed for job %s", jid)


def _finalize_dead_child(
    job_id: str, proc: subprocess.Popen, model_dir: Path, jobdb,
) -> None:
    """If the child crashed without updating the job to a terminal status,
    stamp the job as failed with the subprocess's exit code + stderr tail.

    Successful jobs (child returned 0 AND self-reported terminal) are left
    alone. A ``running`` job with rc==0 shouldn't happen — child returning
    0 without a terminal status means the child bypassed our error paths;
    surface it as failed rather than leave it running forever.
    """
    rc = proc.returncode
    stderr_tail = ""
    try:
        stderr = proc.stderr.read() if proc.stderr else b""
        if stderr:
            stderr_tail = stderr.decode(errors="replace")[-2000:]
    except Exception:
        pass

    with jobdb.get_connection(model_dir) as con:
        job = jobdb.get_job(con, job_id)
        if job is None:
            _log.warning("Reaper: job %s not found in batch db", job_id)
            return
        if job["status"] in ("completed", "failed", "cancelled"):
            # Child self-reported before exiting — leave the status alone.
            return
        error = (
            f"Job subprocess exited with code {rc} before setting status. "
            "Likely a hard crash (native assertion, OOM, or signal). "
            f"Stderr tail: {stderr_tail}" if stderr_tail
            else f"Job subprocess exited with code {rc} before setting status."
        )
        jobdb.update_job(
            con, job_id, status="failed",
            completed_at=_now_utc(), error=error[:4000],
        )
    _log.error(
        "Reaper marked orphaned job as failed",
        extra={"job_id": job_id, "model": model_dir.name, "exit_code": rc},
    )


# Grace period between SIGTERM and SIGKILL when tearing down a child. Long
# enough for a graceful shutdown of the ``_run_*_job`` cleanup path, short
# enough that a truly hung process is still killed within a couple of
# reaper cycles.
_TERMINATE_GRACE_S = 5.0


def _kill_child_tree(proc: subprocess.Popen) -> None:
    """SIGTERM the child, wait up to _TERMINATE_GRACE_S, then SIGKILL if
    it's still alive. Idempotent — safe to call on an already-dead proc."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=_TERMINATE_GRACE_S)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except ProcessLookupError:
            pass


def _kill_zombie_if_job_terminal(
    job_id: str, proc: subprocess.Popen, model_dir: Path, jobdb,
) -> None:
    """A "zombie" here is a subprocess that's still running even though
    its job row in the batch db is already in a terminal state — the DB
    orphan sweep flipped ``running`` → ``failed`` after the grace timeout
    while the child was stuck in cleanup (network I/O, DuckDB retries,
    etc.). Left alone, these hold the model write lock indefinitely and
    block every subsequent job. Terminate them.

    Only triggers when the DB has *decided* the job is done. Live
    children matching a still-``running`` job are legitimate work and
    stay untouched.
    """
    try:
        with jobdb.get_connection(model_dir) as con:
            job = jobdb.get_job(con, job_id)
    except Exception:
        return
    if job is None:
        return
    if job["status"] not in ("completed", "failed", "cancelled"):
        return
    _log.warning(
        "Killing zombie child — DB status is %s but PID %s still alive",
        job["status"], proc.pid,
        extra={"job_id": job_id, "model": model_dir.name, "pid": proc.pid},
    )
    _kill_child_tree(proc)
    # Removing from _children isn't necessary — next reaper pass will
    # see poll() != None and pop it — but do it eagerly so a rapid
    # follow-up submit doesn't race with the stale entry.
    with _children_lock:
        _children.pop(job_id, None)


def terminate_all_tracked_children() -> None:
    """SIGTERM every tracked child, escalate to SIGKILL after grace.

    Called from the batch_service FastAPI lifespan shutdown so a clean
    ``uvicorn`` shutdown / ``make dev`` restart nukes children instead of
    leaving them reparented to init still holding DB locks. Doesn't help
    if the parent gets SIGKILL'd — that's what
    :func:`sweep_and_kill_orphaned_runners` handles on the next startup.
    """
    with _children_lock:
        procs = [proc for proc, _mdir in _children.values()]
        _children.clear()
    for proc in procs:
        try:
            _kill_child_tree(proc)
        except Exception:
            _log.exception("Failed to terminate child PID %s", proc.pid)


def sweep_and_kill_orphaned_runners() -> None:
    """Kill any leftover ``batch_service.runner`` processes whose parent
    is now ``init`` (PPID=1). Called on batch_service startup.

    Orphans come from previous parent lifetimes that ended without
    graceful shutdown — SIGKILL, hard crash, ``make dev`` respawn without
    lifespan cleanup. Their DuckDB write lock blocks every new job we try
    to spawn until they exit on their own (they might not, ever). Killing
    them before the scheduler kicks off is the only reliable recovery.

    Uses ``ps`` rather than a Python-side library so we don't add a psutil
    dep. Matches on the exact module path we spawn with (``-m
    batch_service.runner``) to avoid killing unrelated Python processes.
    """
    import subprocess as _sp
    try:
        result = _sp.run(
            ["ps", "-e", "-o", "pid,ppid,command"],
            capture_output=True, text=True, timeout=5.0,
        )
    except Exception:
        _log.exception("Orphan sweep: could not run 'ps'")
        return
    killed = []
    self_pid = _os.getpid()
    for line in result.stdout.splitlines()[1:]:  # skip header
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        cmd = parts[2]
        if "batch_service.runner" not in cmd:
            continue
        if pid == self_pid:
            continue  # can't happen — self is uvicorn, not runner
        # PPID=1 means init/launchd has taken over; a live batch_service
        # parent would still own its children. Only PPID=1 processes are
        # unambiguously orphans.
        if ppid != 1:
            continue
        try:
            _os.kill(pid, 15)  # SIGTERM
            killed.append(pid)
        except ProcessLookupError:
            pass
        except Exception:
            _log.exception("Failed to SIGTERM orphan PID %s", pid)
    if killed:
        _log.warning(
            "Killed %d orphaned batch_service.runner process(es) from a "
            "previous parent lifetime: %s", len(killed), killed,
        )


def _spawn(
    subcmd: str, model_dir: Path, job_id: str, extra_args: list[str],
) -> subprocess.Popen:
    """Fork off ``python -m batch_service.runner <subcmd>`` for one job.

    The child inherits the parent's environment (poetry venv, VZ_MODELS_DIR,
    etc.); stderr is piped so the reaper can grab a tail on non-zero exits.
    stdout is discarded — the child writes progress via the batch DB.
    """
    args = [
        sys.executable, "-m", "batch_service.runner", subcmd,
        "--model-dir", str(model_dir),
        "--job-id", job_id,
        *extra_args,
    ]
    proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=_os.environ.copy(),
    )
    with _children_lock:
        _children[job_id] = (proc, model_dir)
    _ensure_monitor()
    _log.info(
        "Spawned job subprocess",
        extra={"job_id": job_id, "model": model_dir.name,
               "subcmd": subcmd, "pid": proc.pid},
    )
    return proc


def submit(
    model_dir: Path,
    job_id: str,
    tool: str,
    task: str | None,
    since_override: str | None,
) -> None:
    """Fire off an extract job in a subprocess. Returns immediately; the
    caller polls the batch DB for status."""
    extra = ["--tool", tool]
    if task is not None:
        extra += ["--task", task]
    if since_override is not None:
        extra += ["--since", since_override]
    _spawn("extract", model_dir, job_id, extra)


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
    """Fire off a mapper job in a subprocess. Returns immediately."""
    extra = []
    if mapper_name is not None:
        extra += ["--mapper", mapper_name]
    _spawn("map", model_dir, job_id, extra)


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

        # Pre-execution gate: exactly one mapper per target entity. Multiple
        # mappers stomping the same entity is the SCD2 oscillation bug — fail
        # fast with a clear error rather than silently corrupting data.
        _by_entity: dict[str, list[str]] = {}
        for mc in all_mappers:
            for tgt in mc.targets:
                _by_entity.setdefault(tgt.entity_name, []).append(mc.name)
        _dupes = {e: ms for e, ms in _by_entity.items() if len(ms) > 1}
        if _dupes:
            lines = [f"  {e}: {ms}" for e, ms in _dupes.items()]
            raise RuntimeError(
                "Mapper run aborted — entities are targeted by more than one "
                "mapper, which causes non-deterministic SCD2 writes:\n"
                + "\n".join(lines)
                + "\nRefactor the duplicates into a single mapper or remove one."
            )

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
    """Fire off a materialize job in a subprocess. Returns immediately."""
    extra = []
    if entity_name is not None:
        extra += ["--entity", entity_name]
    _spawn("materialize", model_dir, job_id, extra)


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


def submit_reconcile(
    model_dir: Path,
    job_id: str,
    entity_name: str | None,
    feature_id: str | None,
) -> None:
    """Fire off a feature-reconcile job in a subprocess. Returns immediately."""
    extra = []
    if entity_name is not None:
        extra += ["--entity", entity_name]
    if feature_id is not None:
        extra += ["--feature-id", feature_id]
    _spawn("reconcile", model_dir, job_id, extra)


def _run_reconcile_job(
    model_dir: Path,
    job_id: str,
    entity_name: str | None,
    feature_id: str | None,
) -> None:
    """Validate + reconcile feature definitions in the sem backend.

    Scope:
      - feature_id set → reconcile only that feature
      - entity_name set, feature_id None → reconcile all features for the entity
      - both None → reconcile everything

    Uses the same model_write_lock as materialize so writes from api +
    batch can't collide on single-writer backends (DuckDB).
    """
    from batch_service import db as jobdb

    _log.info(
        "Reconcile job starting",
        extra={
            "job_id": job_id, "model": model_dir.name,
            "entity": entity_name or "all", "feature_id": feature_id or "*",
        },
    )

    def _progress(msg: str) -> None:
        try:
            with jobdb.get_connection(model_dir) as con:
                jobdb.append_progress(con, job_id, _now_utc(), msg)
        except Exception:
            _log.debug("Progress write failed for job %s", job_id, exc_info=True)

    t0 = time.time()
    n_reconciled = 0

    try:
        from batch.lock import LockTimeoutError, model_write_lock
        with model_write_lock(model_dir):
            from core.db import get_backend
            from semantic.feature import reconcile_with_backend
            from semantic.yaml_adapter import YAMLAdapter

            all_entities = YAMLAdapter.load_entities(model_dir / "ontology")
            entities_map = {e.name: e for e in all_entities}
            all_feature_defs = YAMLAdapter.load_features(model_dir / "features")
            if not all_feature_defs:
                _progress("no features defined — nothing to reconcile")
            else:
                # Build materialize_ids filter from scope.
                if feature_id is not None:
                    materialize_ids: set[str] | None = {feature_id}
                elif entity_name is not None:
                    materialize_ids = {
                        fd.feature_id for fd in all_feature_defs
                        if fd.entity_type == entity_name
                    }
                else:
                    materialize_ids = None

                n_reconciled = (
                    len(materialize_ids) if materialize_ids is not None
                    else len(all_feature_defs)
                )
                _progress(f"reconciling {n_reconciled} feature(s)")
                backend = get_backend(model_dir, namespace="sem")
                backend.connect()
                try:
                    reconcile_with_backend(
                        all_feature_defs, entities_map, backend,
                        materialize_ids=materialize_ids,
                    )
                    _progress("features reconciled")
                finally:
                    backend.close()

    except LockTimeoutError as exc:
        _log.error("Write lock timeout for reconcile job %s: %s", job_id, exc,
                   extra={"job_id": job_id, "model": model_dir.name})
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return
    except Exception as exc:
        _log.exception("Unexpected error in reconcile job %s", job_id)
        with jobdb.get_connection(model_dir) as con:
            jobdb.update_job(con, job_id, status="failed",
                             completed_at=_now_utc(), error=str(exc))
        return

    elapsed = round(time.time() - t0, 1)
    with jobdb.get_connection(model_dir) as con:
        jobdb.update_job(con, job_id, status="completed", completed_at=_now_utc(),
                         records=n_reconciled, duration_s=elapsed)
    _log.info(
        "Reconcile job completed",
        extra={"job_id": job_id, "model": model_dir.name,
               "features": n_reconciled, "duration_s": elapsed},
    )
