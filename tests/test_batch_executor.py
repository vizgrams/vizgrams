# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``batch_service/executor.py`` — subprocess spawn + child reaper.

Two layers:

1. ``_spawn`` and each ``submit_*`` — verify they build the right argv and
   register the child in ``_children``. subprocess.Popen is mocked so
   nothing actually runs.

2. ``_finalize_dead_child`` — verify the reaper's decision logic against
   a real (per-test) SQLite batch db. The autouse ``_isolate_batch_db``
   fixture in conftest.py routes ``BATCH_DB_PATH`` to tmp_path so we can
   drive real ``jobdb`` writes/reads without cross-test leakage.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from batch_service import db as jobdb
from batch_service import executor

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_executor_state():
    """Clear the module-level child registry + monitor flag between tests
    so each test starts with a clean slate. The autouse fixture guarantees
    we don't leak child records or leave the lazy monitor thread flag set
    across tests."""
    executor._children.clear()
    executor._monitor_started = False
    yield
    executor._children.clear()
    executor._monitor_started = False


def _insert_running_job(job_id: str, model: str = "m") -> None:
    """Seed a ``running`` job in the batch db so ``_finalize_dead_child``
    has something to look at."""
    with jobdb.get_connection() as con:
        jobdb.insert_job(
            con,
            job_id=job_id, model=model, operation="map",
            tool=None, status="running", started_at="2026-01-01T00:00:00Z",
            triggered_by="test",
        )


def _get_status(job_id: str) -> tuple[str, str | None]:
    with jobdb.get_connection() as con:
        job = jobdb.get_job(con, job_id)
    return job["status"], job.get("error")


def _fake_proc(*, exit_code: int | None = 0, stderr: bytes | None = b"") -> MagicMock:
    """A ``subprocess.Popen``-shaped mock. ``exit_code=None`` means still
    running; the reaper skips those."""
    proc = MagicMock()
    proc.poll.return_value = exit_code
    proc.returncode = exit_code
    proc.stderr = MagicMock()
    proc.stderr.read.return_value = stderr
    proc.pid = 999
    return proc


# ---------------------------------------------------------------------------
# _spawn / submit_* — argv construction + child registration
# ---------------------------------------------------------------------------


def test_spawn_builds_argv_with_model_dir_and_job_id(tmp_path):
    """Spawn always includes ``sys.executable -m batch_service.runner
    <subcmd> --model-dir ... --job-id ...`` — the two args every runner
    subcommand requires."""
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor._spawn("map", tmp_path, "job-1", extra_args=[])
    argv = popen.call_args.args[0]
    assert argv[1] == "-m"
    assert argv[2] == "batch_service.runner"
    assert argv[3] == "map"
    assert "--model-dir" in argv and str(tmp_path) in argv
    assert "--job-id" in argv and "job-1" in argv


def test_spawn_registers_child_for_reaper(tmp_path):
    """The spawned Popen + model_dir must land in ``_children`` keyed by
    job_id so the monitor can find + finalize it after exit."""
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor._spawn("map", tmp_path, "job-2", extra_args=[])
    assert "job-2" in executor._children
    proc, mdir = executor._children["job-2"]
    assert mdir == tmp_path


def test_spawn_starts_monitor_lazily_and_only_once(tmp_path):
    """The reaper thread must be started once on first spawn and never
    respawned — otherwise we'd leak threads on every job. Verify the
    ``_monitor_started`` sentinel gates a repeat start."""
    calls = []

    def fake_thread(*_, target=None, **__):
        calls.append(target)
        t = MagicMock()
        t.start = MagicMock()
        return t

    with patch("batch_service.executor.subprocess.Popen") as popen, \
         patch("batch_service.executor.threading.Thread", side_effect=fake_thread):
        popen.return_value = _fake_proc(exit_code=None)
        executor._spawn("map", tmp_path, "j1", extra_args=[])
        executor._spawn("map", tmp_path, "j2", extra_args=[])
        executor._spawn("materialize", tmp_path, "j3", extra_args=[])
    # Only one Thread(target=_monitor_children) construction total,
    # regardless of how many spawns happen.
    assert len(calls) == 1
    assert calls[0] is executor._monitor_children


def test_submit_extract_forwards_tool_task_since(tmp_path):
    """``submit`` (extract) should pass --tool always, --task/--since only
    when set. Guards a common regression: an empty-string ``task`` being
    forwarded as ``--task ''`` and confusing the runner's argparse."""
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit(tmp_path, "j", tool="git", task="commits", since_override="2026-01-01")
    argv = popen.call_args.args[0]
    assert "--tool" in argv and "git" in argv
    assert "--task" in argv and "commits" in argv
    assert "--since" in argv and "2026-01-01" in argv


def test_submit_extract_omits_optional_none_args(tmp_path):
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit(tmp_path, "j", tool="git", task=None, since_override=None)
    argv = popen.call_args.args[0]
    assert "--task" not in argv
    assert "--since" not in argv


def test_submit_mapper_omits_mapper_when_none(tmp_path):
    """None mapper name means "run all" — must not forward --mapper at
    all, otherwise the runner sees ``--mapper None`` and searches for a
    mapper literally named 'None'."""
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit_mapper(tmp_path, "j", mapper_name=None)
    assert "--mapper" not in popen.call_args.args[0]


def test_submit_mapper_forwards_named_mapper(tmp_path):
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit_mapper(tmp_path, "j", mapper_name="team")
    argv = popen.call_args.args[0]
    assert "--mapper" in argv and "team" in argv


def test_submit_materialize_forwards_entity(tmp_path):
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit_materialize(tmp_path, "j", entity_name="Team")
    argv = popen.call_args.args[0]
    assert "--entity" in argv and "Team" in argv


def test_submit_reconcile_forwards_both_scope_args(tmp_path):
    with patch("batch_service.executor.subprocess.Popen") as popen:
        popen.return_value = _fake_proc(exit_code=None)
        executor.submit_reconcile(tmp_path, "j", entity_name="Repo", feature_id="repo.count")
    argv = popen.call_args.args[0]
    assert "--entity" in argv and "Repo" in argv
    assert "--feature-id" in argv and "repo.count" in argv


# ---------------------------------------------------------------------------
# _finalize_dead_child — reaper decision logic
# ---------------------------------------------------------------------------


def test_reaper_marks_running_job_failed_with_exit_code(tmp_path):
    """The core containment property: a child that exits non-zero without
    updating status must have its job stamped ``failed`` with the exit
    code surfaced in the error. Otherwise the job stays ``running`` in the
    DB indefinitely and users can't tell it crashed."""
    _insert_running_job("orphan-1")
    proc = _fake_proc(exit_code=-9, stderr=b"boom in the extractor\n")
    executor._finalize_dead_child("orphan-1", proc, tmp_path, jobdb)
    status, error = _get_status("orphan-1")
    assert status == "failed"
    assert "-9" in error
    assert "boom in the extractor" in error


def test_reaper_leaves_completed_job_alone(tmp_path):
    """Child that self-reported ``completed`` and then exited 0 — the
    reaper must NOT flip it back to failed. Guards a regression where
    the reaper blindly overwrites terminal states."""
    _insert_running_job("done-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "done-1", status="completed",
                         completed_at="2026-01-01T00:01:00Z")
    proc = _fake_proc(exit_code=0, stderr=b"")
    executor._finalize_dead_child("done-1", proc, tmp_path, jobdb)
    status, _ = _get_status("done-1")
    assert status == "completed"


def test_reaper_leaves_failed_job_alone(tmp_path):
    """Same guard for ``failed``: the child already stamped it, don't
    clobber the original error with the reaper's generic message."""
    _insert_running_job("badjob-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "badjob-1", status="failed",
                         completed_at="2026-01-01T00:01:00Z",
                         error="the real error")
    proc = _fake_proc(exit_code=1, stderr=b"noise")
    executor._finalize_dead_child("badjob-1", proc, tmp_path, jobdb)
    _, error = _get_status("badjob-1")
    assert error == "the real error"


def test_reaper_leaves_cancelled_job_alone(tmp_path):
    _insert_running_job("stop-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "stop-1", status="cancelled",
                         completed_at="2026-01-01T00:01:00Z")
    proc = _fake_proc(exit_code=-15, stderr=b"")
    executor._finalize_dead_child("stop-1", proc, tmp_path, jobdb)
    status, _ = _get_status("stop-1")
    assert status == "cancelled"


def test_reaper_handles_missing_job_gracefully(tmp_path):
    """Job may have been deleted from the DB (e.g. TTL clean-up) between
    spawn and reap. Must not crash; nothing to update."""
    proc = _fake_proc(exit_code=-9, stderr=b"")
    # Should not raise even though the job row doesn't exist.
    executor._finalize_dead_child("ghost-1", proc, tmp_path, jobdb)


def test_reaper_marks_zero_exit_orphan_as_failed(tmp_path):
    """A child that returned exit code 0 but didn't self-report a
    terminal state is a bug — it bypassed the ``_run_*_job`` error paths.
    Surface it as failed rather than leave it running forever."""
    _insert_running_job("silent-1")
    proc = _fake_proc(exit_code=0, stderr=b"")
    executor._finalize_dead_child("silent-1", proc, tmp_path, jobdb)
    status, _ = _get_status("silent-1")
    assert status == "failed"


def test_reaper_truncates_very_long_stderr(tmp_path):
    """Stderr can be arbitrary size. The error column is bounded; the
    reaper must cap the stored string so a runaway log doesn't blow up
    the batch db row."""
    _insert_running_job("verbose-1")
    huge_stderr = ("x" * 10_000).encode()
    proc = _fake_proc(exit_code=-11, stderr=huge_stderr)
    executor._finalize_dead_child("verbose-1", proc, tmp_path, jobdb)
    _, error = _get_status("verbose-1")
    assert len(error) <= 4000


def test_reaper_survives_stderr_read_failure(tmp_path):
    """If reading stderr raises (e.g. the pipe was already closed by
    something else), the reaper must still mark the job failed — the
    exit code is what matters for containment, not the stderr tail."""
    _insert_running_job("pipe-broken-1")
    proc = MagicMock()
    proc.poll.return_value = -9
    proc.returncode = -9
    proc.stderr = MagicMock()
    proc.stderr.read.side_effect = OSError("pipe closed")
    proc.pid = 999
    executor._finalize_dead_child("pipe-broken-1", proc, tmp_path, jobdb)
    status, error = _get_status("pipe-broken-1")
    assert status == "failed"
    assert "-9" in error


# ---------------------------------------------------------------------------
# _kill_zombie_if_job_terminal — running-child-with-terminal-DB-status case
# ---------------------------------------------------------------------------


def test_zombie_check_leaves_running_job_child_alone(tmp_path):
    """Live child + ``running`` DB status is the normal case — legitimate
    work in progress. Must NOT kill it. Guards against a regression where
    the zombie sweep gets over-eager and murders live jobs."""
    _insert_running_job("live-1")
    proc = _fake_proc(exit_code=None)  # still running
    executor._kill_zombie_if_job_terminal("live-1", proc, tmp_path, jobdb)
    proc.terminate.assert_not_called()
    proc.kill.assert_not_called()


def test_zombie_check_kills_running_child_with_failed_job(tmp_path):
    """The reason this whole helper exists: DB status went ``failed`` via
    the orphan sweep but the child is still alive holding a DuckDB lock.
    Must SIGTERM the child so the lock releases."""
    _insert_running_job("hung-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "hung-1", status="failed",
                         completed_at="2026-01-01T00:01:00Z",
                         error="orphan-sweep marked me failed")
    proc = _fake_proc(exit_code=None)
    executor._kill_zombie_if_job_terminal("hung-1", proc, tmp_path, jobdb)
    proc.terminate.assert_called_once()


def test_zombie_check_kills_running_child_with_completed_job(tmp_path):
    """Same guard for ``completed``: a child that self-reported completed
    but somehow kept running (bug in _run_*_job cleanup path) still needs
    killing so it stops holding resources."""
    _insert_running_job("stuck-cleanup-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "stuck-cleanup-1", status="completed",
                         completed_at="2026-01-01T00:01:00Z")
    proc = _fake_proc(exit_code=None)
    executor._kill_zombie_if_job_terminal("stuck-cleanup-1", proc, tmp_path, jobdb)
    proc.terminate.assert_called_once()


def test_zombie_check_kills_running_child_with_cancelled_job(tmp_path):
    """Cancel is a common trigger for this — user hits cancel via API,
    DB flips to ``cancelled``, but if the child ignores the cancel_check
    and keeps running, terminate it."""
    _insert_running_job("cancelme-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "cancelme-1", status="cancelled",
                         completed_at="2026-01-01T00:01:00Z")
    proc = _fake_proc(exit_code=None)
    executor._kill_zombie_if_job_terminal("cancelme-1", proc, tmp_path, jobdb)
    proc.terminate.assert_called_once()


def test_zombie_check_ignores_missing_job(tmp_path):
    """Job row deleted from DB between spawn and zombie check. No status
    to compare against; must not raise or kill the child unnecessarily."""
    proc = _fake_proc(exit_code=None)
    executor._kill_zombie_if_job_terminal("gone-1", proc, tmp_path, jobdb)
    proc.terminate.assert_not_called()


def test_zombie_kill_pops_child_from_registry(tmp_path):
    """After a zombie kill, the entry should be removed from _children
    so a next-tick reaper pass doesn't try to re-finalize it after the
    forced SIGKILL. Regression guard for a race where SIGKILL exit code
    -9 confuses ``_finalize_dead_child`` into re-writing the error."""
    _insert_running_job("zombie-cleanup-1")
    with jobdb.get_connection() as con:
        jobdb.update_job(con, "zombie-cleanup-1", status="failed",
                         completed_at="2026-01-01T00:01:00Z",
                         error="pre-existing")
    proc = _fake_proc(exit_code=None)
    executor._children["zombie-cleanup-1"] = (proc, tmp_path)
    executor._kill_zombie_if_job_terminal("zombie-cleanup-1", proc, tmp_path, jobdb)
    assert "zombie-cleanup-1" not in executor._children


# ---------------------------------------------------------------------------
# terminate_all_tracked_children — shutdown hook
# ---------------------------------------------------------------------------


def test_terminate_all_tracked_children_signals_every_child(tmp_path):
    """Shutdown handler: SIGTERM every entry in _children. If we miss any,
    ``make dev`` restart leaves DuckDB locks held by orphaned children."""
    procs = [_fake_proc(exit_code=None) for _ in range(3)]
    for i, p in enumerate(procs):
        executor._children[f"job-{i}"] = (p, tmp_path)
    executor.terminate_all_tracked_children()
    for p in procs:
        p.terminate.assert_called_once()
    assert executor._children == {}


def test_terminate_all_tracked_children_skips_already_dead(tmp_path):
    """A child that exited on its own between our snapshot and the
    signal call — SIGTERM would raise ProcessLookupError. Must swallow
    it (the process is already gone; nothing to do)."""
    proc = _fake_proc(exit_code=0)  # already exited
    executor._children["done-already"] = (proc, tmp_path)
    executor.terminate_all_tracked_children()
    # No signal sent — _kill_child_tree returns early on poll() != None.
    proc.terminate.assert_not_called()
    assert executor._children == {}


def test_terminate_all_tracked_children_survives_signal_error(tmp_path):
    """One child failing to terminate must not stop us from trying the
    rest. Otherwise a single kernel oddity leaves everything else running."""
    p_good = _fake_proc(exit_code=None)
    p_bad = _fake_proc(exit_code=None)
    p_bad.terminate.side_effect = ProcessLookupError
    executor._children["a"] = (p_good, tmp_path)
    executor._children["b"] = (p_bad, tmp_path)
    executor._children["c"] = (_fake_proc(exit_code=None), tmp_path)
    executor.terminate_all_tracked_children()
    p_good.terminate.assert_called_once()
    # The registry is fully cleared regardless of per-child errors.
    assert executor._children == {}


# ---------------------------------------------------------------------------
# sweep_and_kill_orphaned_runners — startup hook
# ---------------------------------------------------------------------------


def test_sweep_kills_orphaned_runner_processes():
    """After a hard parent exit, the child is reparented to PPID=1. On
    the next startup we must SIGTERM any such process — otherwise its
    DuckDB write lock blocks every new job."""
    ps_output = (
        "  PID  PPID COMMAND\n"
        "12345    1 python -m batch_service.runner extract --model-dir /m --job-id j1\n"
    )
    fake_run = MagicMock()
    fake_run.stdout = ps_output
    with patch("batch_service.executor.subprocess.run", return_value=fake_run), \
         patch("batch_service.executor._os.kill") as kill:
        executor.sweep_and_kill_orphaned_runners()
    kill.assert_called_once_with(12345, 15)


def test_sweep_skips_live_children_owned_by_this_parent():
    """A ``batch_service.runner`` whose PPID isn't 1 has a live parent
    that owns it — not our problem to kill. Guards against nuking a
    sibling batch_service's children in a multi-instance dev setup."""
    self_pid = 99999
    ps_output = (
        "  PID  PPID COMMAND\n"
        f"12345 {self_pid} python -m batch_service.runner extract --model-dir /m --job-id j1\n"
    )
    fake_run = MagicMock()
    fake_run.stdout = ps_output
    with patch("batch_service.executor.subprocess.run", return_value=fake_run), \
         patch("batch_service.executor._os.kill") as kill:
        executor.sweep_and_kill_orphaned_runners()
    kill.assert_not_called()


def test_sweep_ignores_unrelated_processes():
    """Only kill processes with our exact module signature. Nginx,
    postgres, other Python programs — all off-limits."""
    ps_output = (
        "  PID  PPID COMMAND\n"
        "12345    1 python -m unrelated.script\n"
        "12346    1 nginx: worker process\n"
        "12347    1 postgres: writer\n"
    )
    fake_run = MagicMock()
    fake_run.stdout = ps_output
    with patch("batch_service.executor.subprocess.run", return_value=fake_run), \
         patch("batch_service.executor._os.kill") as kill:
        executor.sweep_and_kill_orphaned_runners()
    kill.assert_not_called()


def test_sweep_survives_ps_failure():
    """``ps`` unavailable or timing out shouldn't crash startup. Log the
    problem and continue — worst case we boot with orphans still alive
    and their next scheduled job will fail with a lock conflict (recoverable)."""
    with patch("batch_service.executor.subprocess.run",
               side_effect=FileNotFoundError("ps not on PATH")):
        # Must not raise.
        executor.sweep_and_kill_orphaned_runners()


def test_sweep_survives_kill_permission_error():
    """The PID we tried to kill vanished between ``ps`` output and our
    kill call (race). Must not raise — this is expected on a busy system."""
    ps_output = (
        "  PID  PPID COMMAND\n"
        "12345    1 python -m batch_service.runner extract --model-dir /m --job-id j1\n"
    )
    fake_run = MagicMock()
    fake_run.stdout = ps_output
    with patch("batch_service.executor.subprocess.run", return_value=fake_run), \
         patch("batch_service.executor._os.kill", side_effect=ProcessLookupError):
        # Must not raise; the process is already gone which is what we wanted.
        executor.sweep_and_kill_orphaned_runners()
