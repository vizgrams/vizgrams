# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for batch.lock — per-model write lock."""

import threading
import time

import pytest

from batch.lock import DEFAULT_TIMEOUT, LockTimeoutError, model_write_lock

# ---------------------------------------------------------------------------
# Basic acquisition
# ---------------------------------------------------------------------------


def test_lock_acquired_and_released(tmp_path):
    """Lock is acquired and released without error."""
    with model_write_lock(tmp_path):
        assert (tmp_path / ".write.lock").exists()


def test_lock_file_created(tmp_path):
    with model_write_lock(tmp_path):
        pass
    # Lock file persists after release (harmless; empty sentinel file)
    assert (tmp_path / ".write.lock").exists()


def test_lock_creates_missing_parent(tmp_path):
    """Lock creates model_dir if it doesn't exist yet."""
    model_dir = tmp_path / "new_model"
    assert not model_dir.exists()
    with model_write_lock(model_dir):
        assert model_dir.exists()


def test_lock_is_reentrant_within_same_process(tmp_path):
    """fcntl.flock is per-fd; two sequential acquisitions in the same process work."""
    with model_write_lock(tmp_path):
        pass
    with model_write_lock(tmp_path):
        pass


# ---------------------------------------------------------------------------
# Exclusion across threads
# ---------------------------------------------------------------------------


def test_second_thread_blocks_until_first_releases(tmp_path):
    """A second thread cannot enter the lock while the first holds it."""
    entered = threading.Event()
    release = threading.Event()
    second_acquired_at = []

    def holder():
        with model_write_lock(tmp_path):
            entered.set()
            release.wait()

    def waiter():
        release.wait()  # ensure holder entered first
        with model_write_lock(tmp_path):
            second_acquired_at.append(time.monotonic())

    t1 = threading.Thread(target=holder)
    t2 = threading.Thread(target=waiter)
    t1.start()
    entered.wait()

    t2.start()
    held_until = time.monotonic()
    time.sleep(0.1)
    release.set()

    t1.join()
    t2.join()

    # waiter must have acquired AFTER the holder released
    assert second_acquired_at[0] >= held_until


def test_lock_timeout_raises(tmp_path):
    """LockTimeoutError is raised when the lock cannot be acquired in time."""
    acquired = threading.Event()
    keep_holding = threading.Event()

    def holder():
        with model_write_lock(tmp_path):
            acquired.set()
            keep_holding.wait(timeout=5)

    t = threading.Thread(target=holder)
    t.start()
    acquired.wait()

    try:
        with pytest.raises(LockTimeoutError, match="write lock"), model_write_lock(tmp_path, timeout=0.3):
            pass
    finally:
        keep_holding.set()
        t.join()


def test_lock_timeout_message_includes_model_name(tmp_path):
    acquired = threading.Event()
    keep_holding = threading.Event()
    model_dir = tmp_path / "my_model"
    model_dir.mkdir()

    def holder():
        with model_write_lock(model_dir):
            acquired.set()
            keep_holding.wait(timeout=5)

    t = threading.Thread(target=holder)
    t.start()
    acquired.wait()

    try:
        with pytest.raises(LockTimeoutError, match="my_model"), model_write_lock(model_dir, timeout=0.3):
            pass
    finally:
        keep_holding.set()
        t.join()


# ---------------------------------------------------------------------------
# Exception safety
# ---------------------------------------------------------------------------


def test_lock_released_on_exception(tmp_path):
    """Lock is released even if the body raises."""
    with pytest.raises(ValueError), model_write_lock(tmp_path):
        raise ValueError("boom")

    # Should be acquirable again immediately
    with model_write_lock(tmp_path):
        pass


def test_lock_released_after_timeout_expiry(tmp_path):
    """After a LockTimeoutError the original lock is still held by its owner,
    but once that owner exits the lock is acquirable again."""
    acquired = threading.Event()
    release = threading.Event()

    def holder():
        with model_write_lock(tmp_path):
            acquired.set()
            release.wait(timeout=5)

    t = threading.Thread(target=holder)
    t.start()
    acquired.wait()

    with pytest.raises(LockTimeoutError), model_write_lock(tmp_path, timeout=0.2):
        pass

    release.set()
    t.join()

    # Now the lock is free
    with model_write_lock(tmp_path):
        pass


# ---------------------------------------------------------------------------
# Default timeout constant
# ---------------------------------------------------------------------------


def test_default_timeout_is_reasonable():
    """Serialising extract on the same lock means a job waiting behind a
    live git extract sits in ``running`` status until it either gets the
    lock or hits the timeout. Bigger than a few minutes and operators
    see "jobs running forever" (a real regression we already shipped);
    smaller than a minute and legitimate fast jobs fail on brief
    contention. 300 s is the ambient value the codebase used for
    materialize + reconcile before extract joined; keep it as the floor
    so anyone lowering the default without thinking about the wait UX
    trips this."""
    assert DEFAULT_TIMEOUT >= 60.0
    assert DEFAULT_TIMEOUT <= 3600.0


def test_wait_cb_fires_on_lock_contention(tmp_path):
    """The ``wait_cb`` hook is how job runners surface "queued for the
    lock" as job progress — without it, users see a bare ``running``
    status and can't tell whether the job is doing work or blocked."""
    received = []

    def other_holder():
        with model_write_lock(tmp_path):
            time.sleep(0.5)

    t = threading.Thread(target=other_holder)
    t.start()
    time.sleep(0.1)  # let the other thread grab the lock
    with model_write_lock(tmp_path, wait_cb=received.append):
        pass
    t.join()
    # Two progress notes: one at the start of the wait, one at acquire.
    assert len(received) == 2
    assert "waiting" in received[0].lower()
    assert "acquired" in received[1].lower()


def test_wait_cb_not_called_on_uncontested_acquire(tmp_path):
    """If the lock is free, nothing to report. Waking the operator up
    with a "acquired" note for every job would be noise."""
    received = []
    with model_write_lock(tmp_path, wait_cb=received.append):
        pass
    assert received == []


def test_wait_cb_failure_does_not_break_the_wait(tmp_path):
    """The progress channel is a side effect. A user's ``_progress``
    that raises (e.g. batch DB unreachable) must not prevent the lock
    from being acquired — the wait continues as a plain timeout."""
    def raising_cb(_msg):
        raise RuntimeError("batch db down")

    def other_holder():
        with model_write_lock(tmp_path):
            time.sleep(0.3)

    t = threading.Thread(target=other_holder)
    t.start()
    time.sleep(0.1)
    # Should not raise despite the callback raising.
    with model_write_lock(tmp_path, wait_cb=raising_cb):
        pass
    t.join()


def test_default_timeout_is_env_configurable(monkeypatch):
    """``VZ_WRITE_LOCK_TIMEOUT`` lets ops dial the wait up or down without
    a code change. Reloading the module picks up the override so the env
    is honoured at process start rather than baked at import time."""
    import importlib

    import batch.lock as lock_mod
    monkeypatch.setenv("VZ_WRITE_LOCK_TIMEOUT", "42")
    reloaded = importlib.reload(lock_mod)
    try:
        assert reloaded.DEFAULT_TIMEOUT == 42.0
    finally:
        # Reload back to whatever the ambient env said, so subsequent
        # tests see the real default. monkeypatch un-sets the env var
        # after this test finishes, so this restores state cleanly.
        importlib.reload(lock_mod)
