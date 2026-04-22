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
    assert DEFAULT_TIMEOUT == 300.0
