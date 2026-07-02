# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Per-model write lock for serialising SQLite access across threads and processes."""

import errno
import fcntl
import logging
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

import os as _os

# Lock file name inside model_dir.  Gitignored (see .gitignore).
_LOCK_FILENAME = ".write.lock"

# Default timeout in seconds. The lock serialises all four batch job types
# (extract, map, materialize, reconcile) — so a job waiting behind a live
# extract sits in the ``running`` status until the lock frees. 5 minutes
# is the sweet spot: long enough that a brief file-extract or fast mapper
# will always complete without stepping on other jobs, short enough that
# an operator watching "running" for hours knows something's off (a
# scheduled ``git`` extract that legitimately takes hours will fail every
# subsequent job's ``run mapper`` and ``rematerialize`` triggers within
# the same window — which is the correct signal that a manual retry
# needs to wait for the extract to complete).
#
# Override with ``VZ_WRITE_LOCK_TIMEOUT`` (seconds) — bump it up for
# background ops that legitimately want to queue for hours, drop it for
# fail-fast interactive semantics.
DEFAULT_TIMEOUT = float(_os.environ.get("VZ_WRITE_LOCK_TIMEOUT", 300.0))

# Polling interval while waiting for the lock (seconds).
_POLL_INTERVAL = 0.5


class LockTimeoutError(TimeoutError):
    """Raised when the write lock cannot be acquired within the timeout."""


@contextmanager
def model_write_lock(
    model_dir: Path,
    timeout: float = DEFAULT_TIMEOUT,
    wait_cb=None,
):
    """Acquire an exclusive write lock for *model_dir*.

    Blocks in 0.5-second increments until the lock is available or *timeout*
    seconds have elapsed.  Releases the lock automatically on exit.

    Works across both threads and OS processes (uses ``fcntl.flock``).

    ``wait_cb`` is an optional ``Callable[[str], None]`` invoked once, at
    the moment the acquire starts blocking, with a human-readable reason
    (``"waiting for write lock…"``). Job runners pass their ``_progress``
    hook so operators can see WHY a job's status is ``running`` for a
    while — otherwise a legitimate lock-wait behind a live extract is
    indistinguishable from a hang.

    Usage::

        with model_write_lock(model_dir, wait_cb=_progress):
            db.execute(...)

    Raises:
        LockTimeoutError: if the lock cannot be acquired within *timeout* seconds.
    """
    lock_path = model_dir / _LOCK_FILENAME
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    deadline = time.monotonic() + timeout
    waited = False

    fd = lock_path.open("w")
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break  # acquired
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if time.monotonic() >= deadline:
                    raise LockTimeoutError(
                        f"Could not acquire write lock for model '{model_dir.name}' "
                        f"within {timeout:.0f}s — another job may be running."
                    ) from None
                if not waited:
                    logger.info(
                        "Waiting for write lock on model '%s'", model_dir.name,
                        extra={"model": model_dir.name},
                    )
                    if wait_cb is not None:
                        try:
                            wait_cb(
                                f"waiting for write lock on model "
                                f"'{model_dir.name}' — another job is holding it"
                            )
                        except Exception:
                            # The progress-reporting path is a side channel;
                            # a failure there must never break the wait loop.
                            logger.debug("wait_cb raised", exc_info=True)
                    waited = True
                time.sleep(_POLL_INTERVAL)

        if waited:
            logger.info(
                "Write lock acquired for model '%s'", model_dir.name,
                extra={"model": model_dir.name},
            )
            if wait_cb is not None:
                try:
                    wait_cb("write lock acquired, resuming")
                except Exception:
                    logger.debug("wait_cb raised", exc_info=True)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        fd.close()
