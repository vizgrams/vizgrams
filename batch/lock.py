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

# Lock file name inside model_dir.  Gitignored (see .gitignore).
_LOCK_FILENAME = ".write.lock"

# Default timeout in seconds.  300 s covers even long-running extractors.
DEFAULT_TIMEOUT = 300.0

# Polling interval while waiting for the lock (seconds).
_POLL_INTERVAL = 0.5


class LockTimeoutError(TimeoutError):
    """Raised when the write lock cannot be acquired within the timeout."""


@contextmanager
def model_write_lock(model_dir: Path, timeout: float = DEFAULT_TIMEOUT):
    """Acquire an exclusive write lock for *model_dir*.

    Blocks in 0.5-second increments until the lock is available or *timeout*
    seconds have elapsed.  Releases the lock automatically on exit.

    Works across both threads and OS processes (uses ``fcntl.flock``).

    Usage::

        with model_write_lock(model_dir):
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
                    waited = True
                time.sleep(_POLL_INTERVAL)

        if waited:
            logger.info(
                "Write lock acquired for model '%s'", model_dir.name,
                extra={"model": model_dir.name},
            )
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        fd.close()
