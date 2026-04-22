# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""SQLite job store for the vizgrams-batch service.

Uses the central batch.db at ``{BATCH_DB_PATH}`` (or ``{VZ_BASE_DIR}/data/batch.db``).
All functions accept an open ``sqlite3.Connection``; callers manage lifetime
via ``get_connection(model_dir)`` context manager.

VG-108: consolidated from per-model scryglass-batch.db into the central batch.db.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from core.batch_db import (
    append_batch_progress as append_progress,
)
from core.batch_db import (
    get_batch_db_path,
    list_schedules,
    mark_orphaned_batch_jobs,
    upsert_schedule,
)
from core.batch_db import (
    get_batch_job as get_job,
)
from core.batch_db import (
    get_batch_progress as get_progress,
)
from core.batch_db import (
    get_connection as _core_get_connection,
)
from core.batch_db import (
    insert_batch_job as insert_job,
)
from core.batch_db import (
    list_batch_jobs as list_jobs,
)
from core.batch_db import (
    update_batch_job as update_job,
)

__all__ = [
    "get_connection",
    "db_path",
    "insert_job",
    "update_job",
    "get_job",
    "list_jobs",
    "mark_orphaned_jobs",
    "append_progress",
    "get_progress",
    "upsert_schedule",
    "list_schedules",
]


def db_path(model_dir: Path | None = None) -> Path:
    """Return the path to the central batch.db.

    The ``model_dir`` argument is accepted for backwards compatibility but
    is ignored — all batch service jobs are stored in the central DB.
    """
    return get_batch_db_path()


@contextmanager
def get_connection(model_dir: Path | None = None):
    """Open a connection to the central batch.db.

    The ``model_dir`` argument is accepted for backwards compatibility but
    is ignored — the central DB path is resolved via ``BATCH_DB_PATH`` /
    ``VZ_BASE_DIR`` / repo fallback.
    """
    with _core_get_connection() as con:
        yield con


def mark_orphaned_jobs(model_dir: Path, completed_at: str) -> int:
    """Mark any batch jobs still in 'running' or 'cancelling' status as failed.

    Called at startup to clean up jobs that were running when the process
    last died.  Returns the number of rows updated.

    Note: ``completed_at`` is accepted for backwards compatibility but the
    current timestamp is used internally.
    """
    return mark_orphaned_batch_jobs()
