# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""metadata_db.py — lightweight SQLite store for artifact version history.

Stores every unique save of entity / mapper / feature / extractor / query YAML
in a single central DB shared across all models.

DB path resolution (first wins):
  1. ``db_path`` argument — explicit override used in tests.
  2. ``API_DB_PATH`` environment variable — absolute path to the SQLite file.
  3. ``{VZ_BASE_DIR}/data/api.db`` — alongside the model data directory.
  4. ``{repo_root}/data/api.db`` — fallback for local development.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

# ---------------------------------------------------------------------------
# Artifact type literals
# ---------------------------------------------------------------------------

ARTIFACT_TYPES = frozenset({"entity", "mapper", "feature", "extractor", "query", "view", "application"})

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS artifact_versions (
    id          TEXT    PRIMARY KEY,
    model_id    TEXT    NOT NULL,
    type        TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    version_num INTEGER NOT NULL,
    content     TEXT    NOT NULL,
    checksum    TEXT    NOT NULL,
    message     TEXT,
    created_at  TEXT    NOT NULL,
    is_current  INTEGER NOT NULL DEFAULT 0,
    -- VG-250: ownership columns. Nullable so historical rows + writes
    -- with no resolvable user (system / startup seeds) stay valid.
    created_by  TEXT,
    created_via TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_current
    ON artifact_versions (model_id, type, name)
    WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS ix_history
    ON artifact_versions (model_id, type, name, version_num);

CREATE TABLE IF NOT EXISTS schema_migrations (
    name        TEXT    PRIMARY KEY,
    applied_at  TEXT    NOT NULL
);

-- VG-258: certification marker for views / queries / features. One row
-- per (model_id, type, name); stays sticky across edits. Backfilled
-- once for pre-existing artifacts by the ``backfill_certify_existing``
-- migration so the new default ("only certified is visible") doesn't
-- accidentally hide everything.
CREATE TABLE IF NOT EXISTS artifact_certifications (
    model_id      TEXT NOT NULL,
    type          TEXT NOT NULL,
    name          TEXT NOT NULL,
    certified_by  TEXT,
    certified_at  TEXT NOT NULL,
    PRIMARY KEY (model_id, type, name)
);

-- Epic 26 VG-294: change-proposal queue. Members propose changes to
-- governed surfaces (ontology rows, mappers, extractors); owners +
-- admins approve / reject. Conflict policy: two pending proposals on
-- the same artifact both stay open; approving one marks the other
-- `superseded` with a link to the winner.
CREATE TABLE IF NOT EXISTS proposals (
    id                TEXT    PRIMARY KEY,
    model_id          TEXT    NOT NULL,
    entity_name       TEXT,                -- null for cross-entity (extractor) proposals
    artifact_kind     TEXT    NOT NULL,    -- attribute|relation|computed|mapper|extractor|sub_group
    artifact_name     TEXT    NOT NULL,
    proposed_by       TEXT    NOT NULL,
    reason            TEXT    NOT NULL,
    before_yaml       TEXT,
    after_yaml        TEXT,
    status            TEXT    NOT NULL,    -- pending|approved|rejected|superseded
    notified_to       TEXT    NOT NULL,    -- JSON array of user identifiers
    decision_actor    TEXT,
    decision_at       TEXT,
    decision_comment  TEXT,
    superseded_by     TEXT,                -- id of the winning proposal that closed this
    created_at        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_proposals_entity
    ON proposals (model_id, entity_name, status);
CREATE INDEX IF NOT EXISTS ix_proposals_artifact
    ON proposals (model_id, artifact_kind, artifact_name, status);
"""


# Type literals eligible for certification. Entities / mappers / extractors
# are admin-only and don't appear in the user-facing library, so they don't
# need (or get) a certification surface.
CERTIFIABLE_TYPES = frozenset({"view", "query", "feature"})


# ---------------------------------------------------------------------------
# Migrations
#
# One-shot data migrations live here. Each is keyed by a stable name written
# to ``schema_migrations`` after success; subsequent connects skip it.
# Pure DDL changes go in ``_DDL`` (idempotent CREATE / ALTER IF NOT EXISTS);
# migrations are for *data* fixes that mustn't repeat.
# ---------------------------------------------------------------------------

def _migration_backfill_certify_existing(conn: sqlite3.Connection) -> None:
    """Mark every view/query/feature currently in artifact_versions as certified.

    Runs once per database. New artifacts created after this point default
    to uncertified, which is the whole point of the cert filter — but if we
    didn't backfill, the first deploy would hide every existing library
    item from every user.
    """
    now = datetime.now(UTC).isoformat()
    rows = conn.execute(
        "SELECT DISTINCT model_id, type, name FROM artifact_versions "
        "WHERE is_current=1 AND type IN ('view','query','feature')"
    ).fetchall()
    for r in rows:
        conn.execute(
            "INSERT OR IGNORE INTO artifact_certifications "
            "(model_id, type, name, certified_by, certified_at) "
            "VALUES (?, ?, ?, NULL, ?)",
            (r["model_id"], r["type"], r["name"], now),
        )


def _migration_add_ownership_columns(conn: sqlite3.Connection) -> None:
    """VG-250: add ``created_by`` + ``created_via`` to ``artifact_versions``.

    For fresh databases the columns are already created by ``_DDL``; for
    existing prod DBs they need to be added via ALTER. SQLite's
    ``ADD COLUMN`` isn't idempotent, so we inspect the current schema
    before each ALTER. Both columns are nullable — historical rows stay
    valid with NULL author/origin until backfilled or rewritten.
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(artifact_versions)").fetchall()}
    if "created_by" not in cols:
        conn.execute("ALTER TABLE artifact_versions ADD COLUMN created_by TEXT")
    if "created_via" not in cols:
        conn.execute("ALTER TABLE artifact_versions ADD COLUMN created_via TEXT")


_MIGRATIONS: dict[str, callable] = {
    "backfill_certify_existing": _migration_backfill_certify_existing,
    "add_ownership_columns": _migration_add_ownership_columns,
}


def _run_pending_migrations(conn: sqlite3.Connection) -> None:
    """Apply any migration not already recorded in ``schema_migrations``."""
    applied = {
        r["name"] for r in conn.execute("SELECT name FROM schema_migrations").fetchall()
    }
    now = datetime.now(UTC).isoformat()
    for name, fn in _MIGRATIONS.items():
        if name in applied:
            continue
        fn(conn)
        conn.execute(
            "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)",
            (name, now),
        )


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def get_api_db_path(db_path: Path | None = None) -> Path:
    """Return the central api.db path."""
    if db_path is not None:
        return db_path
    env = os.environ.get("API_DB_PATH")
    if env:
        return Path(env)
    base = os.environ.get("VZ_BASE_DIR")
    if base:
        return Path(base) / "data" / "api.db"
    return Path(__file__).resolve().parents[1] / "data" / "api.db"


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@contextmanager
def _connect(model_dir: Path, db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    path = get_api_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.executescript(_DDL)
        _run_pending_migrations(conn)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Optional hook called after a new artifact version is written. Used by
# the embeddings indexer (Epic 20 VG-230) to reindex on save. Set via
# ``set_index_hook``; nothing else should mutate it.
_INDEX_HOOK = None


def set_index_hook(hook) -> None:
    """Register a ``(model_dir, artifact_type, name, content) -> None`` callback.

    Fired after ``record_version`` writes a new version (skipped on no-op).
    Implementations must not raise — the hook is best-effort, never on
    the critical save path. Pass ``None`` to clear.
    """
    global _INDEX_HOOK
    _INDEX_HOOK = hook


def record_version(
    model_dir: Path,
    artifact_type: str,
    name: str,
    content: str,
    message: str | None = None,
    db_path: Path | None = None,
    user_id: str | None = None,
    via: str | None = None,
) -> bool:
    """Snapshot content for an artifact.

    Returns True if a new version row was created, False if content is
    identical to the current version (no-op). Fires ``_INDEX_HOOK`` on
    True so the embeddings layer can reindex.

    VG-250: ``user_id`` is the authoring user's internal UUID (from
    ``Depends(get_current_user)``); ``via`` is the surface that
    triggered the write — one of ``'editor' | 'chat' | 'sync' |
    'system'``. Both default to None for backwards compatibility with
    callers that pre-date the ownership PR (their writes show up as
    unattributed in the audit trail).
    """
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"Unknown artifact type: {artifact_type!r}")

    checksum = hashlib.sha256(content.encode()).hexdigest()
    model_id = Path(model_dir).name
    now = datetime.now(UTC).isoformat()

    with _connect(model_dir, db_path) as conn:
        # Is the current version identical?
        cur = conn.execute(
            "SELECT checksum FROM artifact_versions "
            "WHERE model_id=? AND type=? AND name=? AND is_current=1",
            (model_id, artifact_type, name),
        )
        row = cur.fetchone()
        if row and row["checksum"] == checksum:
            return False  # content unchanged — skip

        # Next version number
        cur = conn.execute(
            "SELECT COALESCE(MAX(version_num), 0) + 1 "
            "FROM artifact_versions WHERE model_id=? AND type=? AND name=?",
            (model_id, artifact_type, name),
        )
        next_num = cur.fetchone()[0]

        # Retire current flag
        conn.execute(
            "UPDATE artifact_versions SET is_current=0 "
            "WHERE model_id=? AND type=? AND name=? AND is_current=1",
            (model_id, artifact_type, name),
        )

        # Insert new current version
        conn.execute(
            """INSERT INTO artifact_versions
               (id, model_id, type, name, version_num, content, checksum,
                message, created_at, is_current, created_by, created_via)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
            (str(uuid4()), model_id, artifact_type, name,
             next_num, content, checksum, message, now, user_id, via),
        )

    # Fire the indexing hook outside the DB transaction so a slow hook
    # doesn't hold the SQLite lock. Hooks are expected to be non-raising
    # but we guard defensively — index failures must never fail a save.
    if _INDEX_HOOK is not None:
        try:
            _INDEX_HOOK(model_dir, artifact_type, name, content)
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "Artifact index hook raised for %s/%s — ignored", artifact_type, name,
            )
    return True


def list_versions(
    model_dir: Path,
    artifact_type: str,
    name: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Return version metadata (no content) newest-first.

    Includes ``created_by`` / ``created_via`` so callers building activity
    feeds (Epic 26 VG-290) can show who landed each version without an
    extra query per row.
    """
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        rows = conn.execute(
            """SELECT id, version_num, checksum, message, created_at,
                      is_current, created_by, created_via
               FROM artifact_versions
               WHERE model_id=? AND type=? AND name=?
               ORDER BY version_num DESC""",
            (model_id, artifact_type, name),
        ).fetchall()
    return [dict(r) for r in rows]


def get_version(
    model_dir: Path,
    artifact_type: str,
    name: str,
    version_id: str,
    db_path: Path | None = None,
) -> dict | None:
    """Return full version record including content, or None if not found."""
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        row = conn.execute(
            "SELECT * FROM artifact_versions WHERE id=? AND model_id=? AND type=? AND name=?",
            (version_id, model_id, artifact_type, name),
        ).fetchone()
    return dict(row) if row else None


def get_current_content(
    model_dir: Path,
    artifact_type: str,
    name: str,
    db_path: Path | None = None,
) -> str | None:
    """Return the current YAML content for an artifact, or None if not found."""
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"Unknown artifact type: {artifact_type!r}")
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        row = conn.execute(
            "SELECT content FROM artifact_versions "
            "WHERE model_id=? AND type=? AND name=? AND is_current=1",
            (model_id, artifact_type, name),
        ).fetchone()
    return row["content"] if row else None


def list_artifact_names(
    model_dir: Path,
    artifact_type: str,
    db_path: Path | None = None,
) -> list[str]:
    """Return names of all current artifacts of the given type, sorted."""
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"Unknown artifact type: {artifact_type!r}")
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM artifact_versions "
            "WHERE model_id=? AND type=? AND is_current=1 "
            "ORDER BY name",
            (model_id, artifact_type),
        ).fetchall()
    return [r["name"] for r in rows]


def delete_artifact(
    model_dir: Path,
    artifact_type: str,
    name: str,
    db_path: Path | None = None,
) -> None:
    """Mark an artifact's current version as retired (soft delete)."""
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"Unknown artifact type: {artifact_type!r}")
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        conn.execute(
            "UPDATE artifact_versions SET is_current=0 "
            "WHERE model_id=? AND type=? AND name=? AND is_current=1",
            (model_id, artifact_type, name),
        )


def seed_from_directory(model_dir: Path, db_path: Path | None = None) -> int:
    """Seed the metadata DB from YAML files in model subdirectories.

    Used for migrating existing models and for seeding test fixtures.
    Returns the number of artifacts seeded.
    """
    import yaml as _yaml

    model_dir = Path(model_dir)
    type_dir_map = {
        "entity": (model_dir / "ontology", "*.yaml", None),
        "mapper": (model_dir / "mappers", "*.yaml", None),
        "query": (model_dir / "queries", "*.yaml", None),
        "view": (model_dir / "views", "*.yaml", None),
        "application": (model_dir / "applications", "*.yaml", None),
        "extractor": (model_dir / "extractors", "extractor_*.yaml", "extractor_"),
    }
    count = 0
    for artifact_type, (subdir, pattern, strip_prefix) in type_dir_map.items():
        if not subdir.is_dir():
            continue
        for path in sorted(subdir.glob(pattern)):
            stem = path.stem
            if strip_prefix and stem.startswith(strip_prefix):
                stem = stem[len(strip_prefix):]
            content = path.read_text()
            if record_version(model_dir, artifact_type, stem, content, db_path=db_path):
                count += 1

    # Features use feature_id (not file stem) as the name
    features_dir = model_dir / "features"
    if features_dir.is_dir():
        for path in sorted(features_dir.glob("*.yaml")):
            try:
                data = _yaml.safe_load(path.read_text())
                feature_id = data.get("feature_id", path.stem)
            except Exception:
                feature_id = path.stem
            content = path.read_text()
            if record_version(model_dir, "feature", feature_id, content, db_path=db_path):
                count += 1

    return count


# ---------------------------------------------------------------------------
# Ownership (VG-250 / VG-252)
# ---------------------------------------------------------------------------


def get_owner(
    model_dir: Path, artifact_type: str, name: str,
    db_path: Path | None = None,
) -> dict | None:
    """Return ``{created_by, created_via, created_at}`` for the current version.

    None if the artifact has no current version. Values may individually
    be NULL when the row pre-dates VG-250 or was written by a path that
    didn't pass author info.
    """
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        row = conn.execute(
            "SELECT created_by, created_via, created_at "
            "FROM artifact_versions "
            "WHERE model_id=? AND type=? AND name=? AND is_current=1",
            (model_id, artifact_type, name),
        ).fetchone()
    if row is None:
        return None
    return {
        "created_by": row["created_by"],
        "created_via": row["created_via"],
        "created_at": row["created_at"],
    }


def list_owners(
    model_dir: Path, artifact_type: str,
    db_path: Path | None = None,
) -> dict[str, dict]:
    """Batched lookup keyed by ``name`` for ``artifact_type`` (current version).

    Used by list endpoints to populate owner fields in one query.
    """
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        rows = conn.execute(
            "SELECT name, created_by, created_via, created_at "
            "FROM artifact_versions "
            "WHERE model_id=? AND type=? AND is_current=1",
            (model_id, artifact_type),
        ).fetchall()
    return {
        r["name"]: {
            "created_by": r["created_by"],
            "created_via": r["created_via"],
            "created_at": r["created_at"],
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# Certification (VG-258)
# ---------------------------------------------------------------------------


def certify(
    model_dir: Path, artifact_type: str, name: str,
    user_id: str | None = None,
    db_path: Path | None = None,
) -> None:
    """Mark ``(model, type, name)`` as certified by ``user_id``.

    Idempotent — re-certifying an already-certified artifact just refreshes
    the timestamp + user. Raises ``ValueError`` for types outside
    ``CERTIFIABLE_TYPES``.
    """
    if artifact_type not in CERTIFIABLE_TYPES:
        raise ValueError(
            f"Cannot certify type {artifact_type!r}; "
            f"only {sorted(CERTIFIABLE_TYPES)} are certifiable."
        )
    model_id = Path(model_dir).name
    now = datetime.now(UTC).isoformat()
    with _connect(model_dir, db_path) as conn:
        conn.execute(
            "INSERT INTO artifact_certifications "
            "(model_id, type, name, certified_by, certified_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (model_id, type, name) DO UPDATE SET "
            "certified_by=excluded.certified_by, certified_at=excluded.certified_at",
            (model_id, artifact_type, name, user_id, now),
        )


def uncertify(
    model_dir: Path, artifact_type: str, name: str,
    db_path: Path | None = None,
) -> bool:
    """Remove the certification marker. Returns True if a row was deleted."""
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        cur = conn.execute(
            "DELETE FROM artifact_certifications "
            "WHERE model_id=? AND type=? AND name=?",
            (model_id, artifact_type, name),
        )
        return cur.rowcount > 0


def get_certification(
    model_dir: Path, artifact_type: str, name: str,
    db_path: Path | None = None,
) -> dict | None:
    """Return ``{certified_by, certified_at}`` or None if uncertified."""
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        row = conn.execute(
            "SELECT certified_by, certified_at FROM artifact_certifications "
            "WHERE model_id=? AND type=? AND name=?",
            (model_id, artifact_type, name),
        ).fetchone()
    if row is None:
        return None
    return {"certified_by": row["certified_by"], "certified_at": row["certified_at"]}


def is_certified(
    model_dir: Path, artifact_type: str, name: str,
    db_path: Path | None = None,
) -> bool:
    """Whether ``(model, type, name)`` has a certification row."""
    return get_certification(model_dir, artifact_type, name, db_path=db_path) is not None


def list_certifications(
    model_dir: Path, artifact_type: str | None = None,
    db_path: Path | None = None,
) -> dict[tuple[str, str], dict]:
    """All certifications for a model, keyed by ``(type, name)``.

    Used by list endpoints to populate certification fields in one query
    instead of N. Filter by ``artifact_type`` to scope to a single kind.
    """
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        if artifact_type is not None:
            rows = conn.execute(
                "SELECT type, name, certified_by, certified_at "
                "FROM artifact_certifications WHERE model_id=? AND type=?",
                (model_id, artifact_type),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT type, name, certified_by, certified_at "
                "FROM artifact_certifications WHERE model_id=?",
                (model_id,),
            ).fetchall()
    return {
        (r["type"], r["name"]): {
            "certified_by": r["certified_by"],
            "certified_at": r["certified_at"],
        }
        for r in rows
    }


def migrate_from_legacy_db(model_dir: Path, db_path: Path | None = None) -> int:
    """Import current artifact versions from a legacy per-model scryglass-metadata.db.

    Reads every ``is_current=1`` row from the legacy DB and calls
    ``record_version`` to insert it into the central api.db.  Idempotent —
    rows whose content is already present (same checksum) are skipped.

    Returns the number of new rows inserted.
    """
    model_dir = Path(model_dir)
    legacy_path = model_dir / "data" / "scryglass-metadata.db"
    if not legacy_path.exists():
        return 0

    try:
        legacy_conn = sqlite3.connect(str(legacy_path))
        legacy_conn.row_factory = sqlite3.Row
        rows = legacy_conn.execute(
            "SELECT type, name, content FROM artifact_versions WHERE is_current=1"
        ).fetchall()
        legacy_conn.close()
    except Exception:
        return 0

    count = 0
    for row in rows:
        artifact_type = row["type"]
        if artifact_type not in ARTIFACT_TYPES:
            continue
        if record_version(model_dir, artifact_type, row["name"], row["content"], db_path=db_path):
            count += 1
    return count
