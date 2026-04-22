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
    is_current  INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_current
    ON artifact_versions (model_id, type, name)
    WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS ix_history
    ON artifact_versions (model_id, type, name, version_num);
"""


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

def record_version(
    model_dir: Path,
    artifact_type: str,
    name: str,
    content: str,
    message: str | None = None,
    db_path: Path | None = None,
) -> bool:
    """Snapshot content for an artifact.

    Returns True if a new version row was created, False if content is
    identical to the current version (no-op).
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
               (id, model_id, type, name, version_num, content, checksum, message, created_at, is_current)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
            (str(uuid4()), model_id, artifact_type, name,
             next_num, content, checksum, message, now),
        )
        return True


def list_versions(
    model_dir: Path,
    artifact_type: str,
    name: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Return version metadata (no content) newest-first."""
    model_id = Path(model_dir).name
    with _connect(model_dir, db_path) as conn:
        rows = conn.execute(
            """SELECT id, version_num, checksum, message, created_at, is_current
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
