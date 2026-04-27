# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""vizgrams_db.py — platform-level SQLite store for published vizgrams.

Stores published vizgrams (chart posts), feed engagement (likes/saves), and
the data hashes used for LLM caption cache invalidation.

Shares the same DB file as metadata_db (api.db).  DB path resolution
(first wins):
  1. ``db_path`` argument — explicit override used in tests.
  2. ``API_DB_PATH`` environment variable — absolute path to the SQLite file.
  3. ``{VZ_BASE_DIR}/data/api.db`` — alongside the model data directory.
  4. ``{repo_root}/data/api.db`` — fallback for local development.

Schema overview:
  vizgrams            — published chart posts (core content unit)
  vizgram_engagements — likes and saves per user per vizgram
  users               — stable identity table (provider + external_id → UUID)
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS models (
    id           TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    owner        TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    updated_at   TEXT,
    status       TEXT NOT NULL DEFAULT 'experimental',
    tags         TEXT NOT NULL DEFAULT '[]',
    access_rules TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,
    provider        TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    email           TEXT,
    display_name    TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (provider, external_id)
);

CREATE TABLE IF NOT EXISTS vizgrams (
    id                   TEXT    PRIMARY KEY,
    dataset_ref          TEXT    NOT NULL,
    query_ref            TEXT    NOT NULL,
    slice_config         TEXT    NOT NULL DEFAULT '{}',
    chart_config         TEXT    NOT NULL DEFAULT '{}',
    title                TEXT    NOT NULL,
    caption              TEXT,
    caption_hash         TEXT,
    live                 INTEGER NOT NULL DEFAULT 1,
    data_snapshot        TEXT,
    data_hash            TEXT,
    last_data_updated    TEXT,
    significance_score   REAL    NOT NULL DEFAULT 0.0,
    author_id            TEXT    NOT NULL,
    author_display_name  TEXT,
    published_at         TEXT    NOT NULL,
    updated_at           TEXT    NOT NULL,
    tags                 TEXT    NOT NULL DEFAULT '[]',
    is_deleted           INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_vg_feed
    ON vizgrams (is_deleted, significance_score DESC, published_at DESC);

CREATE INDEX IF NOT EXISTS ix_vg_author
    ON vizgrams (author_id, is_deleted, published_at DESC);

CREATE INDEX IF NOT EXISTS ix_vg_dataset
    ON vizgrams (dataset_ref, is_deleted);

CREATE TABLE IF NOT EXISTS vizgram_engagements (
    id          TEXT    PRIMARY KEY,
    vizgram_id  TEXT    NOT NULL REFERENCES vizgrams(id),
    user_id     TEXT    NOT NULL,
    type        TEXT    NOT NULL CHECK(type IN ('like', 'save')),
    created_at  TEXT    NOT NULL,
    UNIQUE (vizgram_id, user_id, type)
);

CREATE INDEX IF NOT EXISTS ix_eng_vizgram
    ON vizgram_engagements (vizgram_id, type);

CREATE INDEX IF NOT EXISTS ix_eng_user
    ON vizgram_engagements (user_id, type);
"""

# ---------------------------------------------------------------------------
# DB path resolution
# ---------------------------------------------------------------------------

def get_db_path(db_path: Path | None = None) -> Path:
    """Return the central api.db path (shared with metadata_db)."""
    from core.metadata_db import get_api_db_path
    return get_api_db_path(db_path)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@contextmanager
def _connect(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    path = get_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.executescript(_DDL)
        _run_migrations(conn)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Apply additive schema changes to existing databases."""
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM pragma_table_info('vizgrams')"
        ).fetchall()
    }
    if "author_display_name" not in existing:
        conn.execute("ALTER TABLE vizgrams ADD COLUMN author_display_name TEXT")
        conn.commit()


def _now() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# User identity
# ---------------------------------------------------------------------------

def resolve_user(
    provider: str,
    external_id: str,
    *,
    email: str | None = None,
    display_name: str | None = None,
    db_path: Path | None = None,
) -> str:
    """Return the internal UUID for a (provider, external_id) pair.

    Creates a new user record on first encounter.  Updates email / display_name
    if they have changed since last seen.
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM users WHERE provider=? AND external_id=?",
            (provider, external_id),
        ).fetchone()
        if row:
            user_id = row["id"]
            if email or display_name:
                conn.execute(
                    """UPDATE users
                       SET email=COALESCE(?,email),
                           display_name=COALESCE(?,display_name),
                           updated_at=?
                       WHERE id=?""",
                    (email, display_name, _now(), user_id),
                )
            return user_id
        user_id = str(uuid4())
        now = _now()
        conn.execute(
            """INSERT INTO users (id, provider, external_id, email, display_name, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (user_id, provider, external_id, email, display_name, now, now),
        )
    return user_id


def get_user_display_name(user_id: str, db_path: Path | None = None) -> str:
    """Return a human-readable name for display, falling back to the UUID."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT display_name, email FROM users WHERE id=?", (user_id,)
        ).fetchone()
    if not row:
        return user_id
    return row["display_name"] or (row["email"] or user_id).split("@")[0]


def migrate_email_ids(db_path: Path | None = None) -> int:
    """One-time migration: create user records for legacy email-based author/user IDs.

    Scans vizgrams.author_id and vizgram_engagements.user_id for values that
    look like email addresses and replaces them with stable internal UUIDs.
    Safe to re-run — already-migrated UUIDs are left untouched.

    Returns the number of distinct email identities migrated.
    """
    with _connect(db_path) as conn:
        emails: set[str] = set()
        for row in conn.execute(
            "SELECT DISTINCT author_id FROM vizgrams WHERE author_id LIKE '%@%'"
        ):
            emails.add(row[0])
        for row in conn.execute(
            "SELECT DISTINCT user_id FROM vizgram_engagements WHERE user_id LIKE '%@%'"
        ):
            emails.add(row[0])

        for email in emails:
            existing = conn.execute(
                "SELECT id FROM users WHERE provider='dex' AND external_id=?",
                (email,),
            ).fetchone()
            if existing:
                user_id = existing["id"]
            else:
                user_id = str(uuid4())
                now = _now()
                name = email.split("@")[0]
                conn.execute(
                    """INSERT INTO users (id, provider, external_id, email, display_name, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (user_id, "dex", email, email, name, now, now),
                )
            conn.execute(
                "UPDATE vizgrams SET author_id=? WHERE author_id=?",
                (user_id, email),
            )
            conn.execute(
                "UPDATE vizgram_engagements SET user_id=? WHERE user_id=?",
                (user_id, email),
            )
    return len(emails)


# ---------------------------------------------------------------------------
# Vizgram CRUD
# ---------------------------------------------------------------------------

def create_vizgram(
    *,
    dataset_ref: str,
    query_ref: str,
    title: str,
    author_id: str,
    author_display_name: str | None = None,
    slice_config: dict | None = None,
    chart_config: dict | None = None,
    tags: list[str] | None = None,
    live: bool = True,
    data_snapshot: list | None = None,
    significance_score: float = 0.0,
    db_path: Path | None = None,
) -> str:
    """Insert a new vizgram. Returns the generated id."""
    vizgram_id = str(uuid4())
    now = _now()
    conn_path = get_db_path(db_path)
    with _connect(conn_path) as conn:
        conn.execute(
            """INSERT INTO vizgrams
               (id, dataset_ref, query_ref, slice_config, chart_config,
                title, live, data_snapshot, significance_score,
                author_id, author_display_name, published_at, updated_at, tags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                vizgram_id,
                dataset_ref,
                query_ref,
                json.dumps(slice_config or {}),
                json.dumps(chart_config or {}),
                title,
                1 if live else 0,
                json.dumps(data_snapshot) if data_snapshot is not None else None,
                significance_score,
                author_id,
                author_display_name,
                now,
                now,
                json.dumps(tags or []),
            ),
        )
    return vizgram_id


def get_vizgram(vizgram_id: str, db_path: Path | None = None) -> dict | None:
    """Return a single vizgram by id, or None if not found / deleted."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM vizgrams WHERE id=? AND is_deleted=0",
            (vizgram_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def list_feed(
    *,
    limit: int = 20,
    offset: int = 0,
    dataset_ref: str | None = None,
    author_id: str | None = None,
    viewer_id: str | None = None,
    saved_only: bool = False,
    user_context: dict | None = None,
    db_path: Path | None = None,
) -> list[dict]:
    """Return published vizgrams ranked by freshness × significance × diversity.

    Fetches a candidate pool from the DB (including engagement counts and the
    viewer's own like/save state), then applies the ranking algorithm in Python.
    Pagination (offset) is applied after ranking so page ordering is stable.

    Optionally filtered by dataset_ref, author_id, or saved_only (viewer's
    bookmarked vizgrams — requires viewer_id).
    """
    from core.ranking import rank_feed

    pool_size = min(max((offset + limit) * 5, 100), 500)

    clauses = ["v.is_deleted=0"]
    where_params: list = []
    if dataset_ref:
        clauses.append("v.dataset_ref=?")
        where_params.append(dataset_ref)
    if author_id:
        clauses.append("v.author_id=?")
        where_params.append(author_id)
    if saved_only and viewer_id:
        clauses.append(
            "EXISTS (SELECT 1 FROM vizgram_engagements"
            " WHERE vizgram_id=v.id AND user_id=? AND type='save')"
        )
        where_params.append(viewer_id)
    where = " AND ".join(clauses)

    # Params order must match SQL: viewer engagement subqueries (in SELECT)
    # come before the WHERE clause and the LIMIT.
    params: list = [viewer_id or "", viewer_id or ""] + where_params + [pool_size]

    with _connect(db_path) as conn:
        rows = conn.execute(
            f"""SELECT v.*,
                    COALESCE((
                        SELECT COUNT(*) FROM vizgram_engagements
                        WHERE vizgram_id=v.id AND type='like'
                    ), 0) AS like_count,
                    COALESCE((
                        SELECT COUNT(*) FROM vizgram_engagements
                        WHERE vizgram_id=v.id AND type='save'
                    ), 0) AS save_count,
                    COALESCE((
                        SELECT 1 FROM vizgram_engagements
                        WHERE vizgram_id=v.id AND user_id=? AND type='like'
                        LIMIT 1
                    ), 0) AS viewer_liked,
                    COALESCE((
                        SELECT 1 FROM vizgram_engagements
                        WHERE vizgram_id=v.id AND user_id=? AND type='save'
                        LIMIT 1
                    ), 0) AS viewer_saved
                FROM vizgrams v
                WHERE {where}
                ORDER BY v.published_at DESC
                LIMIT ?""",
            params,
        ).fetchall()

    candidates = [_row_to_dict(r) for r in rows]
    return rank_feed(candidates, limit=limit, offset=offset, user_context=user_context)


def find_caption_by_hash(caption_hash: str, db_path: Path | None = None) -> str | None:
    """Return a cached caption previously generated for this data hash, or None.

    Allows reuse of an existing LLM caption when the same data is published
    again — avoids redundant API calls.
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT caption FROM vizgrams WHERE caption_hash=? AND caption IS NOT NULL LIMIT 1",
            (caption_hash,),
        ).fetchone()
    return row["caption"] if row else None


def update_caption(
    vizgram_id: str,
    caption: str,
    caption_hash: str,
    db_path: Path | None = None,
) -> None:
    """Store the LLM-generated caption and the data hash it was generated from."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE vizgrams SET caption=?, caption_hash=?, updated_at=? WHERE id=?",
            (caption, caption_hash, _now(), vizgram_id),
        )


def update_data(
    vizgram_id: str,
    *,
    data_hash: str,
    last_data_updated: str,
    significance_score: float,
    data_snapshot: list | None = None,
    db_path: Path | None = None,
) -> None:
    """Update live data metadata after a scheduled re-run."""
    with _connect(db_path) as conn:
        conn.execute(
            """UPDATE vizgrams
               SET data_hash=?, last_data_updated=?, significance_score=?,
                   data_snapshot=?, updated_at=?
               WHERE id=?""",
            (
                data_hash,
                last_data_updated,
                significance_score,
                json.dumps(data_snapshot) if data_snapshot is not None else None,
                _now(),
                vizgram_id,
            ),
        )


def delete_vizgram(vizgram_id: str, db_path: Path | None = None) -> None:
    """Soft-delete a vizgram."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE vizgrams SET is_deleted=1, updated_at=? WHERE id=?",
            (_now(), vizgram_id),
        )


# ---------------------------------------------------------------------------
# Engagements (likes / saves)
# ---------------------------------------------------------------------------

def add_engagement(
    vizgram_id: str,
    user_id: str,
    engagement_type: str,
    db_path: Path | None = None,
) -> bool:
    """Record a like or save. Returns True if created, False if already exists."""
    if engagement_type not in ("like", "save"):
        raise ValueError(f"Invalid engagement type: {engagement_type!r}")
    try:
        with _connect(db_path) as conn:
            conn.execute(
                """INSERT INTO vizgram_engagements (id, vizgram_id, user_id, type, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (str(uuid4()), vizgram_id, user_id, engagement_type, _now()),
            )
        return True
    except sqlite3.IntegrityError:
        return False  # already exists — UNIQUE constraint


def remove_engagement(
    vizgram_id: str,
    user_id: str,
    engagement_type: str,
    db_path: Path | None = None,
) -> None:
    """Remove a like or save."""
    with _connect(db_path) as conn:
        conn.execute(
            "DELETE FROM vizgram_engagements WHERE vizgram_id=? AND user_id=? AND type=?",
            (vizgram_id, user_id, engagement_type),
        )


def get_viewer_engagement(
    vizgram_id: str,
    user_id: str,
    db_path: Path | None = None,
) -> dict[str, bool]:
    """Return {'liked': bool, 'saved': bool} for a specific viewer."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT type FROM vizgram_engagements WHERE vizgram_id=? AND user_id=?",
            (vizgram_id, user_id),
        ).fetchall()
    types = {r["type"] for r in rows}
    return {"liked": "like" in types, "saved": "save" in types}


def get_engagement_counts(
    vizgram_id: str,
    db_path: Path | None = None,
) -> dict[str, int]:
    """Return {'like': N, 'save': N} for a vizgram."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT type, COUNT(*) as n FROM vizgram_engagements WHERE vizgram_id=? GROUP BY type",
            (vizgram_id,),
        ).fetchall()
    counts: dict[str, int] = {"like": 0, "save": 0}
    for row in rows:
        counts[row["type"]] = row["n"]
    return counts


# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

def load_registry_from_db(db_path: Path | None = None) -> dict[str, dict]:
    """Return all models from the DB as {id: metadata_dict}. Empty dict if none."""
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM models").fetchall()
    result = {}
    for row in rows:
        d = dict(row)
        result[d["id"]] = {
            "display_name": d["display_name"],
            "description": d["description"] or "",
            "owner": d["owner"] or "",
            "created_at": d["created_at"],
            "status": d["status"],
            "tags": json.loads(d["tags"] or "[]"),
        }
    return result


def upsert_model_in_db(model_id: str, fields: dict, db_path: Path | None = None) -> None:
    """Upsert model metadata. Never overwrites access_rules — use set_model_access_rules for that."""
    now = _now()
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO models (id, display_name, description, owner, created_at, updated_at, status, tags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   display_name = excluded.display_name,
                   description  = excluded.description,
                   owner        = excluded.owner,
                   updated_at   = excluded.updated_at,
                   status       = excluded.status,
                   tags         = excluded.tags""",
            (
                model_id,
                fields.get("display_name", model_id),
                fields.get("description", ""),
                fields.get("owner", ""),
                fields.get("created_at", now),
                now,
                fields.get("status", "experimental"),
                json.dumps(fields.get("tags", [])),
            ),
        )


def delete_model_from_db(model_id: str, db_path: Path | None = None) -> None:
    """Remove a model from the registry table."""
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM models WHERE id=?", (model_id,))


def get_model_access_rules(model_id: str, db_path: Path | None = None) -> list[dict] | None:
    """Return the access_rules JSON for a model, or None if not set in DB (fall back to config.yaml)."""
    with _connect(db_path) as conn:
        row = conn.execute("SELECT access_rules FROM models WHERE id=?", (model_id,)).fetchone()
    if not row or row["access_rules"] is None:
        return None
    return json.loads(row["access_rules"])


def set_model_access_rules(model_id: str, rules: list[dict] | None, db_path: Path | None = None) -> None:
    """Set (or clear) the access_rules for a model. Pass None to revert to config.yaml fallback."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE models SET access_rules=?, updated_at=? WHERE id=?",
            (json.dumps(rules) if rules is not None else None, _now(), model_id),
        )


def seed_model_registry(models_dir: Path, db_path: Path | None = None) -> int:
    """Seed the models table from registry.yaml + config.yaml access blocks.

    Only inserts models not already present in the DB — fully idempotent.
    Call this on startup; remove once all deployments have migrated.
    Returns the number of newly inserted models.
    """
    import yaml

    registry_path = models_dir / "registry.yaml"
    if not registry_path.exists():
        return 0

    with open(registry_path) as f:
        data = yaml.safe_load(f) or {}
    registry: dict[str, dict] = data.get("models", {})
    if not registry:
        return 0

    seeded = 0
    now = _now()
    for model_id, meta in registry.items():
        with _connect(db_path) as conn:
            existing = conn.execute("SELECT id FROM models WHERE id=?", (model_id,)).fetchone()
            if existing:
                continue

        # Read access block from config.yaml if present
        access_rules = None
        config_path = models_dir / model_id / "config.yaml"
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}
            access = config.get("access")
            if access:
                access_rules = access

        with _connect(db_path) as conn:
            conn.execute(
                """INSERT INTO models
                       (id, display_name, description, owner, created_at, updated_at, status, tags, access_rules)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO NOTHING""",
                (
                    model_id,
                    meta.get("display_name", model_id),
                    meta.get("description", ""),
                    meta.get("owner", ""),
                    meta.get("created_at", now),
                    now,
                    meta.get("status", "experimental"),
                    json.dumps(meta.get("tags", [])),
                    json.dumps(access_rules) if access_rules is not None else None,
                ),
            )
        seeded += 1

    return seeded


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    for json_field in ("slice_config", "chart_config", "tags"):
        if d.get(json_field) is not None:
            d[json_field] = json.loads(d[json_field])
    if d.get("data_snapshot") is not None:
        d["data_snapshot"] = json.loads(d["data_snapshot"])
    for bool_field in ("live", "is_deleted", "viewer_liked", "viewer_saved"):
        if bool_field in d:
            d[bool_field] = bool(d[bool_field])
    return d
