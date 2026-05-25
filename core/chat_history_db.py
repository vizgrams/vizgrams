# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Chat session + turn persistence (Epic 25 VG-280).

Stores conversation history in the shared ``api.db`` so users can
resume past sessions, the publish flow can link saved artifacts back
to the originating turn (VG-283), and the eval feedback loop has a
place to attach thumbs-up/down (VG-267).

Mirrors the shape of ``core/vizgrams_db.py``: schema + thin CRUD
helpers here, no business logic. The chat module (``api/services/chat/``)
consumes these helpers for orchestrator wiring + API responses.

Owner-scoped throughout — every read takes the viewer's ``user_id``
so the API surface can never accidentally surface another user's
session.
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
CREATE TABLE IF NOT EXISTS chat_sessions (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    model_id   TEXT NOT NULL,
    title      TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    ended_at   TEXT
);

CREATE INDEX IF NOT EXISTS ix_chat_sessions_user
    ON chat_sessions (user_id, ended_at, updated_at DESC);

CREATE TABLE IF NOT EXISTS chat_turns (
    id                  TEXT PRIMARY KEY,
    session_id          TEXT NOT NULL REFERENCES chat_sessions(id),
    ord                 INTEGER NOT NULL,
    role                TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
    content             TEXT,
    -- Full ChatResponse JSON for assistant turns (saved_view OR inline_view
    -- + diagnostics). The UI re-hydrates the chat from this directly.
    response_json       TEXT,
    -- VG-283: JSON array of {kind, name} pointing at artifacts the
    -- chat publish flow created from this turn.
    saved_artifact_ids  TEXT,
    -- VG-267: feedback for the eval loop. JSON {rating, reason?}.
    feedback            TEXT,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_chat_turns_session
    ON chat_turns (session_id, ord);
"""


# ---------------------------------------------------------------------------
# Connection helper — shares api.db with metadata_db + vizgrams_db
# ---------------------------------------------------------------------------


def _get_db_path(db_path: Path | None = None) -> Path:
    from core.metadata_db import get_api_db_path
    return get_api_db_path(db_path)


@contextmanager
def _connect(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    path = _get_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.executescript(_DDL)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _now() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


def create_session(
    *,
    user_id: str,
    model_id: str,
    title: str | None = None,
    db_path: Path | None = None,
) -> str:
    """Insert a new session row. Returns the generated id."""
    session_id = str(uuid4())
    now = _now()
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO chat_sessions
               (id, user_id, model_id, title, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (session_id, user_id, model_id, title, now, now),
        )
    return session_id


def get_session(
    session_id: str,
    *,
    user_id: str,
    db_path: Path | None = None,
) -> dict | None:
    """Return a session by id, or None if not found / not owned by user.

    Owner-scoped — passing the wrong user_id returns None (treat as
    not-found rather than 403 to avoid existence leaks).
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM chat_sessions WHERE id=? AND user_id=?",
            (session_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def list_sessions_for_user(
    *,
    user_id: str,
    model_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    include_ended: bool = False,
    db_path: Path | None = None,
) -> list[dict]:
    """List sessions owned by ``user_id``, newest-updated first.

    ``model_id`` filters to one model (the common UI case — the sidebar
    on /chat shows just this model's history). ``include_ended`` is
    False by default; ended sessions are hidden from the main listing
    but accessible via direct id lookup.
    """
    clauses = ["user_id=?"]
    params: list = [user_id]
    if model_id is not None:
        clauses.append("model_id=?")
        params.append(model_id)
    if not include_ended:
        clauses.append("ended_at IS NULL")
    sql = (
        "SELECT * FROM chat_sessions "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def update_session_title(
    session_id: str,
    *,
    user_id: str,
    title: str,
    db_path: Path | None = None,
) -> bool:
    """Rename a session. Returns True if a row was updated.

    Owner-scoped — wrong user_id is a no-op.
    """
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE chat_sessions SET title=?, updated_at=? WHERE id=? AND user_id=?",
            (title, _now(), session_id, user_id),
        )
        return cur.rowcount > 0


def end_session(
    session_id: str,
    *,
    user_id: str,
    db_path: Path | None = None,
) -> bool:
    """Mark a session as ended (soft delete). Returns True if updated.

    The session + its turns stay in the DB — useful for analytics +
    the eval feedback loop. ``list_sessions_for_user`` hides ended
    sessions by default.
    """
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE chat_sessions SET ended_at=? WHERE id=? AND user_id=? AND ended_at IS NULL",
            (_now(), session_id, user_id),
        )
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Turns
# ---------------------------------------------------------------------------


def append_turn(
    *,
    session_id: str,
    role: str,
    content: str | None = None,
    response: dict | None = None,
    saved_artifact_ids: list | None = None,
    feedback: dict | None = None,
    db_path: Path | None = None,
) -> str:
    """Insert a new turn at the end of ``session_id``. Returns the turn id.

    ``ord`` is computed as ``max(ord) + 1`` within the session — caller
    doesn't pass it. Bumps the session's ``updated_at`` so list-sessions
    keeps the just-touched session at the top.
    """
    turn_id = str(uuid4())
    now = _now()
    with _connect(db_path) as conn:
        next_ord = conn.execute(
            "SELECT COALESCE(MAX(ord), -1) + 1 FROM chat_turns WHERE session_id=?",
            (session_id,),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO chat_turns
               (id, session_id, ord, role, content, response_json,
                saved_artifact_ids, feedback, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                turn_id, session_id, next_ord, role, content,
                json.dumps(response) if response is not None else None,
                json.dumps(saved_artifact_ids) if saved_artifact_ids is not None else None,
                json.dumps(feedback) if feedback is not None else None,
                now,
            ),
        )
        conn.execute(
            "UPDATE chat_sessions SET updated_at=? WHERE id=?",
            (now, session_id),
        )
    return turn_id


def list_turns_for_session(
    session_id: str,
    *,
    user_id: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Return the full transcript newest-first... actually oldest-first
    (the natural display order).

    Owner-scoped via the session lookup — passing the wrong user_id for
    a session returns []. (We re-check ownership here instead of
    trusting the caller — defence in depth for an endpoint that takes
    a session_id from a URL param.)
    """
    if get_session(session_id, user_id=user_id, db_path=db_path) is None:
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM chat_turns WHERE session_id=? ORDER BY ord ASC",
            (session_id,),
        ).fetchall()
    return [_row_to_turn_dict(r) for r in rows]


def attach_saved_artifacts(
    turn_id: str,
    *,
    artifacts: list[dict],
    db_path: Path | None = None,
) -> None:
    """Record artifacts that the chat publish flow created from this turn.

    ``artifacts`` is a list of ``{kind, name}`` dicts (e.g.
    ``[{"kind": "view", "name": "dora_clt_by_team_v2"}]``).
    Merges with whatever's already stored — used by the publish flow
    to flag "this turn produced these artifacts" without overwriting
    any prior attribution.
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT saved_artifact_ids FROM chat_turns WHERE id=?", (turn_id,),
        ).fetchone()
        if row is None:
            return
        existing = json.loads(row["saved_artifact_ids"]) if row["saved_artifact_ids"] else []
        merged = list(existing) + list(artifacts)
        conn.execute(
            "UPDATE chat_turns SET saved_artifact_ids=? WHERE id=?",
            (json.dumps(merged), turn_id),
        )


def set_turn_feedback(
    turn_id: str,
    *,
    feedback: dict,
    db_path: Path | None = None,
) -> None:
    """Set the feedback blob for a turn (VG-267). Overwrites any prior value."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE chat_turns SET feedback=? WHERE id=?",
            (json.dumps(feedback), turn_id),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_turn_dict(row: sqlite3.Row) -> dict:
    """Deserialise the JSON-blob columns on a turn row."""
    d = dict(row)
    for json_field in ("response_json", "saved_artifact_ids", "feedback"):
        if d.get(json_field) is not None:
            d[json_field] = json.loads(d[json_field])
    return d
