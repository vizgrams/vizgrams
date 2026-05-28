# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""In-app notifications (Epic 26 VG-295).

A proposal creates one notification per recipient in ``notified_to``
(owner + admins). When the proposal transitions out of pending, all
notifications for it get ``resolved_at`` stamped — the bell stops
showing them as unread. Schema is in ``core.metadata_db`` (single DB).

Kept deliberately small: no notification *types* beyond
``proposal_pending``, no email/Slack hooks. Those can layer on top
later by reading the same table.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

from core.metadata_db import _connect

NOTIFICATION_KIND_PROPOSAL_PENDING = "proposal_pending"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def notify_proposal_pending(
    *,
    proposal_id: str,
    recipients: list[str],
    db_path: Path | None = None,
) -> list[str]:
    """Insert one ``proposal_pending`` notification per recipient.

    ``recipients`` are user identifiers (email or UUID — the proposals
    UI displays the resolved form via the existing user-id cache).
    Returns the inserted notification ids in input order so callers
    can correlate if needed.

    Idempotent at the table level (different uuid per call), but the
    caller is responsible for not double-notifying the same user about
    the same proposal — the create endpoint enforces this by calling
    once at creation time.
    """
    if not recipients:
        return []
    now = _now_iso()
    ids: list[str] = []
    with _connect(Path("."), db_path) as conn:
        for user_id in recipients:
            nid = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO notifications
                   (id, user_id, kind, proposal_id, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (nid, user_id, NOTIFICATION_KIND_PROPOSAL_PENDING,
                 proposal_id, now),
            )
            ids.append(nid)
    return ids


def resolve_for_proposal(proposal_id: str, *, db_path: Path | None = None) -> int:
    """Mark every notification for ``proposal_id`` as resolved. Called
    when a proposal is approved / rejected / superseded. Returns the
    number of rows updated."""
    now = _now_iso()
    with _connect(Path("."), db_path) as conn:
        cur = conn.execute(
            "UPDATE notifications SET resolved_at=? "
            "WHERE proposal_id=? AND resolved_at IS NULL",
            (now, proposal_id),
        )
        return cur.rowcount or 0


def list_pending_for_user(user_id: str, *, db_path: Path | None = None) -> list[dict]:
    """Unresolved notifications for ``user_id``, newest first. Powers
    the bell. Joins back to the proposals table so the bell can show
    the artifact in one query — keeps the bell snappy on every page."""
    with _connect(Path("."), db_path) as conn:
        rows = conn.execute(
            """SELECT n.id, n.kind, n.proposal_id, n.created_at,
                      p.entity_name, p.artifact_kind, p.artifact_name,
                      p.proposed_by, p.reason, p.model_id
               FROM notifications n
               LEFT JOIN proposals p ON p.id = n.proposal_id
               WHERE n.user_id = ? AND n.resolved_at IS NULL
               ORDER BY n.created_at DESC""",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def count_pending_for_user(user_id: str, *, db_path: Path | None = None) -> int:
    """Cheap bell-badge query — count only, no row payload."""
    with _connect(Path("."), db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM notifications "
            "WHERE user_id=? AND resolved_at IS NULL",
            (user_id,),
        ).fetchone()
    return int(row["n"] if row else 0)
