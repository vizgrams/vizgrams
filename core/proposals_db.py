# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Change-proposal storage (Epic 26 VG-294).

Members of a model propose changes to governed surfaces (ontology rows,
mappers, extractors) by inserting a row here; owners + admins approve
or reject by transitioning the status. The table lives in the central
``api.db`` alongside artifact_versions (schema in ``core.metadata_db``).

Conflict policy (decided 2026-05-27): two pending proposals on the same
artifact both stay open. Approving one marks the other ``superseded``
with a back-link via ``superseded_by`` so the audit trail records both
the winning and losing proposals plus the relationship between them.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from core.metadata_db import _connect

logger = logging.getLogger(__name__)

# Status enum.  Pending → {approved | rejected | superseded}.  Terminal
# statuses are not transitioned out of — once decided, a proposal is
# immutable for audit purposes.
PROPOSAL_STATUSES = frozenset({"pending", "approved", "rejected", "superseded"})

# Artifact kinds we accept proposals for. Charts + computed features
# don't go through this flow (members write directly under their own
# ownership stamp) — they're absent here on purpose.
PROPOSAL_KINDS = frozenset({
    "attribute", "relation", "computed", "mapper", "extractor", "sub_group",
})


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _row_to_dict(row) -> dict:
    """Convert a sqlite Row (or any dict-like with notified_to JSON) into
    a typed payload, parsing ``notified_to`` back into a Python list."""
    d = dict(row)
    raw = d.get("notified_to") or "[]"
    try:
        d["notified_to"] = json.loads(raw)
    except json.JSONDecodeError:
        d["notified_to"] = []
    return d


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


def create_proposal(
    *,
    model_id: str,
    artifact_kind: str,
    artifact_name: str,
    proposed_by: str,
    reason: str,
    entity_name: str | None = None,
    before_yaml: str | None = None,
    after_yaml: str | None = None,
    notified_to: list[str] | None = None,
    db_path: Path | None = None,
) -> str:
    """Insert a new pending proposal. Returns its id.

    Raises ``ValueError`` for bad inputs (unknown kind, empty reason)
    so callers can surface 400s rather than 500s."""
    if artifact_kind not in PROPOSAL_KINDS:
        raise ValueError(f"Unknown artifact_kind: {artifact_kind!r}")
    if not reason.strip():
        raise ValueError("reason is required")
    if not proposed_by:
        raise ValueError("proposed_by is required")
    pid = str(uuid.uuid4())
    payload = (
        pid, model_id, entity_name, artifact_kind, artifact_name,
        proposed_by, reason.strip(),
        before_yaml, after_yaml,
        "pending",
        json.dumps(notified_to or []),
        _now_iso(),
    )
    with _connect(Path("."), db_path) as conn:
        conn.execute(
            """INSERT INTO proposals (
                id, model_id, entity_name, artifact_kind, artifact_name,
                proposed_by, reason,
                before_yaml, after_yaml,
                status, notified_to, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
        )
    return pid


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def get_proposal(proposal_id: str, *, db_path: Path | None = None) -> dict | None:
    """Return a single proposal, or None if not found."""
    with _connect(Path("."), db_path) as conn:
        row = conn.execute(
            "SELECT * FROM proposals WHERE id=?", (proposal_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def list_proposals(
    *,
    model_id: str,
    entity_name: str | None = None,
    status: str | None = None,
    artifact_kind: str | None = None,
    artifact_name: str | None = None,
    db_path: Path | None = None,
) -> list[dict]:
    """Return matching proposals, newest first.

    Filters are AND'd: pass any combination of ``entity_name`` /
    ``status`` / ``artifact_kind`` / ``artifact_name`` to narrow.
    """
    clauses = ["model_id = ?"]
    args: list[Any] = [model_id]
    if entity_name is not None:
        clauses.append("entity_name = ?")
        args.append(entity_name)
    if status is not None:
        if status not in PROPOSAL_STATUSES:
            raise ValueError(f"Unknown status: {status!r}")
        clauses.append("status = ?")
        args.append(status)
    if artifact_kind is not None:
        clauses.append("artifact_kind = ?")
        args.append(artifact_kind)
    if artifact_name is not None:
        clauses.append("artifact_name = ?")
        args.append(artifact_name)
    sql = (
        "SELECT * FROM proposals "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC"
    )
    with _connect(Path("."), db_path) as conn:
        rows = conn.execute(sql, args).fetchall()
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Decide — approve / reject / supersede
# ---------------------------------------------------------------------------


class ProposalStateError(Exception):
    """Raised when a state transition isn't allowed (e.g. decide on
    a proposal that's already decided). Service layer surfaces as 409."""


def _assert_pending(proposal: dict, action: str) -> None:
    if proposal["status"] != "pending":
        raise ProposalStateError(
            f"Cannot {action} proposal {proposal['id']!r}: status is "
            f"{proposal['status']!r}, must be 'pending'",
        )


def approve_proposal(
    proposal_id: str,
    *,
    actor: str,
    comment: str | None = None,
    db_path: Path | None = None,
) -> dict:
    """Mark a proposal approved + supersede any other pending proposals
    on the same artifact (per the conflict policy).

    Returns the updated proposal record. Raises ``KeyError`` for unknown
    id; ``ProposalStateError`` for non-pending state."""
    existing = get_proposal(proposal_id, db_path=db_path)
    if existing is None:
        raise KeyError(f"Proposal {proposal_id!r} not found")
    _assert_pending(existing, "approve")
    now = _now_iso()
    with _connect(Path("."), db_path) as conn:
        conn.execute(
            "UPDATE proposals SET status='approved', "
            "decision_actor=?, decision_at=?, decision_comment=? "
            "WHERE id=?",
            (actor, now, comment, proposal_id),
        )
        # Supersede any other pending proposals on the same artifact.
        # Same model_id + artifact_kind + artifact_name (entity_name
        # may be null for extractor proposals, so we compare via
        # IS NOT DISTINCT FROM semantics).
        conn.execute(
            """UPDATE proposals
               SET status='superseded',
                   superseded_by=?,
                   decision_actor=?, decision_at=?,
                   decision_comment='superseded by approved proposal'
               WHERE id != ?
                 AND status='pending'
                 AND model_id=?
                 AND artifact_kind=?
                 AND artifact_name=?""",
            (proposal_id, actor, now, proposal_id,
             existing["model_id"], existing["artifact_kind"],
             existing["artifact_name"]),
        )
    return get_proposal(proposal_id, db_path=db_path)


def reject_proposal(
    proposal_id: str,
    *,
    actor: str,
    comment: str,
    db_path: Path | None = None,
) -> dict:
    """Mark a proposal rejected. Other pending proposals on the same
    artifact are left untouched — rejection doesn't decide them."""
    if not comment.strip():
        raise ValueError("comment is required when rejecting a proposal")
    existing = get_proposal(proposal_id, db_path=db_path)
    if existing is None:
        raise KeyError(f"Proposal {proposal_id!r} not found")
    _assert_pending(existing, "reject")
    now = _now_iso()
    with _connect(Path("."), db_path) as conn:
        conn.execute(
            "UPDATE proposals SET status='rejected', "
            "decision_actor=?, decision_at=?, decision_comment=? "
            "WHERE id=?",
            (actor, now, comment.strip(), proposal_id),
        )
    return get_proposal(proposal_id, db_path=db_path)
