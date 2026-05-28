# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Propose-change workflow (Epic 26 VG-295).

Wraps the DB layer with the business rules the routes care about:
- creating a proposal resolves who to notify (owner + admins) and
  inserts notifications atomically
- approving / rejecting checks the actor is the owner or an admin
- decision events resolve the related notifications so the bell
  empties out

"Apply atomically" — writing the proposed YAML into the artifact on
approval — is deferred to VG-297, which has the per-artifact write
plumbing it needs. This service records the decision; the actual
mutation lands when the pipeline editing PR wires the writers up.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from api.services import entity_service
from core import notifications_db, proposals_db

logger = logging.getLogger(__name__)


# Sentinel used in the notified_to list to mean "all platform admins".
# Resolved at notification-fan-out time from VZ_SYSTEM_ADMINS rather
# than enumerated at creation time — admins change less often than
# proposals do, and we want late-joining admins to still see open
# proposals.
NOTIFY_ADMINS = "admins"


def _admin_emails() -> list[str]:
    """Resolve the literal list of admin emails from ``VZ_SYSTEM_ADMINS``.

    Catch-all (``*``) is intentionally NOT expanded — a global notify
    would be useless. Domain wildcards (``*@example.com``) are also
    skipped because we don't have a user directory to enumerate them.
    Only literal emails generate notifications."""
    raw = os.environ.get("VZ_SYSTEM_ADMINS", "")
    out = []
    for pat in raw.split(","):
        pat = pat.strip()
        if pat and "*" not in pat:
            out.append(pat)
    dev = os.environ.get("DEV_USER")
    if dev and dev not in out:
        out.append(dev)
    return out


def _resolve_recipients(
    model_dir: Path,
    entity_name: str | None,
    artifact_kind: str,
    artifact_name: str,
) -> list[str]:
    """Determine who to notify on a new proposal.

    For governed *ontology* rows (attribute / relation / computed) on
    a specific entity, we look up the last-touched-by of that row via
    ``entity_service.resolve_row_owner`` and notify them + admins.
    For mappers / extractors / sub_groups (admin-only domains today),
    we notify admins only — they're the only ones who can decide."""
    recipients: list[str] = []
    if entity_name and artifact_kind in ("attribute", "relation", "computed"):
        owner = entity_service.resolve_row_owner(
            model_dir, entity_name, artifact_kind, artifact_name,
        )
        if owner:
            recipients.append(owner)
    recipients.extend(_admin_emails())
    # De-dupe while preserving order (owner first, then admins).
    seen: set[str] = set()
    result: list[str] = []
    for r in recipients:
        if r not in seen:
            seen.add(r)
            result.append(r)
    return result


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


def create(
    *,
    model_dir: Path,
    proposed_by: str,
    artifact_kind: str,
    artifact_name: str,
    reason: str,
    entity_name: str | None = None,
    before_yaml: str | None = None,
    after_yaml: str | None = None,
) -> dict:
    """Create a proposal + fan out notifications. Returns the new
    proposal record."""
    recipients = _resolve_recipients(
        model_dir, entity_name, artifact_kind, artifact_name,
    )
    pid = proposals_db.create_proposal(
        model_id=Path(model_dir).name,
        entity_name=entity_name,
        artifact_kind=artifact_kind,
        artifact_name=artifact_name,
        proposed_by=proposed_by,
        reason=reason,
        before_yaml=before_yaml,
        after_yaml=after_yaml,
        notified_to=recipients,
    )
    notifications_db.notify_proposal_pending(
        proposal_id=pid, recipients=recipients,
    )
    return proposals_db.get_proposal(pid)


# ---------------------------------------------------------------------------
# Authorization helpers
# ---------------------------------------------------------------------------


class ProposalAuthError(Exception):
    """Raised when the caller lacks permission to decide a proposal.
    Service uses this; routes translate to 403."""


def _resolve_decider(
    model_dir: Path,
    proposal: dict,
    actor: str,
    is_admin: bool,
) -> None:
    """Raise ``ProposalAuthError`` if ``actor`` cannot decide this
    proposal. Admins can decide everything; non-admins can only decide
    proposals on artifacts they own.

    For ontology rows (attribute / relation / computed): owner =
    last-touched-by from the entity's version history. For mappers /
    extractors / sub_groups: there is no per-row "owner" yet, so only
    admins can decide."""
    if is_admin:
        return
    kind = proposal["artifact_kind"]
    entity = proposal.get("entity_name")
    if entity and kind in ("attribute", "relation", "computed"):
        owner = entity_service.resolve_row_owner(
            model_dir, entity, kind, proposal["artifact_name"],
        )
        if owner == actor:
            return
    raise ProposalAuthError(
        f"User {actor!r} cannot decide proposals of kind {kind!r} on this artifact",
    )


# ---------------------------------------------------------------------------
# Decide
# ---------------------------------------------------------------------------


def approve(
    *,
    model_dir: Path,
    proposal_id: str,
    actor: str,
    is_admin: bool,
    comment: str | None = None,
) -> dict:
    """Approve a proposal. Authorization: admin OR (owner-of-the-row).

    Side effects: marks any other pending proposals on the same
    artifact as ``superseded``, and resolves the related notifications
    so the bell empties out for everyone who was notified."""
    existing = proposals_db.get_proposal(proposal_id)
    if existing is None:
        raise KeyError(f"Proposal {proposal_id!r} not found")
    _resolve_decider(model_dir, existing, actor, is_admin)
    out = proposals_db.approve_proposal(
        proposal_id, actor=actor, comment=comment,
    )
    # Notifications for this proposal AND for the superseded losers
    # should all clear. Find every proposal that points at this one
    # via superseded_by and resolve their notifications too.
    notifications_db.resolve_for_proposal(proposal_id)
    for loser in proposals_db.list_proposals(
        model_id=existing["model_id"],
        artifact_kind=existing["artifact_kind"],
        artifact_name=existing["artifact_name"],
        status="superseded",
    ):
        if loser.get("superseded_by") == proposal_id:
            notifications_db.resolve_for_proposal(loser["id"])
    return out


def reject(
    *,
    model_dir: Path,
    proposal_id: str,
    actor: str,
    is_admin: bool,
    comment: str,
) -> dict:
    """Reject a proposal. Same authorization as approve. Other pending
    proposals on the same artifact stay pending — rejection doesn't
    decide them."""
    existing = proposals_db.get_proposal(proposal_id)
    if existing is None:
        raise KeyError(f"Proposal {proposal_id!r} not found")
    _resolve_decider(model_dir, existing, actor, is_admin)
    out = proposals_db.reject_proposal(
        proposal_id, actor=actor, comment=comment,
    )
    notifications_db.resolve_for_proposal(proposal_id)
    return out
