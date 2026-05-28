# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the propose-change service layer (Epic 26 VG-295).

Two distinct concerns covered here:
- Recipient resolution — who gets notified when a proposal is created
  (owner of the ontology row + admins from ``VZ_SYSTEM_ADMINS``).
- Authorization — admin OR owner can decide; everyone else gets 403.

The DB-level lifecycle (status transitions, supersession) is covered
in ``test_proposals_db.py`` already; this file focuses on the workflow
wrapping that the route layer relies on.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from api.services import proposals_service
from api.services.proposals_service import (
    NOTIFY_ADMINS,
    ProposalAuthError,
    _admin_emails,
    _resolve_recipients,
)
from core import notifications_db, proposals_db
from tests.conftest import seed_artifact

_ENTITY_YAML_V1 = """\
entity: Widget
identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
"""

_ENTITY_YAML_V2 = """\
entity: Widget
identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
  score:
    type: INTEGER
    semantic: MEASURE
"""


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


@pytest.fixture
def model_with_row_owners(model_dir):
    """Two ontology row owners across two entity versions:
    - alice created the entity (owns `name` + `widget_key`)
    - bob added `score` in v2 (owns that row)"""
    from core import metadata_db
    metadata_db.record_version(model_dir, "entity", "widget", _ENTITY_YAML_V1,
                               user_id="alice", via="api")
    metadata_db.record_version(model_dir, "entity", "widget", _ENTITY_YAML_V2,
                               user_id="bob", via="api")
    return model_dir


# ---------------------------------------------------------------------------
# _admin_emails — resolves VZ_SYSTEM_ADMINS literal entries
# ---------------------------------------------------------------------------


def test_admin_emails_returns_literal_addresses(monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "alice@x.com,bob@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    assert _admin_emails() == ["alice@x.com", "bob@x.com"]


def test_admin_emails_skips_catch_all_and_wildcards(monkeypatch):
    """Domain wildcards / catch-all can't enumerate users — skipped."""
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "*,*@example.com,alice@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    assert _admin_emails() == ["alice@x.com"]


def test_admin_emails_includes_dev_user(monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "alice@x.com")
    monkeypatch.setenv("DEV_USER", "dev@local")
    assert "dev@local" in _admin_emails()
    assert "alice@x.com" in _admin_emails()


# ---------------------------------------------------------------------------
# _resolve_recipients — owner + admins
# ---------------------------------------------------------------------------


def test_resolve_recipients_for_attribute_includes_owner_and_admins(
    model_with_row_owners, monkeypatch,
):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    recipients = _resolve_recipients(
        model_with_row_owners, "Widget", "attribute", "score",
    )
    assert "bob" in recipients      # owner of `score`
    assert "admin@x.com" in recipients
    # Owner first, admins after — UI banner respects the order.
    assert recipients.index("bob") < recipients.index("admin@x.com")


def test_resolve_recipients_dedupes_when_owner_is_also_admin(
    model_with_row_owners, monkeypatch,
):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "bob,admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    recipients = _resolve_recipients(
        model_with_row_owners, "Widget", "attribute", "score",
    )
    # bob appears exactly once even though they're both the owner AND
    # listed as an admin.
    assert recipients.count("bob") == 1


def test_resolve_recipients_for_extractor_skips_owner_lookup(monkeypatch):
    """Extractors aren't per-row ontology entries — recipients are just admins."""
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    recipients = _resolve_recipients(
        Path("/tmp/anywhere"), entity_name=None,
        artifact_kind="extractor", artifact_name="github_pulls",
    )
    assert recipients == ["admin@x.com"]


# ---------------------------------------------------------------------------
# create — fans out notifications + records the proposal
# ---------------------------------------------------------------------------


def test_create_inserts_proposal_with_pending_status(model_with_row_owners, monkeypatch):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    p = proposals_service.create(
        model_dir=model_with_row_owners,
        proposed_by="cathy",
        artifact_kind="attribute",
        artifact_name="score",
        reason="change INTEGER → FLOAT",
        entity_name="Widget",
    )
    assert p["status"] == "pending"
    assert p["proposed_by"] == "cathy"
    assert "bob" in p["notified_to"]            # owner
    assert "admin@x.com" in p["notified_to"]    # admin


def test_create_creates_a_notification_per_recipient(
    model_with_row_owners, monkeypatch,
):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    p = proposals_service.create(
        model_dir=model_with_row_owners,
        proposed_by="cathy",
        artifact_kind="attribute",
        artifact_name="score",
        reason="x",
        entity_name="Widget",
    )
    # bob's bell has one item; admin@x.com's bell has one item.
    assert notifications_db.count_pending_for_user("bob") == 1
    assert notifications_db.count_pending_for_user("admin@x.com") == 1
    # Both pointing at the same proposal.
    for u in ("bob", "admin@x.com"):
        rows = notifications_db.list_pending_for_user(u)
        assert rows[0]["proposal_id"] == p["id"]


# ---------------------------------------------------------------------------
# Authorization — admin OR owner can decide
# ---------------------------------------------------------------------------


def test_admin_can_approve_anything(model_with_row_owners):
    pid = proposals_db.create_proposal(
        model_id=model_with_row_owners.name,
        entity_name="Widget", artifact_kind="attribute",
        artifact_name="score", proposed_by="cathy", reason="x",
    )
    out = proposals_service.approve(
        model_dir=model_with_row_owners,
        proposal_id=pid, actor="random@x.com", is_admin=True,
    )
    assert out["status"] == "approved"


def test_row_owner_can_approve_proposal_on_their_row(model_with_row_owners):
    """bob owns `score` (last-touched-by). bob can approve proposals on it
    even without admin."""
    pid = proposals_db.create_proposal(
        model_id=model_with_row_owners.name,
        entity_name="Widget", artifact_kind="attribute",
        artifact_name="score", proposed_by="cathy", reason="x",
    )
    out = proposals_service.approve(
        model_dir=model_with_row_owners,
        proposal_id=pid, actor="bob", is_admin=False,
    )
    assert out["status"] == "approved"
    assert out["decision_actor"] == "bob"


def test_non_owner_non_admin_cannot_approve(model_with_row_owners):
    pid = proposals_db.create_proposal(
        model_id=model_with_row_owners.name,
        entity_name="Widget", artifact_kind="attribute",
        artifact_name="score", proposed_by="cathy", reason="x",
    )
    with pytest.raises(ProposalAuthError):
        proposals_service.approve(
            model_dir=model_with_row_owners,
            proposal_id=pid, actor="random@x.com", is_admin=False,
        )


def test_non_admin_cannot_decide_extractor_proposal(model_with_row_owners):
    """Extractor proposals have no per-row owner; only admins can decide."""
    pid = proposals_db.create_proposal(
        model_id=model_with_row_owners.name,
        artifact_kind="extractor", artifact_name="github_pulls",
        proposed_by="cathy", reason="x",
    )
    with pytest.raises(ProposalAuthError):
        proposals_service.approve(
            model_dir=model_with_row_owners,
            proposal_id=pid, actor="bob", is_admin=False,
        )


def test_owner_can_reject_proposal_on_their_row(model_with_row_owners):
    pid = proposals_db.create_proposal(
        model_id=model_with_row_owners.name,
        entity_name="Widget", artifact_kind="attribute",
        artifact_name="score", proposed_by="cathy", reason="x",
    )
    out = proposals_service.reject(
        model_dir=model_with_row_owners,
        proposal_id=pid, actor="bob", is_admin=False, comment="not now",
    )
    assert out["status"] == "rejected"
    assert out["decision_comment"] == "not now"


def test_approve_unknown_id_raises_key_error(model_with_row_owners):
    with pytest.raises(KeyError):
        proposals_service.approve(
            model_dir=model_with_row_owners,
            proposal_id="not-a-uuid", actor="admin", is_admin=True,
        )


# ---------------------------------------------------------------------------
# Decision resolves notifications — for this proposal AND for losers it
# supersedes
# ---------------------------------------------------------------------------


def test_approve_resolves_notifications_for_this_proposal(
    model_with_row_owners, monkeypatch,
):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    p = proposals_service.create(
        model_dir=model_with_row_owners, proposed_by="cathy",
        artifact_kind="attribute", artifact_name="score",
        reason="x", entity_name="Widget",
    )
    assert notifications_db.count_pending_for_user("bob") == 1
    proposals_service.approve(
        model_dir=model_with_row_owners,
        proposal_id=p["id"], actor="admin@x.com", is_admin=True,
    )
    assert notifications_db.count_pending_for_user("bob") == 0


def test_approve_clears_notifications_for_superseded_losers(
    model_with_row_owners, monkeypatch,
):
    """When approving p1 supersedes p2, p2's notifications must also
    clear — otherwise the bell would keep nagging about a closed
    proposal."""
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    p1 = proposals_service.create(
        model_dir=model_with_row_owners, proposed_by="cathy",
        artifact_kind="attribute", artifact_name="score",
        reason="change to FLOAT", entity_name="Widget",
    )
    p2 = proposals_service.create(
        model_dir=model_with_row_owners, proposed_by="daniel",
        artifact_kind="attribute", artifact_name="score",
        reason="change to LONG", entity_name="Widget",
    )
    # Both proposals notified bob — he has 2 pending bell items.
    assert notifications_db.count_pending_for_user("bob") == 2
    proposals_service.approve(
        model_dir=model_with_row_owners,
        proposal_id=p1["id"], actor="admin@x.com", is_admin=True,
    )
    # p1 approved, p2 superseded → both notifications cleared.
    assert notifications_db.count_pending_for_user("bob") == 0
    assert proposals_db.get_proposal(p2["id"])["status"] == "superseded"


def test_reject_resolves_notifications_for_this_proposal_only(
    model_with_row_owners, monkeypatch,
):
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@x.com")
    monkeypatch.delenv("DEV_USER", raising=False)
    p1 = proposals_service.create(
        model_dir=model_with_row_owners, proposed_by="cathy",
        artifact_kind="attribute", artifact_name="score", reason="x",
        entity_name="Widget",
    )
    p2 = proposals_service.create(
        model_dir=model_with_row_owners, proposed_by="daniel",
        artifact_kind="attribute", artifact_name="score", reason="y",
        entity_name="Widget",
    )
    proposals_service.reject(
        model_dir=model_with_row_owners,
        proposal_id=p1["id"], actor="admin@x.com", is_admin=True,
        comment="not yet",
    )
    # p1's notifications resolved; p2's stay pending.
    pending = notifications_db.list_pending_for_user("bob")
    assert [n["proposal_id"] for n in pending] == [p2["id"]]


# Suppress lint warning about an unused import — kept available as a
# named constant for the route layer / future tickets.
_ = (NOTIFY_ADMINS, seed_artifact)
