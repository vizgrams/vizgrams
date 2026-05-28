# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core.proposals_db (Epic 26 VG-294).

DB-layer only — the API + UI bits land in VG-295 / VG-296. These pin
down: CRUD lifecycle, status transitions, the supersession rule (two
pending proposals on same artifact; approving one closes the other),
the validation guards, and the filter combinations on ``list_proposals``.
"""

from __future__ import annotations

import pytest

from core import proposals_db
from core.proposals_db import (
    PROPOSAL_KINDS,
    PROPOSAL_STATUSES,
    ProposalStateError,
    approve_proposal,
    create_proposal,
    get_proposal,
    list_proposals,
    reject_proposal,
)

# ---------------------------------------------------------------------------
# create_proposal
# ---------------------------------------------------------------------------


def test_create_returns_id_and_persists_pending_record():
    pid = create_proposal(
        model_id="demo",
        entity_name="PullRequest",
        artifact_kind="attribute",
        artifact_name="state",
        proposed_by="alice",
        reason="add a 'draft' state",
        before_yaml="state: enum<open,merged,closed>",
        after_yaml="state: enum<draft,open,merged,closed>",
        notified_to=["bob (owner)", "admins"],
    )
    p = get_proposal(pid)
    assert p is not None
    assert p["status"] == "pending"
    assert p["proposed_by"] == "alice"
    assert p["reason"] == "add a 'draft' state"
    assert p["notified_to"] == ["bob (owner)", "admins"]
    assert p["decision_actor"] is None
    assert p["superseded_by"] is None


def test_create_accepts_null_entity_for_extractor_proposals():
    """Extractor proposals are cross-entity — no entity_name."""
    pid = create_proposal(
        model_id="demo", entity_name=None,
        artifact_kind="extractor", artifact_name="github_pulls",
        proposed_by="alice", reason="bump pagination",
    )
    p = get_proposal(pid)
    assert p["entity_name"] is None
    assert p["artifact_kind"] == "extractor"


def test_create_strips_whitespace_around_reason():
    pid = create_proposal(
        model_id="demo", artifact_kind="attribute", artifact_name="x",
        proposed_by="alice", reason="  add it  \n",
    )
    assert get_proposal(pid)["reason"] == "add it"


def test_create_rejects_unknown_artifact_kind():
    with pytest.raises(ValueError, match="Unknown artifact_kind"):
        create_proposal(
            model_id="demo", artifact_kind="not_a_kind", artifact_name="x",
            proposed_by="alice", reason="x",
        )


def test_create_rejects_empty_reason():
    with pytest.raises(ValueError, match="reason is required"):
        create_proposal(
            model_id="demo", artifact_kind="attribute", artifact_name="x",
            proposed_by="alice", reason="   ",
        )


def test_create_rejects_empty_proposed_by():
    with pytest.raises(ValueError, match="proposed_by is required"):
        create_proposal(
            model_id="demo", artifact_kind="attribute", artifact_name="x",
            proposed_by="", reason="add a thing",
        )


def test_create_defaults_notified_to_to_empty_list():
    pid = create_proposal(
        model_id="demo", artifact_kind="attribute", artifact_name="x",
        proposed_by="alice", reason="x",
    )
    assert get_proposal(pid)["notified_to"] == []


# ---------------------------------------------------------------------------
# get_proposal / list_proposals
# ---------------------------------------------------------------------------


def test_get_returns_none_for_unknown_id():
    assert get_proposal("not-a-uuid") is None


def test_list_returns_proposals_for_model_only():
    create_proposal(model_id="m1", artifact_kind="attribute", artifact_name="a",
                    proposed_by="x", reason="x")
    create_proposal(model_id="m2", artifact_kind="attribute", artifact_name="a",
                    proposed_by="x", reason="x")
    rows = list_proposals(model_id="m1")
    assert len(rows) == 1
    assert rows[0]["model_id"] == "m1"


def test_list_filters_by_entity_name():
    create_proposal(model_id="m", entity_name="A", artifact_kind="attribute",
                    artifact_name="x", proposed_by="x", reason="x")
    create_proposal(model_id="m", entity_name="B", artifact_kind="attribute",
                    artifact_name="x", proposed_by="x", reason="x")
    rows = list_proposals(model_id="m", entity_name="A")
    assert len(rows) == 1
    assert rows[0]["entity_name"] == "A"


def test_list_filters_by_status():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="x", reason="x")
    create_proposal(model_id="m", artifact_kind="attribute",
                    artifact_name="y", proposed_by="x", reason="x")
    approve_proposal(pid, actor="admin")
    pending = list_proposals(model_id="m", status="pending")
    approved = list_proposals(model_id="m", status="approved")
    assert len(pending) == 1 and pending[0]["artifact_name"] == "y"
    assert len(approved) == 1 and approved[0]["id"] == pid


def test_list_filters_by_artifact_kind_and_name():
    create_proposal(model_id="m", artifact_kind="attribute",
                    artifact_name="x", proposed_by="x", reason="x")
    create_proposal(model_id="m", artifact_kind="relation",
                    artifact_name="x", proposed_by="x", reason="x")
    rows = list_proposals(model_id="m", artifact_kind="attribute",
                          artifact_name="x")
    assert len(rows) == 1
    assert rows[0]["artifact_kind"] == "attribute"


def test_list_rejects_unknown_status_filter():
    with pytest.raises(ValueError, match="Unknown status"):
        list_proposals(model_id="m", status="weird")


def test_list_orders_newest_first():
    """Multiple proposals — most recent should sit at index 0."""
    import time
    ids = []
    for _ in range(3):
        ids.append(create_proposal(model_id="m", artifact_kind="attribute",
                                   artifact_name=f"x{_}", proposed_by="x",
                                   reason="x"))
        time.sleep(0.01)
    rows = list_proposals(model_id="m")
    assert [r["id"] for r in rows] == list(reversed(ids))


# ---------------------------------------------------------------------------
# approve_proposal — happy path + supersession
# ---------------------------------------------------------------------------


def test_approve_transitions_pending_to_approved_with_actor():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="alice", reason="x")
    out = approve_proposal(pid, actor="admin", comment="LGTM")
    assert out["status"] == "approved"
    assert out["decision_actor"] == "admin"
    assert out["decision_at"] is not None
    assert out["decision_comment"] == "LGTM"


def test_approve_supersedes_other_pending_proposals_on_same_artifact():
    """Conflict policy: approving one closes the other with a back-link."""
    p1 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="state", proposed_by="alice",
                         reason="add draft")
    p2 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="state", proposed_by="bob",
                         reason="add archived")
    approve_proposal(p1, actor="admin")
    loser = get_proposal(p2)
    assert loser["status"] == "superseded"
    assert loser["superseded_by"] == p1
    assert "superseded" in (loser["decision_comment"] or "")


def test_approve_does_not_supersede_proposals_on_different_artifacts():
    p1 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="state", proposed_by="alice", reason="x")
    p2 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="title", proposed_by="bob", reason="x")
    approve_proposal(p1, actor="admin")
    assert get_proposal(p2)["status"] == "pending"


def test_approve_does_not_supersede_across_models():
    p1 = create_proposal(model_id="m1", artifact_kind="attribute",
                         artifact_name="x", proposed_by="x", reason="x")
    p2 = create_proposal(model_id="m2", artifact_kind="attribute",
                         artifact_name="x", proposed_by="x", reason="x")
    approve_proposal(p1, actor="admin")
    assert get_proposal(p2)["status"] == "pending"


def test_approve_unknown_id_raises_key_error():
    with pytest.raises(KeyError):
        approve_proposal("not-a-uuid", actor="admin")


def test_approve_already_decided_raises_state_error():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="alice", reason="x")
    approve_proposal(pid, actor="admin")
    with pytest.raises(ProposalStateError, match="must be 'pending'"):
        approve_proposal(pid, actor="admin")


# ---------------------------------------------------------------------------
# reject_proposal
# ---------------------------------------------------------------------------


def test_reject_transitions_pending_to_rejected():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="alice", reason="x")
    out = reject_proposal(pid, actor="admin", comment="too risky")
    assert out["status"] == "rejected"
    assert out["decision_actor"] == "admin"
    assert out["decision_comment"] == "too risky"


def test_reject_requires_non_empty_comment():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="alice", reason="x")
    with pytest.raises(ValueError, match="comment is required"):
        reject_proposal(pid, actor="admin", comment="   ")


def test_reject_does_not_supersede_other_pending_proposals():
    p1 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="state", proposed_by="alice", reason="x")
    p2 = create_proposal(model_id="m", artifact_kind="attribute",
                         artifact_name="state", proposed_by="bob", reason="x")
    reject_proposal(p1, actor="admin", comment="nope")
    assert get_proposal(p2)["status"] == "pending"


def test_reject_unknown_id_raises_key_error():
    with pytest.raises(KeyError):
        reject_proposal("not-a-uuid", actor="admin", comment="x")


def test_reject_already_decided_raises_state_error():
    pid = create_proposal(model_id="m", artifact_kind="attribute",
                          artifact_name="x", proposed_by="alice", reason="x")
    reject_proposal(pid, actor="admin", comment="no")
    with pytest.raises(ProposalStateError):
        reject_proposal(pid, actor="admin", comment="really no")


# ---------------------------------------------------------------------------
# Schema sanity — enums match expected sets
# ---------------------------------------------------------------------------


def test_status_enum_covers_all_decision_paths():
    assert {"pending", "approved", "rejected", "superseded"} == PROPOSAL_STATUSES


def test_artifact_kinds_cover_governed_surfaces():
    assert {
        "attribute", "relation", "computed", "mapper", "extractor", "sub_group",
    } == PROPOSAL_KINDS


def test_module_exposes_expected_public_surface():
    """Regression — these are the names the API + UI tickets (VG-295,
    VG-296) will import."""
    expected = {
        "create_proposal", "get_proposal", "list_proposals",
        "approve_proposal", "reject_proposal",
        "ProposalStateError", "PROPOSAL_STATUSES", "PROPOSAL_KINDS",
    }
    assert expected <= set(dir(proposals_db))
