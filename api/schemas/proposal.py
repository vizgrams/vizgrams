# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Pydantic schemas for the propose-change endpoints (Epic 26 VG-295)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProposalCreate(BaseModel):
    artifact_kind: str = Field(
        ...,
        description=(
            "Kind of artifact being proposed. Must be one of: attribute, "
            "relation, computed, mapper, extractor, sub_group."
        ),
    )
    artifact_name: str
    reason: str = Field(..., min_length=1, max_length=2000)
    entity_name: str | None = None  # null for cross-entity (extractor) proposals
    before_yaml: str | None = None
    after_yaml: str | None = None


class ProposalDecision(BaseModel):
    comment: str | None = Field(default=None, max_length=2000)


class ProposalRejection(BaseModel):
    comment: str = Field(..., min_length=1, max_length=2000)


class Proposal(BaseModel):
    id: str
    model_id: str
    entity_name: str | None
    artifact_kind: str
    artifact_name: str
    proposed_by: str
    reason: str
    before_yaml: str | None
    after_yaml: str | None
    status: str
    notified_to: list[str]
    decision_actor: str | None
    decision_at: str | None
    decision_comment: str | None
    superseded_by: str | None
    created_at: str


class NotificationOut(BaseModel):
    """One row from /me/notifications, joined with its proposal so the bell
    can render a useful preview without a second roundtrip."""
    id: str
    kind: str
    proposal_id: str
    created_at: str
    # Joined fields from proposals — null if the proposal was deleted
    # (shouldn't happen in normal flow but defensive).
    entity_name: str | None = None
    artifact_kind: str | None = None
    artifact_name: str | None = None
    proposed_by: str | None = None
    reason: str | None = None
    model_id: str | None = None
