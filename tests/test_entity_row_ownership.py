# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_service.resolve_row_owner (Epic 26 VG-290).

Per-row ontology ownership = last-touched-by on the entity's own
version timeline. This helper exists so the propose-change flow
(VG-295) can route a proposal's notifications to the right user when
a Member proposes editing one row inside an entity. Computed features
are independently-versioned, so the resolver delegates to the existing
ownership service for that case.
"""

from __future__ import annotations

import pytest

from api.services.entity_service import resolve_row_owner
from core import metadata_db


def _entity_yaml_with(attributes=None, relations=None):
    """Minimal entity YAML with the requested attributes and relations."""
    attrs = attributes or [("name", "STRING")]
    rels = relations or []
    attr_lines = "\n".join(
        f"  {n}:\n    type: {t}\n    semantic: IDENTIFIER" for n, t in attrs
    )
    rel_yaml = ""
    if rels:
        rel_lines = "\n".join(
            f"  {n}:\n    target: {target}\n    cardinality: {card}"
            for n, target, card in rels
        )
        rel_yaml = f"\nrelations:\n{rel_lines}"
    return f"""entity: Widget
identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
{attr_lines}{rel_yaml}
"""


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


def _record(model_dir, artifact_type, name, content, user_id):
    """Record a version with a specific created_by — exercises the path that
    the proposal-routing code cares about."""
    metadata_db.record_version(
        model_dir, artifact_type, name, content, user_id=user_id, via="api",
    )


# ---------------------------------------------------------------------------
# Attribute ownership
# ---------------------------------------------------------------------------


def test_returns_creator_of_attribute_when_one_version(model_dir):
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "STRING")]),
            user_id="alice")
    assert resolve_row_owner(model_dir, "Widget", "attribute", "name") == "alice"


def test_returns_last_modifier_when_attribute_changed(model_dir):
    """alice creates the attribute; bob later changes its type. bob is the
    current owner."""
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "STRING")]),
            user_id="alice")
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "TEXT")]),  # type changed
            user_id="bob")
    assert resolve_row_owner(model_dir, "Widget", "attribute", "name") == "bob"


def test_returns_original_creator_when_other_rows_change(model_dir):
    """alice creates 'name'; bob adds 'score'. 'name' is still alice's because
    bob's version didn't touch it."""
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "STRING")]),
            user_id="alice")
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "STRING"), ("score", "FLOAT")]),
            user_id="bob")
    assert resolve_row_owner(model_dir, "Widget", "attribute", "name") == "alice"
    assert resolve_row_owner(model_dir, "Widget", "attribute", "score") == "bob"


def test_returns_none_for_unknown_attribute(model_dir):
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(attributes=[("name", "STRING")]),
            user_id="alice")
    assert resolve_row_owner(model_dir, "Widget", "attribute", "missing") is None


def test_returns_none_when_entity_has_no_history(model_dir):
    assert resolve_row_owner(model_dir, "Widget", "attribute", "name") is None


# ---------------------------------------------------------------------------
# Relation ownership
# ---------------------------------------------------------------------------


def test_returns_creator_of_relation(model_dir):
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(relations=[("team", "Team", "many-to-one")]),
            user_id="cathy")
    assert resolve_row_owner(model_dir, "Widget", "relation", "team") == "cathy"


def test_returns_last_modifier_of_relation(model_dir):
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(relations=[("team", "Team", "many-to-one")]),
            user_id="cathy")
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(relations=[("team", "Team", "one-to-many")]),
            user_id="daniel")
    assert resolve_row_owner(model_dir, "Widget", "relation", "team") == "daniel"


# ---------------------------------------------------------------------------
# Computed → delegates to feature ownership service
# ---------------------------------------------------------------------------


def test_computed_row_delegates_to_feature_ownership(model_dir):
    """``row_kind=computed`` is independently-versioned — the resolver should
    delegate rather than walk the entity timeline."""
    _record(model_dir, "feature", "is_open",
            'feature_id: is_open\nname: is_open\nentity_type: Widget\n'
            'data_type: BOOLEAN\nexpr: "state == \'open\'"\n',
            user_id="bob")
    assert resolve_row_owner(model_dir, "Widget", "computed", "is_open") == "bob"


def test_computed_row_returns_none_when_feature_missing(model_dir):
    assert resolve_row_owner(model_dir, "Widget", "computed", "missing") is None


# ---------------------------------------------------------------------------
# Unknown row_kind
# ---------------------------------------------------------------------------


def test_unknown_row_kind_returns_none(model_dir):
    _record(model_dir, "entity", "widget",
            _entity_yaml_with(),
            user_id="alice")
    assert resolve_row_owner(model_dir, "Widget", "metric", "anything") is None
