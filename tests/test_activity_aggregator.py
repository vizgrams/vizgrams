# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_service.get_activity_for_entity (Epic 26 VG-290).

The /explore Activity tab merges four streams:
1. Entity-artifact diffs projected per row (attribute / relation changes)
2. Feature version bumps for features scoped to this entity
3. View (chart) version bumps for views rooted on this entity
4. Mapper version bumps for mappers targeting this entity

These tests pin down each stream individually + the merge ordering, the
ontology-version clustering label, and the pagination contract.
"""

from __future__ import annotations

import time

import pytest

from api.services.entity_service import (
    _diff_entity_versions,
    get_activity_for_entity,
)
from tests.conftest import seed_artifact

# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------


def _entity_yaml(
    name: str = "Widget",
    attributes: list[tuple[str, str]] | None = None,
    relations: list[tuple[str, str, str]] | None = None,
) -> str:
    attrs = attributes or [("name", "STRING")]
    rels = relations or []
    attr_lines = "\n".join(
        f"  {n}:\n    type: {t}\n    semantic: IDENTIFIER"
        for n, t in attrs
    )
    rel_yaml = ""
    if rels:
        rel_lines = "\n".join(
            f"  {n}:\n    target: {target}\n    cardinality: {card}"
            for n, target, card in rels
        )
        rel_yaml = f"\nrelations:\n{rel_lines}"
    return f"""entity: {name}
identity:
  {name.lower()}_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
{attr_lines}{rel_yaml}
"""


def _feature_yaml(name: str, entity_type: str, expr: str = "1") -> str:
    return f"""feature_id: {name}
name: {name}
entity_type: {entity_type}
data_type: INTEGER
expr: "{expr}"
"""


def _query_yaml(name: str, root: str) -> str:
    return f"""name: {name}
root: {root}
attributes:
  - {root.lower()}_key
"""


def _view_yaml(name: str, query: str) -> str:
    return f"""name: {name}
type: chart
query: {query}
visualization:
  chart_type: bar
"""


def _mapper_yaml(name: str, target_entity: str) -> str:
    return f"""mapper: {name}
sources:
  - alias: s
    table: raw_{target_entity.lower()}
    columns: [id]
targets:
  - entity: {target_entity}
    columns:
      - name: id
        expr: s.id
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


def _seed_versions_apart(model_dir, artifact_type, name, contents):
    """Seed multiple versions of the same artifact with a small sleep
    between writes so created_at timestamps are distinct + orderable."""
    for content in contents:
        seed_artifact(model_dir, artifact_type, name, content)
        time.sleep(0.01)  # ISO timestamps have ms resolution


# ---------------------------------------------------------------------------
# _diff_entity_versions — the per-row projection helper
# ---------------------------------------------------------------------------


def test_diff_detects_added_attribute():
    prev = _entity_yaml("Widget", attributes=[("name", "STRING")])
    nxt = _entity_yaml("Widget", attributes=[("name", "STRING"), ("score", "FLOAT")])
    changes = _diff_entity_versions(prev, nxt)
    assert {"action": "created", "kind": "attribute", "name": "score"} in changes


def test_diff_detects_removed_attribute():
    prev = _entity_yaml("Widget", attributes=[("name", "STRING"), ("score", "FLOAT")])
    nxt = _entity_yaml("Widget", attributes=[("name", "STRING")])
    changes = _diff_entity_versions(prev, nxt)
    assert {"action": "deleted", "kind": "attribute", "name": "score"} in changes


def test_diff_detects_updated_attribute():
    prev = _entity_yaml("Widget", attributes=[("name", "STRING")])
    nxt = _entity_yaml("Widget", attributes=[("name", "TEXT")])  # type changed
    changes = _diff_entity_versions(prev, nxt)
    assert {"action": "updated", "kind": "attribute", "name": "name"} in changes


def test_diff_detects_added_relation():
    prev = _entity_yaml("Widget")
    nxt = _entity_yaml("Widget", relations=[("team", "Team", "many-to-one")])
    changes = _diff_entity_versions(prev, nxt)
    assert {"action": "created", "kind": "relation", "name": "team"} in changes


def test_diff_first_version_treats_all_rows_as_created():
    """No previous YAML → every row in the new YAML is a creation."""
    nxt = _entity_yaml("Widget",
                       attributes=[("name", "STRING"), ("score", "FLOAT")],
                       relations=[("team", "Team", "many-to-one")])
    changes = _diff_entity_versions("", nxt)
    names = {(c["kind"], c["name"]) for c in changes}
    assert ("attribute", "name") in names
    assert ("attribute", "score") in names
    assert ("relation", "team") in names
    # All actions should be 'created'
    assert all(c["action"] == "created" for c in changes)


def test_diff_empty_when_yaml_identical():
    same = _entity_yaml("Widget")
    assert _diff_entity_versions(same, same) == []


def test_diff_handles_malformed_yaml_without_crashing():
    assert _diff_entity_versions("not: valid:\nyaml :", _entity_yaml("Widget")) == []


# ---------------------------------------------------------------------------
# Per-stream events — verify each source contributes
# ---------------------------------------------------------------------------


def test_entity_version_events_projects_row_changes(model_dir):
    """Two entity versions → events for each added attribute, tagged with
    ontology_version label (`v1 → v2`)."""
    _seed_versions_apart(model_dir, "entity", "widget", [
        _entity_yaml("Widget", attributes=[("name", "STRING")]),
        _entity_yaml("Widget", attributes=[("name", "STRING"), ("score", "FLOAT")]),
    ])
    feed = get_activity_for_entity(model_dir, "Widget")
    events = feed["events"]
    # Find the projected-from-v2 event for 'score'
    score_events = [e for e in events if e["object_name"] == "score"]
    assert len(score_events) == 1
    assert score_events[0]["action"] == "created"
    assert score_events[0]["object_kind"] == "attribute"
    assert score_events[0]["ontology_version"] == "v1 → v2"


def test_feature_events_only_for_scoped_features(model_dir):
    """A feature with entity_type=Widget surfaces in Widget's activity;
    a feature for Gadget does not."""
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    seed_artifact(model_dir, "entity", "gadget", _entity_yaml("Gadget"))
    seed_artifact(model_dir, "feature", "is_big",
                  _feature_yaml("is_big", "Widget", 'score > 10'))
    seed_artifact(model_dir, "feature", "other",
                  _feature_yaml("other", "Gadget", '1'))

    feed = get_activity_for_entity(model_dir, "Widget")
    names = [(e["object_kind"], e["object_name"]) for e in feed["events"]]
    assert ("computed", "is_big") in names
    assert ("computed", "other") not in names


def test_chart_events_only_for_views_rooted_on_entity(model_dir):
    """Views whose underlying query is rooted on Widget appear; others don't."""
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    seed_artifact(model_dir, "entity", "gadget", _entity_yaml("Gadget"))
    seed_artifact(model_dir, "query", "q_widgets", _query_yaml("q_widgets", "Widget"))
    seed_artifact(model_dir, "query", "q_gadgets", _query_yaml("q_gadgets", "Gadget"))
    seed_artifact(model_dir, "view", "widget_chart", _view_yaml("widget_chart", "q_widgets"))
    seed_artifact(model_dir, "view", "gadget_chart", _view_yaml("gadget_chart", "q_gadgets"))

    feed = get_activity_for_entity(model_dir, "Widget")
    chart_names = [e["object_name"] for e in feed["events"] if e["object_kind"] == "chart"]
    assert chart_names == ["widget_chart"]


def test_mapper_events_only_for_mappers_targeting_entity(model_dir):
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    seed_artifact(model_dir, "entity", "gadget", _entity_yaml("Gadget"))
    seed_artifact(model_dir, "mapper", "widget_mapper", _mapper_yaml("widget_mapper", "Widget"))
    seed_artifact(model_dir, "mapper", "gadget_mapper", _mapper_yaml("gadget_mapper", "Gadget"))

    feed = get_activity_for_entity(model_dir, "Widget")
    mapper_names = [e["object_name"] for e in feed["events"] if e["object_kind"] == "mapper"]
    assert mapper_names == ["widget_mapper"]


# ---------------------------------------------------------------------------
# Merge + sort + pagination
# ---------------------------------------------------------------------------


def test_events_are_returned_newest_first(model_dir):
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    time.sleep(0.02)
    seed_artifact(model_dir, "feature", "f1", _feature_yaml("f1", "Widget"))

    feed = get_activity_for_entity(model_dir, "Widget")
    timestamps = [e["created_at"] for e in feed["events"]]
    assert timestamps == sorted(timestamps, reverse=True)


def test_pagination_limit_returns_first_n(model_dir):
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    # Seed several features so the timeline has > 1 entry
    for i in range(5):
        seed_artifact(model_dir, "feature", f"f{i}", _feature_yaml(f"f{i}", "Widget"))
        time.sleep(0.005)

    feed = get_activity_for_entity(model_dir, "Widget", limit=2)
    assert len(feed["events"]) == 2
    assert feed["has_more"] is True


def test_pagination_offset_returns_next_window(model_dir):
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    for i in range(5):
        seed_artifact(model_dir, "feature", f"f{i}", _feature_yaml(f"f{i}", "Widget"))
        time.sleep(0.005)

    first = get_activity_for_entity(model_dir, "Widget", limit=2, offset=0)
    second = get_activity_for_entity(model_dir, "Widget", limit=2, offset=2)
    assert first["events"][0]["created_at"] != second["events"][0]["created_at"]


def test_empty_when_entity_has_no_history(model_dir):
    feed = get_activity_for_entity(model_dir, "Unknown")
    assert feed == {"events": [], "has_more": False}


def test_ontology_version_label_shared_across_rows_in_same_bump(model_dir):
    """Multiple changes from the same entity-version bump must share the
    same ``ontology_version`` label — that's what the UI uses to cluster
    them into a single OntologyBumpCard."""
    _seed_versions_apart(model_dir, "entity", "widget", [
        _entity_yaml("Widget", attributes=[("name", "STRING")]),
        _entity_yaml("Widget",
                     attributes=[("name", "STRING"), ("score", "FLOAT"), ("rank", "INTEGER")]),
    ])
    feed = get_activity_for_entity(model_dir, "Widget")
    v2_events = [e for e in feed["events"] if e["ontology_version"] == "v1 → v2"]
    # Both 'score' and 'rank' added in v2 → both carry the same label.
    names = [e["object_name"] for e in v2_events]
    assert "score" in names
    assert "rank" in names


def test_first_entity_version_uses_solo_version_label(model_dir):
    """The very first entity version has no `prev` → label is just `v1`."""
    seed_artifact(model_dir, "entity", "widget", _entity_yaml("Widget"))
    feed = get_activity_for_entity(model_dir, "Widget")
    v1_events = [e for e in feed["events"] if e.get("ontology_version")]
    assert all(e["ontology_version"] == "v1" for e in v1_events)
