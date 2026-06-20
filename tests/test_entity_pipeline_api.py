# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_service.get_pipeline_for_entity (Epic 26 VG-290).

The /explore Pipeline tab needs the full lineage graph for an entity:
the single mapper writing to it, the raw tables that mapper joins, and
for each raw table the extractor + tool that produced it. These tests
pin down the single-source case, the multi-source case (the design's
key complication), the mapper-with-sub-groups case, and the graceful
degradation when extractors are missing.
"""

from __future__ import annotations

import pytest

from api.services.entity_service import (
    _build_extractor_table_index,
    _find_mapper_targeting,
    get_pipeline_for_entity,
)
from tests.conftest import seed_artifact

# ---------------------------------------------------------------------------
# YAML fixtures
# ---------------------------------------------------------------------------


_ENTITY_YAML = """\
entity: {name}
description: "{name} for pipeline tests."
identity:
  {pk}:
    type: STRING
    semantic: PRIMARY_KEY
"""


def _entity(name: str, pk: str = "id") -> str:
    return _ENTITY_YAML.format(name=name, pk=pk)


def _mapper_single_source(name: str, target_entity: str, raw_table: str) -> str:
    """Mapper joining one raw table, writing to one entity, no row groups."""
    return f"""mapper: {name}
sources:
  - alias: s
    table: {raw_table}
    columns: [id]
targets:
  - entity: {target_entity}
    columns:
      - name: id
        expr: s.id
"""


def _mapper_multi_source(name: str, target_entity: str, raw_tables: list[str]) -> str:
    """Mapper joining two raw tables (the multi-source case the design needs)."""
    sources_yaml = "\n".join(
        f"  - alias: s{i}\n    table: {t}\n    columns: [id]"
        for i, t in enumerate(raw_tables)
    )
    return f"""mapper: {name}
sources:
{sources_yaml}
targets:
  - entity: {target_entity}
    columns:
      - name: id
        expr: s0.id
"""


def _mapper_with_subgroups(name: str, target_entity: str, raw_table: str, groups: list[str]) -> str:
    """Mapper with multiple RowGroup entries on the same target."""
    rows_yaml = "\n".join(
        f"      - from: {g}\n        joins: []\n        columns:\n          - name: id\n            expr: {g}.id"
        for g in groups
    )
    sources_yaml = "\n".join(
        f"  - alias: {g}\n    table: {raw_table}\n    columns: [id]"
        for g in groups
    )
    return f"""mapper: {name}
sources:
{sources_yaml}
targets:
  - entity: {target_entity}
    rows:
{rows_yaml}
"""


def _extractor(name: str, tool: str, raw_table: str) -> str:
    return f"""extractor: {name}
tasks:
  - name: pull
    tool: {tool}
    command: list
    output:
      table: {raw_table}
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


# ---------------------------------------------------------------------------
# get_pipeline_for_entity — happy paths
# ---------------------------------------------------------------------------


def test_returns_none_when_no_mapper_targets_entity(model_dir):
    seed_artifact(model_dir, "entity", "lonely", _entity("Lonely"))
    assert get_pipeline_for_entity(model_dir, "Lonely") is None


def test_single_source_pipeline_links_mapper_to_extractor(model_dir):
    seed_artifact(model_dir, "entity", "widget", _entity("Widget"))
    seed_artifact(model_dir, "mapper", "widget_mapper",
                  _mapper_single_source("widget_mapper", "Widget", "raw_widgets"))
    seed_artifact(model_dir, "extractor", "widget_extractor",
                  _extractor("widget_extractor", "github", "raw_widgets"))

    result = get_pipeline_for_entity(model_dir, "Widget")

    assert result["entity"] == "Widget"
    assert result["mapper"]["name"] == "widget_mapper"
    assert result["mapper"]["groups"] == []  # no row sub-groups
    assert result["sources"] == [
        {"tool": "github", "extractor": "widget_extractor", "raw_table": "raw_widgets"},
    ]


def test_multi_source_pipeline_preserves_source_order(model_dir):
    """Each raw table joined in the mapper becomes its own LineagePath in the
    response. Order follows the YAML's sources: list."""
    seed_artifact(model_dir, "entity", "pr", _entity("PullRequest"))
    seed_artifact(model_dir, "mapper", "pr_mapper",
                  _mapper_multi_source("pr_mapper", "PullRequest",
                                       ["raw_pull_requests", "raw_users"]))
    seed_artifact(model_dir, "extractor", "github_pulls",
                  _extractor("github_pulls", "github", "raw_pull_requests"))
    seed_artifact(model_dir, "extractor", "github_users",
                  _extractor("github_users", "github", "raw_users"))

    result = get_pipeline_for_entity(model_dir, "PullRequest")

    assert [s["raw_table"] for s in result["sources"]] == [
        "raw_pull_requests", "raw_users",
    ]
    assert [s["extractor"] for s in result["sources"]] == [
        "github_pulls", "github_users",
    ]


def test_mapper_with_sub_groups_surfaces_them(model_dir):
    """Contribution-style: one mapper, one target entity, several RowGroups
    discriminated by from_alias. UI renders them as sub-group list."""
    seed_artifact(model_dir, "entity", "contribution", _entity("Contribution"))
    seed_artifact(model_dir, "mapper", "contributions",
                  _mapper_with_subgroups("contributions", "Contribution", "raw_events",
                                         ["authors", "reviews", "commits"]))

    result = get_pipeline_for_entity(model_dir, "Contribution")

    assert [g["name"] for g in result["mapper"]["groups"]] == [
        "authors", "reviews", "commits",
    ]


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


def test_mapper_raw_prefix_matches_bare_extractor_table(model_dir):
    """Real-world iagai shape: mapper sources reference ``raw_<table>`` (the
    CH cross-database convention) but extractor YAMLs declare the bare
    ``<table>`` they actually write to. Pre-fix the lookup compared the
    literal mapper string against the extractor index and missed every
    iagai source, so Tool + Extractor chips all rendered as "(not in
    catalog)" in the UI."""
    seed_artifact(model_dir, "entity", "widget", _entity("Widget"))
    seed_artifact(model_dir, "mapper", "widget_mapper",
                  _mapper_single_source("widget_mapper", "Widget", "raw_widgets"))
    # Note: extractor declares the BARE table name, not raw_widgets.
    seed_artifact(model_dir, "extractor", "widget_extractor",
                  _extractor("widget_extractor", "github", "widgets"))

    result = get_pipeline_for_entity(model_dir, "Widget")

    assert result["sources"] == [
        {"tool": "github", "extractor": "widget_extractor", "raw_table": "raw_widgets"},
    ]


def test_raw_table_with_no_producing_extractor_surfaces_with_null_extractor(model_dir):
    """An orphan raw table (no extractor produces it) still appears in
    sources so the UI can show the gap rather than silently dropping it."""
    seed_artifact(model_dir, "entity", "widget", _entity("Widget"))
    seed_artifact(model_dir, "mapper", "widget_mapper",
                  _mapper_single_source("widget_mapper", "Widget", "raw_orphan"))

    result = get_pipeline_for_entity(model_dir, "Widget")

    assert result["sources"] == [
        {"tool": None, "extractor": None, "raw_table": "raw_orphan"},
    ]


def test_malformed_mapper_is_skipped(model_dir):
    """A mapper that doesn't parse must not crash pipeline lookup for other entities."""
    seed_artifact(model_dir, "entity", "widget", _entity("Widget"))
    seed_artifact(model_dir, "mapper", "broken", "not: valid: mapper\nyaml :")
    seed_artifact(model_dir, "mapper", "good",
                  _mapper_single_source("good", "Widget", "raw_widgets"))

    result = get_pipeline_for_entity(model_dir, "Widget")
    assert result is not None
    assert result["mapper"]["name"] == "good"


def test_first_mapper_wins_when_two_target_same_entity(model_dir):
    """Convention is one mapper per entity. If two exist (a config error or
    transient state), the first one found is returned — deterministic for tests."""
    seed_artifact(model_dir, "entity", "widget", _entity("Widget"))
    seed_artifact(model_dir, "mapper", "a_mapper",
                  _mapper_single_source("a_mapper", "Widget", "raw_a"))
    seed_artifact(model_dir, "mapper", "z_mapper",
                  _mapper_single_source("z_mapper", "Widget", "raw_z"))

    result = get_pipeline_for_entity(model_dir, "Widget")
    # `list_artifact_names` returns alphabetically — a_mapper wins.
    assert result["mapper"]["name"] == "a_mapper"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def test_build_extractor_table_index_returns_per_table_lookup(model_dir):
    seed_artifact(model_dir, "extractor", "e1", _extractor("e1", "github", "raw_a"))
    seed_artifact(model_dir, "extractor", "e2", _extractor("e2", "jira", "raw_b"))

    idx = _build_extractor_table_index(model_dir)
    assert idx == {
        "raw_a": {"name": "e1", "tool": "github"},
        "raw_b": {"name": "e2", "tool": "jira"},
    }


def test_find_mapper_targeting_returns_none_when_no_mapper_matches(model_dir):
    seed_artifact(model_dir, "mapper", "other",
                  _mapper_single_source("other", "OtherEntity", "raw_other"))
    assert _find_mapper_targeting(model_dir, "Widget") is None
