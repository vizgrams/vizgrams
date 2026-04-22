# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/feature_service.py."""

import pytest

from api.services.feature_service import (
    get_feature,
    list_all_features,
    list_features,
)

# ---------------------------------------------------------------------------
# Minimal YAML content
# ---------------------------------------------------------------------------

_WIDGET_ENTITY_YAML = """\
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
    type: FLOAT
    semantic: MEASURE
"""

_WIDGET_FEATURE_RAW_SQL = """\
feature_id: widget.score_doubled
name: Score Doubled
entity_type: Widget
entity_key: widget_key
data_type: FLOAT
materialization_mode: materialized
raw_sql: "SELECT widget_key AS entity_id, score * 2 AS value FROM sem_widget"
"""

_ISSUE_ENTITY_YAML = """\
entity: Issue

identity:
  issue_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  resolved:
    type: STRING
    semantic: TIMESTAMP
"""

# Uses old `definition:` key — should normalise to raw_sql
_ISSUE_FEATURE_DEFINITION = """\
feature_id: issue.resolved_at
name: Resolved At
entity_type: Issue
entity_key: issue_key
data_type: STRING
materialization_mode: materialized
definition: |
  SELECT issue_key AS entity_id, resolved AS value FROM sem_issue
"""


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    return tmp_path


@pytest.fixture
def model_dir_with_widget_feature(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "entity", "widget", _WIDGET_ENTITY_YAML)
    seed_artifact(model_dir, "feature", "widget.score_doubled", _WIDGET_FEATURE_RAW_SQL)
    return model_dir


@pytest.fixture
def model_dir_with_both_entities(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "entity", "widget", _WIDGET_ENTITY_YAML)
    seed_artifact(model_dir, "entity", "issue", _ISSUE_ENTITY_YAML)
    seed_artifact(model_dir, "feature", "widget.score_doubled", _WIDGET_FEATURE_RAW_SQL)
    seed_artifact(model_dir, "feature", "issue.resolved_at", _ISSUE_FEATURE_DEFINITION)
    return model_dir


# ---------------------------------------------------------------------------
# list_features (per entity)
# ---------------------------------------------------------------------------

def test_list_features_empty_when_no_features_dir(tmp_path):
    result = list_features(tmp_path, "Widget")
    assert result == []


def test_list_features_returns_features_for_entity(model_dir_with_widget_feature):
    result = list_features(model_dir_with_widget_feature, "Widget")
    names = [f["name"] for f in result]
    assert "score_doubled" in names


def test_list_features_filters_by_entity(model_dir_with_both_entities):
    widget_features = list_features(model_dir_with_both_entities, "Widget")
    issue_features = list_features(model_dir_with_both_entities, "Issue")
    widget_names = [f["name"] for f in widget_features]
    issue_names = [f["name"] for f in issue_features]
    assert "score_doubled" in widget_names
    assert "resolved_at" in issue_names
    assert "resolved_at" not in widget_names


def test_list_features_includes_entity_and_type(model_dir_with_widget_feature):
    result = list_features(model_dir_with_widget_feature, "Widget")
    feat = result[0]
    assert feat["entity"] == "Widget"
    assert "feature_type" in feat


# ---------------------------------------------------------------------------
# list_all_features
# ---------------------------------------------------------------------------

def test_list_all_features_returns_all_entities(model_dir_with_both_entities):
    result = list_all_features(model_dir_with_both_entities)
    feature_ids = [f["feature_id"] for f in result]
    assert "widget.score_doubled" in feature_ids
    assert "issue.resolved_at" in feature_ids


def test_list_all_features_empty_when_no_dir(tmp_path):
    result = list_all_features(tmp_path)
    assert result == []


def test_list_all_features_sorted_by_feature_id(model_dir_with_both_entities):
    result = list_all_features(model_dir_with_both_entities)
    ids = [f["feature_id"] for f in result]
    assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# get_feature
# ---------------------------------------------------------------------------

def test_get_feature_found(model_dir_with_widget_feature):
    result = get_feature(model_dir_with_widget_feature, "Widget", "score_doubled")
    assert result["name"] == "score_doubled"
    assert result["entity"] == "Widget"


def test_get_feature_not_found_raises_key_error(model_dir_with_widget_feature):
    with pytest.raises(KeyError):
        get_feature(model_dir_with_widget_feature, "Widget", "nonexistent")


def test_get_feature_wrong_entity_raises_key_error(model_dir_with_both_entities):
    with pytest.raises(KeyError):
        get_feature(model_dir_with_both_entities, "Issue", "score_doubled")


def test_get_feature_pascal_case_entity_with_lowercase_feature_id_prefix(model_dir_with_both_entities):
    """
    Regression: feature_id stored as 'issue.resolved_at' (lowercase prefix)
    but entity name in the request is 'Issue' (PascalCase).
    Lookup must match on entity_type field, not reconstruct the feature_id.
    """
    # This should NOT raise KeyError
    result = get_feature(model_dir_with_both_entities, "Issue", "resolved_at")
    assert result["name"] == "resolved_at"
    assert result["entity"] == "Issue"


def test_get_feature_raw_sql_included(model_dir_with_both_entities):
    result = get_feature(model_dir_with_both_entities, "Issue", "resolved_at")
    assert "raw_sql" in result or "definition" in result or result.get("feature_type") == "raw_sql"
