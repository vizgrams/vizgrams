# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_service.list_charts_for_entity (Epic 26 VG-290).

The /explore Charts tab needs "give me every chart rooted on entity X".
Filtering happens by walking each view's underlying query and matching
on ``query.entity``. These tests pin that filter down + verify the
flattened ``chart_type`` we surface alongside the existing ViewSummary
shape.
"""

from __future__ import annotations

import pytest

from api.services.entity_service import _chart_type_label, list_charts_for_entity
from tests.conftest import seed_artifact

_WIDGET_ENTITY_YAML = """\
entity: Widget
description: "A widget for testing."
identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
"""

_GADGET_ENTITY_YAML = """\
entity: Gadget
description: "A different entity."
identity:
  gadget_key:
    type: STRING
    semantic: PRIMARY_KEY
"""


def _query_yaml(name: str, root: str) -> str:
    return f"""name: {name}
root: {root}
attributes:
  - widget_key
"""


def _view_yaml(name: str, query: str, chart_type: str = "bar", view_type: str = "chart") -> str:
    return f"""name: {name}
type: {view_type}
query: {query}
visualization:
  chart_type: {chart_type}
  x: x_col
  y:
    - y_col
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


@pytest.fixture
def two_entity_model(model_dir):
    """Two entities + two queries + two views, one rooted on each entity."""
    seed_artifact(model_dir, "entity", "widget", _WIDGET_ENTITY_YAML)
    seed_artifact(model_dir, "entity", "gadget", _GADGET_ENTITY_YAML)
    seed_artifact(model_dir, "query", "q_widgets", _query_yaml("q_widgets", "Widget"))
    seed_artifact(model_dir, "query", "q_gadgets", _query_yaml("q_gadgets", "Gadget"))
    seed_artifact(model_dir, "view", "widgets_by_name", _view_yaml("widgets_by_name", "q_widgets"))
    seed_artifact(model_dir, "view", "gadgets_by_name", _view_yaml("gadgets_by_name", "q_gadgets"))
    return model_dir


# ---------------------------------------------------------------------------
# Filter behaviour
# ---------------------------------------------------------------------------


def test_returns_only_charts_rooted_on_requested_entity(two_entity_model):
    result = list_charts_for_entity(two_entity_model, "Widget")
    names = [c["name"] for c in result]
    assert names == ["widgets_by_name"]


def test_returns_empty_list_for_entity_with_no_charts(two_entity_model):
    lonely_yaml = (
        "entity: Lonely\n"
        "identity:\n"
        "  k:\n"
        "    type: STRING\n"
        "    semantic: PRIMARY_KEY\n"
    )
    seed_artifact(two_entity_model, "entity", "lonely", lonely_yaml)
    result = list_charts_for_entity(two_entity_model, "Lonely")
    assert result == []


def test_handles_no_views_at_all(model_dir):
    seed_artifact(model_dir, "entity", "widget", _WIDGET_ENTITY_YAML)
    result = list_charts_for_entity(model_dir, "Widget")
    assert result == []


def test_skips_views_whose_query_is_missing(model_dir):
    """A view pointing at a non-existent query is filtered out (degrades silently)."""
    seed_artifact(model_dir, "view", "orphan_view", _view_yaml("orphan_view", "missing_query"))
    result = list_charts_for_entity(model_dir, "Widget")
    assert result == []


def test_multiple_charts_same_entity_are_all_returned(two_entity_model):
    seed_artifact(two_entity_model, "view", "widgets_top10",
                  _view_yaml("widgets_top10", "q_widgets", chart_type="bar"))
    seed_artifact(two_entity_model, "view", "widgets_trend",
                  _view_yaml("widgets_trend", "q_widgets", chart_type="line"))
    result = list_charts_for_entity(two_entity_model, "Widget")
    names = sorted(c["name"] for c in result)
    assert names == ["widgets_by_name", "widgets_top10", "widgets_trend"]


# ---------------------------------------------------------------------------
# Shape — must mirror ViewSummary + add chart_type
# ---------------------------------------------------------------------------


def test_returned_charts_have_view_summary_fields(two_entity_model):
    [chart] = list_charts_for_entity(two_entity_model, "Widget")
    # ViewSummary fields:
    assert chart["name"] == "widgets_by_name"
    assert chart["type"] == "chart"
    assert chart["query"] == "q_widgets"
    # Cert fields default to false/None for uncertified:
    assert chart["is_certified"] is False
    # Owner fields are present but may be None for non-tracked legacy artifacts:
    assert "created_by" in chart
    assert "created_at" in chart


def test_flattens_visualization_chart_type_to_top_level(two_entity_model):
    [chart] = list_charts_for_entity(two_entity_model, "Widget")
    assert chart["chart_type"] == "bar"


def test_chart_type_label_for_chart_view_uses_visualization_chart_type():
    class FakeView:
        type = "chart"
        visualization = {"chart_type": "line"}
    assert _chart_type_label(FakeView()) == "line"


def test_chart_type_label_for_chart_view_falls_back_when_visualization_empty():
    class FakeView:
        type = "chart"
        visualization = {}
    assert _chart_type_label(FakeView()) == "chart"


def test_chart_type_label_for_metric_view_returns_kpi():
    class FakeView:
        type = "metric"
        visualization = {}
    assert _chart_type_label(FakeView()) == "kpi"


def test_chart_type_label_for_table_view_returns_table():
    class FakeView:
        type = "table"
        visualization = {}
    assert _chart_type_label(FakeView()) == "table"


def test_chart_type_label_handles_missing_visualization_attr():
    class FakeView:
        type = "chart"
        # no visualization attribute at all
    assert _chart_type_label(FakeView()) == "chart"
