# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the run_saved_view Tool handler (Epic 20 VG-234)."""

from __future__ import annotations

import pytest

from semantic.llm.tools.registry import ToolContext
from semantic.llm.tools.run_saved_view import RUN_SAVED_VIEW, _chart_spec_from_view


@pytest.fixture(autouse=True)
def patch_services(monkeypatch):
    """Stub view_service + query_service for the handler's lazy imports."""
    import sys
    import types

    calls = {"execute_view": [], "get_view": [], "get_query": []}

    def execute_view(model_dir, name, *, limit=1000, offset=0, params=None):
        calls["execute_view"].append({"model_dir": model_dir, "name": name,
                                      "limit": limit, "params": params})
        if name == "missing":
            raise KeyError(f"view {name!r} not found")
        if name == "broken":
            raise RuntimeError("synthetic view-execute error")
        # Default: a chart view with two y-columns
        return {
            "name": name,
            "type": "chart",
            "query": f"{name}_query",
            "visualization": {
                "chart_type": "line",
                "x": "month",
                "y": ["avg_clt", "avg_merge"],
                "point_drilldown": {"view": "drill_detail", "params": {}},
            },
            "columns": ["month", "avg_clt", "avg_merge"],
            "rows": [["2025-01", 12.5, 30.2], ["2025-02", 14.1, 28.7]],
            "row_count": 2,
            "total_row_count": 2,
            "duration_ms": 5,
            "truncated": False,
            "raw_yaml": f"name: {name}\ntype: chart\n",
        }

    def get_view(model_dir, name):
        calls["get_view"].append({"model_dir": model_dir, "name": name})
        return {"name": name, "raw_yaml": f"name: {name}\ntype: chart\n"}

    def get_query(model_dir, name):
        calls["get_query"].append({"model_dir": model_dir, "name": name})
        return {"name": name, "raw_yaml": f"name: {name}\nroot: PullRequest\n"}

    view_stub = types.SimpleNamespace(execute_view=execute_view, get_view=get_view)
    query_stub = types.SimpleNamespace(execute_query=lambda *a, **k: {}, get_query=get_query)

    monkeypatch.setitem(sys.modules, "api.services.view_service", view_stub)
    monkeypatch.setitem(sys.modules, "api.services.query_service", query_stub)
    import api.services
    monkeypatch.setattr(api.services, "view_service", view_stub, raising=False)
    monkeypatch.setattr(api.services, "query_service", query_stub, raising=False)
    return calls


# ---------------------------------------------------------------------------
# _chart_spec_from_view — view-shape to chat-shape translation
# ---------------------------------------------------------------------------


def test_chart_view_translates_to_line_with_first_y():
    spec = _chart_spec_from_view({
        "type": "chart",
        "visualization": {
            "chart_type": "line", "x": "month",
            "y": ["avg_clt", "avg_merge"],
            "point_drilldown": {"view": "drill"},
        },
    })
    assert spec["chart_type"] == "line"
    assert spec["x_field"] == "month"
    # Only first y from the list — chat shape is single-y
    assert spec["y_field"] == "avg_clt"
    assert spec["drilldown"] == {"view": "drill"}


def test_chart_view_with_bar_chart_type():
    spec = _chart_spec_from_view({
        "type": "chart",
        "visualization": {"chart_type": "bar", "x": "team", "y": ["count"]},
    })
    assert spec["chart_type"] == "bar"
    assert spec["x_field"] == "team"


def test_chart_view_with_unsupported_chart_type_falls_back_to_table():
    """calendar_heatmap / map / etc. — chat doesn't render those, table is the sane fallback."""
    spec = _chart_spec_from_view({
        "type": "chart",
        "visualization": {"chart_type": "calendar_heatmap", "date": "d", "value": "v"},
    })
    assert spec["chart_type"] == "table"


def test_table_view_translates_to_table_chart():
    spec = _chart_spec_from_view({
        "type": "table",
        "visualization": {
            "columns": ["a", "b", "c"],
            "row_drilldown": {"entity": "PR", "id_column": "pr_key"},
        },
    })
    assert spec["chart_type"] == "table"
    assert spec["drilldown"] == {"entity": "PR", "id_column": "pr_key"}


def test_metric_view_translates_to_kpi():
    spec = _chart_spec_from_view({
        "type": "metric",
        "measure": "total_revenue",
        "visualization": {},
    })
    assert spec["chart_type"] == "kpi"
    assert spec["y_field"] == "total_revenue"


def test_unknown_view_type_falls_back_to_table():
    spec = _chart_spec_from_view({"type": "map", "visualization": {}})
    assert spec["chart_type"] == "table"


# ---------------------------------------------------------------------------
# Handler — happy path
# ---------------------------------------------------------------------------


def test_handler_invokes_named_view_and_returns_rows(patch_services, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_VIEW.handler({"name": "dora_clt_by_team"}, ctx)

    assert result.success
    assert result.payload["columns"] == ["month", "avg_clt", "avg_merge"]
    assert result.payload["row_count"] == 2
    assert result.payload["view_name"] == "dora_clt_by_team"
    assert result.payload["chart_type"] == "line"

    # Extras carry orchestrator-only pieces
    assert result.extras["saved_view_name"] == "dora_clt_by_team"
    assert result.extras["underlying_query"] == "dora_clt_by_team_query"
    assert result.extras["chart_spec"]["chart_type"] == "line"
    assert result.extras["chart_spec"]["x_field"] == "month"
    assert result.extras["chart_spec"]["y_field"] == "avg_clt"
    assert result.extras["chart_spec"]["drilldown"] == {"view": "drill_detail", "params": {}}
    assert result.extras["view_yaml"]
    assert result.extras["querydef_yaml"]


def test_handler_passes_params_and_limit_through(patch_services, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    RUN_SAVED_VIEW.handler(
        {"name": "x", "params": {"team": "Lovelace"}, "limit": 100},
        ctx,
    )
    call = patch_services["execute_view"][-1]
    assert call["params"] == {"team": "Lovelace"}
    assert call["limit"] == 100


# ---------------------------------------------------------------------------
# Handler — failure paths
# ---------------------------------------------------------------------------


def test_handler_returns_failure_when_view_missing(patch_services, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_VIEW.handler({"name": "missing"}, ctx)
    assert not result.success
    assert "not found" in result.payload["error"]


def test_handler_wraps_runtime_errors(patch_services, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_VIEW.handler({"name": "broken"}, ctx)
    assert not result.success
    assert "synthetic view-execute error" in result.payload["error"]


def test_handler_rejects_missing_name(patch_services, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_VIEW.handler({}, ctx)
    assert not result.success


def test_handler_fails_cleanly_when_no_model_dir(patch_services):
    result = RUN_SAVED_VIEW.handler({"name": "x"}, ToolContext())
    assert not result.success
    assert "model_dir" in result.payload["error"]


# ---------------------------------------------------------------------------
# Tool metadata + registry
# ---------------------------------------------------------------------------


def test_tool_definition_carries_expected_metadata():
    assert RUN_SAVED_VIEW.name == "run_saved_view"
    assert "query_authoring" in RUN_SAVED_VIEW.tags
    assert "view_invocation" in RUN_SAVED_VIEW.tags
    assert RUN_SAVED_VIEW.parameters_schema["required"] == ["name"]


def test_default_registry_includes_run_saved_view():
    from semantic.llm.tools import build_default_registry
    reg = build_default_registry()
    assert reg.get("run_saved_view") is not None
    qa_names = {t.name for t in reg.list(tags=("query_authoring",))}
    assert {"build_and_run_query", "find_artifacts",
            "run_saved_query", "run_saved_view"} <= qa_names


def test_summarize_renders_one_line_trace():
    from semantic.llm.tools.registry import ToolResult
    from semantic.llm.tools.run_saved_view import _summarize
    summary = _summarize(ToolResult(payload={
        "view_name": "dora_clt_by_team",
        "row_count": 18,
        "chart_type": "bar",
    }))
    assert "dora_clt_by_team" in summary
    assert "18 rows" in summary
    assert "bar" in summary
