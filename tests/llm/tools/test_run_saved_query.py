# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the run_saved_query Tool handler (Epic 20 VG-233)."""

from __future__ import annotations

import pytest

from semantic.llm.tools.registry import ToolContext
from semantic.llm.tools.run_saved_query import RUN_SAVED_QUERY


@pytest.fixture(autouse=True)
def patch_query_service(monkeypatch):
    """Replace api.services.query_service with a stub for the handler's lazy import.

    The handler does ``from api.services import query_service`` inside its
    body, so we swap the module in ``sys.modules`` — subsequent imports
    return the stub. Tracks invocations on a dict the test reads back.
    """
    import sys
    import types

    calls = {"execute": [], "get": []}

    def execute_query(model_dir, name, *, limit=1000, offset=0, fmt="json", params=None):
        calls["execute"].append({"model_dir": model_dir, "name": name,
                                 "limit": limit, "params": params})
        if name == "missing":
            raise KeyError(f"query {name!r} not found")
        if name == "broken":
            raise RuntimeError("synthetic compile error")
        return {
            "query": name,
            "sql": f"SELECT ... FROM {name}",
            "columns": ["team", "pr_count"],
            "rows": [["alpha", 12], ["beta", 7]],
            "row_count": 2,
            "total_row_count": 2,
            "duration_ms": 5,
            "truncated": False,
            "formats": {},
        }

    def get_query(model_dir, name):
        calls["get"].append({"model_dir": model_dir, "name": name})
        return {
            "name": name,
            "raw_yaml": f"name: {name}\nroot: PullRequest\n",
            "compiled_sql": f"SELECT ... FROM {name}",
        }

    stub = types.SimpleNamespace(execute_query=execute_query, get_query=get_query)
    monkeypatch.setitem(sys.modules, "api.services.query_service", stub)
    # The lazy ``from api.services import query_service`` inside the
    # handler looks up the attribute on the parent package, which was
    # already bound when api.services.query_service was first imported
    # earlier in the test run. Patching the attribute is what actually
    # diverts the import.
    import api.services
    monkeypatch.setattr(api.services, "query_service", stub, raising=False)
    return calls


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_handler_invokes_named_query_and_returns_rows(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({"name": "dora_clt_by_team"}, ctx)

    assert result.success
    assert result.payload["columns"] == ["team", "pr_count"]
    assert result.payload["rows"] == [["alpha", 12], ["beta", 7]]
    assert result.payload["row_count"] == 2
    assert result.payload["query_name"] == "dora_clt_by_team"
    # Saved YAML and SQL go in extras (orchestrator inspects; LLM doesn't see)
    assert "dora_clt_by_team" in result.extras["querydef_yaml"]
    assert result.extras["sql"].startswith("SELECT")
    assert result.extras["saved_query_name"] == "dora_clt_by_team"


def test_handler_passes_params_to_execute_query(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    RUN_SAVED_QUERY.handler(
        {"name": "team_health", "params": {"team_name": "Lovelace", "weeks": "12"}},
        ctx,
    )
    call = patch_query_service["execute"][-1]
    assert call["params"] == {"team_name": "Lovelace", "weeks": "12"}


def test_handler_passes_limit(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    RUN_SAVED_QUERY.handler({"name": "x", "limit": 100}, ctx)
    assert patch_query_service["execute"][-1]["limit"] == 100


def test_handler_defaults_limit_to_1000(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    RUN_SAVED_QUERY.handler({"name": "x"}, ctx)
    assert patch_query_service["execute"][-1]["limit"] == 1000


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


def test_handler_returns_failure_when_query_missing(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({"name": "missing"}, ctx)
    assert not result.success
    assert "not found" in result.payload["error"]


def test_handler_swallows_runtime_errors(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({"name": "broken"}, ctx)
    assert not result.success
    assert "synthetic compile error" in result.payload["error"]


def test_handler_rejects_missing_name(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({}, ctx)
    assert not result.success
    assert "name" in result.payload["error"]


def test_handler_rejects_whitespace_name(patch_query_service, tmp_path):
    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({"name": "   "}, ctx)
    assert not result.success


def test_handler_fails_cleanly_when_no_model_dir(patch_query_service):
    """ToolContext default has model_dir=None; tool should fail not crash."""
    result = RUN_SAVED_QUERY.handler({"name": "x"}, ToolContext())
    assert not result.success
    assert "model_dir" in result.payload["error"]


def test_handler_survives_get_query_failure(monkeypatch, tmp_path):
    """get_query exists for provenance YAML; if it fails the result still works."""
    import sys
    import types

    def execute_query(model_dir, name, **kwargs):
        return {"columns": ["x"], "rows": [[1]], "row_count": 1,
                "total_row_count": 1, "sql": "SELECT 1", "truncated": False}

    def get_query(model_dir, name):
        raise RuntimeError("synthetic get_query failure")

    stub = types.SimpleNamespace(execute_query=execute_query, get_query=get_query)
    monkeypatch.setitem(sys.modules, "api.services.query_service", stub)
    import api.services
    monkeypatch.setattr(api.services, "query_service", stub, raising=False)

    ctx = ToolContext(model_id="demo", model_dir=tmp_path)
    result = RUN_SAVED_QUERY.handler({"name": "x"}, ctx)
    # Execute succeeded, get_query failed → result still success, no YAML
    assert result.success
    assert result.payload["rows"] == [[1]]
    assert result.extras["querydef_yaml"] is None


# ---------------------------------------------------------------------------
# Tool metadata
# ---------------------------------------------------------------------------


def test_tool_definition_carries_expected_metadata():
    assert RUN_SAVED_QUERY.name == "run_saved_query"
    assert "query_authoring" in RUN_SAVED_QUERY.tags  # exposed to text2query
    assert "reuse" in RUN_SAVED_QUERY.tags
    assert RUN_SAVED_QUERY.parameters_schema["required"] == ["name"]


def test_default_registry_includes_run_saved_query():
    from semantic.llm.tools import build_default_registry
    reg = build_default_registry()
    assert reg.get("run_saved_query") is not None
    qa_names = {t.name for t in reg.list(tags=("query_authoring",))}
    assert {"build_and_run_query", "find_artifacts", "run_saved_query"} <= qa_names


def test_summarize_renders_one_line_trace():
    from semantic.llm.tools.registry import ToolResult
    from semantic.llm.tools.run_saved_query import _summarize
    summary = _summarize(ToolResult(payload={
        "query_name": "dora_clt_by_team",
        "row_count": 18,
        "columns": ["team", "avg_clt"],
    }))
    assert "dora_clt_by_team" in summary
    assert "18 rows" in summary
    assert "team" in summary
