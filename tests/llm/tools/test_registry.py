# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Tool / ToolContext / ToolRegistry primitives (VG-231)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from semantic.llm.tools.registry import (
    Tool,
    ToolContext,
    ToolRegistry,
    ToolResult,
    build_default_registry,
)

# ---------------------------------------------------------------------------
# ToolResult
# ---------------------------------------------------------------------------


def test_tool_result_to_tool_message_content_truncates_rows():
    r = ToolResult(payload={
        "columns": ["x"],
        "rows": [[i] for i in range(100)],
        "row_count": 100,
    })
    out = json.loads(r.to_tool_message_content(max_rows=10))
    assert len(out["rows"]) == 10
    assert out["truncated"] is True
    assert out["rows_shown"] == 10
    assert out["row_count"] == 100  # unchanged — orchestrator-facing total


def test_tool_result_passes_short_rows_through_untruncated():
    r = ToolResult(payload={"rows": [[1], [2]], "row_count": 2})
    out = json.loads(r.to_tool_message_content(max_rows=10))
    assert out["rows"] == [[1], [2]]
    assert "truncated" not in out


def test_tool_result_extras_not_in_message_content():
    """Internal-only fields stored in extras must not leak to the LLM."""
    r = ToolResult(
        payload={"columns": ["x"], "rows": [[1]], "row_count": 1},
        extras={"querydef": object(), "sql": "SELECT ..."},
    )
    rendered = r.to_tool_message_content()
    assert "querydef" not in rendered
    assert "SELECT" not in rendered


def test_tool_result_handles_non_json_serialisable_with_default_str():
    r = ToolResult(payload={"when": Path("/tmp")})
    out = r.to_tool_message_content()
    # Path serialises via default=str rather than raising
    assert "/tmp" in out


# ---------------------------------------------------------------------------
# ToolContext
# ---------------------------------------------------------------------------


def test_tool_context_defaults_are_all_optional():
    """Tests should be able to construct ToolContext() with no args."""
    ctx = ToolContext()
    assert ctx.model_id == ""
    assert ctx.model_dir is None
    assert ctx.executor is None
    assert ctx.search is None
    assert ctx.extras == {}


# ---------------------------------------------------------------------------
# ToolRegistry — registration / lookup
# ---------------------------------------------------------------------------


def _noop_handler(args: dict, ctx: ToolContext) -> ToolResult:  # noqa: ARG001
    return ToolResult(payload={})


def _tool(name: str, *, tags: tuple[str, ...] = ()) -> Tool:
    return Tool(
        name=name, description=f"the {name} tool",
        parameters_schema={"type": "object"},
        handler=_noop_handler, tags=tags,
    )


def test_register_and_get():
    reg = ToolRegistry()
    t = _tool("a")
    reg.register(t)
    assert reg.get("a") is t
    assert reg.get("nope") is None


def test_register_duplicate_raises():
    reg = ToolRegistry()
    reg.register(_tool("a"))
    with pytest.raises(ValueError, match="already registered"):
        reg.register(_tool("a"))


def test_list_returns_all_by_default():
    reg = ToolRegistry()
    reg.register(_tool("a"))
    reg.register(_tool("b"))
    assert {t.name for t in reg.list()} == {"a", "b"}


def test_list_filters_by_tag():
    reg = ToolRegistry()
    reg.register(_tool("a", tags=("query_authoring",)))
    reg.register(_tool("b", tags=("view_selection",)))
    reg.register(_tool("c", tags=("query_authoring", "catalog")))

    qa = {t.name for t in reg.list(tags=("query_authoring",))}
    assert qa == {"a", "c"}

    catalog = {t.name for t in reg.list(tags=("catalog",))}
    assert catalog == {"c"}


def test_names_returns_all_in_registration_order():
    reg = ToolRegistry()
    reg.register(_tool("a"))
    reg.register(_tool("b"))
    assert reg.names() == ["a", "b"]


# ---------------------------------------------------------------------------
# Rendering for LLM providers
# ---------------------------------------------------------------------------


def test_to_openai_tools_uses_function_envelope():
    reg = ToolRegistry()
    reg.register(Tool(
        name="x", description="d",
        parameters_schema={"type": "object", "properties": {"a": {"type": "string"}}},
        handler=_noop_handler,
    ))
    out = reg.to_openai_tools()
    assert out == [{
        "type": "function",
        "function": {
            "name": "x",
            "description": "d",
            "parameters": {"type": "object", "properties": {"a": {"type": "string"}}},
        },
    }]


def test_to_mcp_definitions_uses_input_schema_envelope():
    reg = ToolRegistry()
    reg.register(Tool(
        name="x", description="d",
        parameters_schema={"type": "object"},
        handler=_noop_handler,
    ))
    out = reg.to_mcp_definitions()
    assert out == [{"name": "x", "description": "d", "inputSchema": {"type": "object"}}]


def test_render_filters_by_names_and_tags():
    reg = ToolRegistry()
    reg.register(_tool("a", tags=("g1",)))
    reg.register(_tool("b", tags=("g2",)))
    reg.register(_tool("c", tags=("g1",)))

    by_name = reg.to_openai_tools(names=["a", "b"])
    assert {t["function"]["name"] for t in by_name} == {"a", "b"}

    by_tag = reg.to_openai_tools(tags=("g1",))
    assert {t["function"]["name"] for t in by_tag} == {"a", "c"}


def test_render_with_unknown_names_silently_skips():
    """Unknown names in the `names` filter should be dropped, not raise."""
    reg = ToolRegistry()
    reg.register(_tool("a"))
    out = reg.to_openai_tools(names=["a", "nonexistent"])
    assert {t["function"]["name"] for t in out} == {"a"}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def test_dispatch_invokes_handler_with_args_and_ctx():
    received = []

    def handler(args, ctx):
        received.append((args, ctx))
        return ToolResult(payload={"got": args})

    reg = ToolRegistry()
    reg.register(Tool(
        name="echo", description="d", parameters_schema={}, handler=handler,
    ))
    ctx = ToolContext(model_id="demo")
    result = reg.dispatch("echo", {"hello": "world"}, ctx)

    assert result.payload == {"got": {"hello": "world"}}
    assert received == [({"hello": "world"}, ctx)]


def test_dispatch_unknown_tool_raises_keyerror():
    reg = ToolRegistry()
    with pytest.raises(KeyError, match="missing"):
        reg.dispatch("missing", {}, ToolContext())


# ---------------------------------------------------------------------------
# Default registry
# ---------------------------------------------------------------------------


def test_default_registry_includes_build_and_run_query_and_present_view():
    reg = build_default_registry()
    names = set(reg.names())
    assert "build_and_run_query" in names
    assert "present_view" in names


def test_default_registry_tags_are_set():
    reg = build_default_registry()
    assert "query_authoring" in reg.get("build_and_run_query").tags
    assert "view_selection" in reg.get("present_view").tags


def test_default_registry_present_view_marked_terminal():
    reg = build_default_registry()
    assert reg.get("present_view").terminal is True
    assert reg.get("build_and_run_query").terminal is False
