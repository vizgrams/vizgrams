# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api.services.explore_chat orchestrator."""

from __future__ import annotations

from pathlib import Path

from api.services.explore_chat import (
    SemanticLayerExecutor,
    _history_to_openai,
    chat_turn,
)
from semantic.llm.text2query import QueryExecutionResult
from semantic.query import PaginationDef, QueryAttribute, QueryDef
from tests.llm.conftest import FakeLLMClient, response_text, response_with_tool

# ---------------------------------------------------------------------------
# _history_to_openai — message-shape translation
# ---------------------------------------------------------------------------


def test_history_filters_unknown_roles():
    history = [
        {"role": "user", "content": "hi"},
        {"role": "system", "content": "ignored"},
        {"role": "assistant", "content": "ok"},
        {"role": "tool", "content": "also ignored"},
    ]
    assert _history_to_openai(history) == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "ok"},
    ]


def test_history_skips_empty_content():
    history = [
        {"role": "user", "content": ""},
        {"role": "assistant", "content": "ok"},
    ]
    assert _history_to_openai(history) == [{"role": "assistant", "content": "ok"}]


def test_history_handles_none_input():
    assert _history_to_openai(None) == []
    assert _history_to_openai([]) == []


# ---------------------------------------------------------------------------
# chat_turn — happy path with both LLM calls mocked
# ---------------------------------------------------------------------------


def _stub_loaders(monkeypatch):
    """Patch the YAMLAdapter loaders + schema_context so tests don't need a real model."""
    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_entities", lambda _p: [],
    )
    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_features", lambda _p: [],
    )
    monkeypatch.setattr(
        "api.services.explore_chat.build_schema_context",
        lambda *args, **kwargs: "STUB SCHEMA",
    )


class _FakeExecutor:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def execute(self, query: QueryDef) -> QueryExecutionResult:
        self.calls.append(query)
        return self.results.pop(0)


# ---------------------------------------------------------------------------
# chat_turn — single agentic tool loop (replaces the prior two-phase
# text2query → text2view orchestration; the four tests below cover the
# three terminal paths + the "LLM gave up" failure mode against the
# unified loop directly).
# ---------------------------------------------------------------------------


def _skip_view_validation(monkeypatch):
    """Validator needs a real saved query to resolve columns; stub it out
    so tests can exercise the orchestrator without dragging in the DB."""
    monkeypatch.setattr(
        "api.services.explore_chat.view_service.validate_inline_view",
        lambda *a, **k: {"valid": True, "errors": []},
    )


def _patch_run_saved_view(monkeypatch, *, name: str, view_yaml: str, query_yaml: str):
    """Stub view_service.execute_view + get_view so the run_saved_view
    tool succeeds when invoked, returning the canned shape."""
    monkeypatch.setattr(
        "api.services.view_service.execute_view",
        lambda model_dir, view_name, **kw: {
            "name": view_name, "type": "chart", "query": "underlying_q",
            "columns": ["team", "avg_clt"], "rows": [["alpha", 12.0]],
            "row_count": 1, "total_row_count": 1,
            "visualization": {"chart_type": "bar", "x": "team", "y": ["avg_clt"]},
            "sql": "SELECT ...",
            "params": [],
        },
    )
    monkeypatch.setattr(
        "api.services.view_service.get_view",
        lambda model_dir, view_name: {"raw_yaml": view_yaml},
    )
    monkeypatch.setattr(
        "api.services.query_service.get_query",
        lambda model_dir, query_name: {"raw_yaml": query_yaml},
    )


def _patch_run_saved_query(monkeypatch, *, name: str, query_yaml: str):
    """Stub query_service.execute_query + get_query so run_saved_query
    succeeds when invoked."""
    monkeypatch.setattr(
        "api.services.query_service.execute_query",
        lambda model_dir, query_name, **kw: {
            "columns": ["author", "n"], "rows": [["alice", 42]],
            "row_count": 1, "total_row_count": 1,
            "sql": "SELECT ...",
        },
    )
    monkeypatch.setattr(
        "api.services.query_service.get_query",
        lambda model_dir, query_name: {"raw_yaml": query_yaml},
    )


def test_chat_turn_path_c_build_then_present_returns_inline_view(monkeypatch):
    """Path C — LLM authors a fresh query then picks a chart.
    Wrapper view yaml + inline query yaml both come back."""
    _stub_loaders(monkeypatch)
    _skip_view_validation(monkeypatch)
    llm = FakeLLMClient()
    llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "Widget",
        "measures": [{"name": "n", "field": "widget_key", "rollup": "count"}],
    }))
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "kpi", "y_field": "n", "caption": "42 widgets.",
    }))
    executor = _FakeExecutor([QueryExecutionResult(
        success=True, rows=[[42]], columns=["n"], row_count=1, sql="SELECT ...",
    )])

    result = chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="how many widgets?",
        llm_client=llm, executor=executor,
    )

    assert result.success
    assert result.saved_view is None
    assert result.inline_view is not None
    assert result.inline_view["view_yaml"]
    assert result.inline_view["query_yaml"]    # inline query carried — path C
    assert result.iterations == 2              # two LLM calls: build, present
    assert result.query_yaml is not None
    assert result.view_yaml is not None
    assert result.sql and result.sql.startswith("SELECT")


def test_chat_turn_failure_when_llm_stops_without_terminal_tool(monkeypatch):
    """If the LLM emits text instead of a tool call before any terminal
    tool runs, the turn fails — there's no view to render."""
    _stub_loaders(monkeypatch)
    llm = FakeLLMClient()
    llm.responses.append(response_text("I can't build that query."))

    result = chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="impossible question",
        llm_client=llm, executor=_FakeExecutor([]),
    )

    assert not result.success
    assert result.error is not None
    assert "I can't" in result.error or "stopped" in result.error
    assert len(llm.received) == 1


def test_chat_turn_path_a_run_saved_view_terminates_without_present_view(monkeypatch):
    """Path A — LLM finds a saved view and runs it. Result is a saved_view
    ref with no inline_view. present_view is NEVER called (saved views
    carry their own chart spec)."""
    _stub_loaders(monkeypatch)
    _patch_run_saved_view(
        monkeypatch,
        name="dora_clt_by_team",
        view_yaml="name: dora_clt_by_team\ntype: chart\nquery: underlying_q\n",
        query_yaml="name: underlying_q\nroot: PullRequest\n",
    )

    llm = FakeLLMClient()
    llm.responses.append(response_with_tool("find_artifacts", {"query": "dora clt"}))
    llm.responses.append(response_with_tool("run_saved_view", {"name": "dora_clt_by_team"}))

    result = chat_turn(
        model_dir=Path("/fake/m"),
        message="show me dora clt by team",
        llm_client=llm, executor=_FakeExecutor([]),
    )

    assert result.success
    assert result.saved_view == {"name": "dora_clt_by_team", "params": {}}
    assert result.inline_view is None
    # No third LLM call — the loop exited as soon as run_saved_view
    # succeeded. Two calls only: find_artifacts → run_saved_view.
    assert len(llm.received) == 2


def test_chat_turn_path_b_uses_saved_query_name_in_wrapper_view(monkeypatch):
    """Path B — LLM runs an existing saved query, then picks a chart.
    The wrapper view's ``query:`` field references the saved query's
    actual name (not the "text2query" placeholder)."""
    _stub_loaders(monkeypatch)
    _skip_view_validation(monkeypatch)
    _patch_run_saved_query(
        monkeypatch,
        name="top_pr_authors",
        query_yaml="name: top_pr_authors\nroot: PullRequest\n",
    )

    llm = FakeLLMClient()
    llm.responses.append(response_with_tool("find_artifacts", {"query": "prolific developers"}))
    llm.responses.append(response_with_tool("run_saved_query", {"name": "top_pr_authors"}))
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "bar", "x_field": "author", "y_field": "n",
        "caption": "alice leads with 42.",
    }))

    result = chat_turn(
        model_dir=Path("/fake/m"),
        message="most prolific developers",
        llm_client=llm, executor=_FakeExecutor([]),
    )

    assert result.success
    assert result.inline_view is not None
    # Path B → query already saved, so the inline_view payload doesn't
    # include a transient query yaml.
    assert result.inline_view["query_yaml"] is None
    # The wrapper view's ``query:`` field must reference the saved name.
    assert "query: top_pr_authors" in result.inline_view["view_yaml"]
    assert result.iterations == 3   # find → run_saved_query → present


def test_chat_turn_passes_history_to_llm(monkeypatch):
    """Conversation history (prior user/assistant turns) lands in the
    LLM message list before the current user prompt."""
    _stub_loaders(monkeypatch)
    _skip_view_validation(monkeypatch)
    llm = FakeLLMClient()
    llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "Widget",
        "measures": [{"name": "n", "field": "widget_key", "rollup": "count"}],
    }))
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "bar", "y_field": "n", "caption": "x",
    }))
    executor = _FakeExecutor([QueryExecutionResult(
        success=True, rows=[["a", 1]], columns=["x", "n"], row_count=1,
    )])

    history = [
        {"role": "user", "content": "first question"},
        {"role": "assistant", "content": "first answer"},
    ]
    chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="follow-up",
        history=history,
        llm_client=llm, executor=executor,
    )

    first_call_msgs = llm.received[0]["messages"]
    # system + history(2) + user = 4 messages on the very first call.
    assert len(first_call_msgs) == 4
    assert first_call_msgs[1]["content"] == "first question"
    assert first_call_msgs[2]["content"] == "first answer"
    assert first_call_msgs[3]["content"] == "follow-up"


def test_chat_turn_present_view_before_query_is_recoverable(monkeypatch):
    """If the LLM jumps straight to present_view (no prior query), the
    orchestrator feeds back an error and lets the loop continue so the
    LLM can recover. This catches a regression class where a bad
    sequence would silently produce an empty inline_view."""
    _stub_loaders(monkeypatch)
    _skip_view_validation(monkeypatch)
    llm = FakeLLMClient()
    # First: LLM jumps to present_view (wrong).
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "kpi", "y_field": "n", "caption": "premature.",
    }))
    # Second: LLM recovers — build a query.
    llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "Widget",
        "measures": [{"name": "n", "field": "widget_key", "rollup": "count"}],
    }, call_id="call_2"))
    # Third: LLM presents the result correctly.
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "kpi", "y_field": "n", "caption": "42.",
    }, call_id="call_3"))
    executor = _FakeExecutor([QueryExecutionResult(
        success=True, rows=[[42]], columns=["n"], row_count=1,
    )])

    result = chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="how many widgets?",
        llm_client=llm, executor=executor,
    )

    assert result.success
    assert result.inline_view is not None


# ---------------------------------------------------------------------------
# SemanticLayerExecutor — surface-level error handling
# ---------------------------------------------------------------------------


def test_executor_returns_error_when_entity_not_in_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_entities", lambda _p: [],
    )
    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_features", lambda _p: [],
    )
    qd = QueryDef(
        name="x", entity="NonExistent", detail=True,
        attributes=[QueryAttribute(parts=["a"])],
        pagination=PaginationDef(),
    )
    result = SemanticLayerExecutor(model_dir=tmp_path).execute(qd)
    assert not result.success
    assert "NonExistent" in (result.error or "")


def test_executor_catches_engine_exceptions(tmp_path, monkeypatch):
    """When the engine raises, the executor returns success=False — never propagates."""

    class ExplodingEntities:
        def __iter__(self):
            raise RuntimeError("synthetic engine failure")

    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_entities",
        lambda _p: ExplodingEntities(),
    )
    monkeypatch.setattr(
        "api.services.explore_chat.YAMLAdapter.load_features", lambda _p: [],
    )
    qd = QueryDef(name="x", entity="Whatever", detail=True)
    result = SemanticLayerExecutor(model_dir=tmp_path).execute(qd)
    assert not result.success
    assert "synthetic engine failure" in (result.error or "")
