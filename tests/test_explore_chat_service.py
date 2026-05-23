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


def test_chat_turn_happy_path(monkeypatch):
    _stub_loaders(monkeypatch)
    llm = FakeLLMClient()
    # text2query: build_and_run_query → executor success → success
    llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "Widget",
        "measures": [{"name": "n", "field": "widget_key", "rollup": "count"}],
    }))
    # text2view: present_view
    llm.responses.append(response_with_tool("present_view", {
        "chart_type": "kpi",
        "y_field": "n",
        "caption": "42 widgets total.",
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
    assert result.content == "42 widgets total."
    assert result.chart_type == "kpi"
    assert result.y_field == "n"
    assert result.rows == [[42]]
    assert result.columns == ["n"]
    assert result.row_count == 1
    assert result.query_yaml is not None
    assert result.view_yaml is not None
    assert result.sql.startswith("SELECT")
    assert result.iterations == 1


def test_chat_turn_failure_returns_error_without_calling_text2view(monkeypatch):
    _stub_loaders(monkeypatch)
    llm = FakeLLMClient()
    # text2query: LLM gives up — text response, no tool calls
    llm.responses.append(response_text("I can't build that query."))
    executor = _FakeExecutor([])

    result = chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="impossible question",
        llm_client=llm, executor=executor,
    )

    assert not result.success
    assert result.error is not None
    # Only the one text2query LLM call — text2view never reached
    assert len(llm.received) == 1


def test_chat_turn_falls_back_to_table_when_text2view_fails(monkeypatch):
    _stub_loaders(monkeypatch)
    llm = FakeLLMClient()
    llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "Widget",
        "measures": [{"name": "n", "field": "widget_key", "rollup": "count"}],
    }))
    # text2view: LLM responds with text instead of calling present_view
    llm.responses.append(response_text("not sure"))
    executor = _FakeExecutor([QueryExecutionResult(
        success=True, rows=[[42]], columns=["n"], row_count=1,
    )])

    result = chat_turn(
        model_dir=Path("/fake/widget_model"),
        message="count widgets",
        llm_client=llm, executor=executor,
    )

    # Partial success: query worked, chart picker fell back to table.
    assert result.success
    assert result.chart_type == "table"
    assert result.rows == [[42]]
    assert "Chart selection failed" in result.content


def test_chat_turn_passes_history_through_to_text2query(monkeypatch):
    _stub_loaders(monkeypatch)
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

    # The first LLM call (text2query) should have system + history (2) + user = 4 messages.
    first_call_msgs = llm.received[0]["messages"]
    assert len(first_call_msgs) == 4
    assert first_call_msgs[1]["content"] == "first question"
    assert first_call_msgs[2]["content"] == "first answer"
    assert first_call_msgs[3]["content"] == "follow-up"


def test_chat_turn_uses_view_spec_when_run_saved_view_succeeded(monkeypatch):
    """VG-234: text2view's chart pick is overridden by a saved view's spec."""
    _stub_loaders(monkeypatch)

    # Simulate a text2query result that came from run_saved_view: it
    # carries a view_spec with the saved chart shape.
    from semantic.llm.text2query import Text2QueryResult
    from semantic.llm.text2view import Text2ViewResult

    def fake_text2query(**kwargs):
        return Text2QueryResult(
            success=True,
            yaml="name: dora_clt_q\nroot: PullRequest\n",
            rows=[["alpha", 12.0]],
            columns=["team", "avg_clt"],
            row_count=1,
            sql="SELECT ...",
            iterations=1,
            # Saved view spec — bar chart (text2view would have said line)
            view_spec={
                "chart_type": "bar",
                "x_field": "team",
                "y_field": "avg_clt",
                "color_field": None,
                "drilldown": None,
            },
            view_yaml="name: dora_clt_by_team\ntype: chart\n",
            saved_view_name="dora_clt_by_team",
        )

    def fake_text2view(**kwargs):
        # text2view would pick LINE (different from the saved bar) and
        # writes its own caption. Orchestrator should use the caption
        # but override the chart shape with the view_spec.
        return Text2ViewResult(
            success=True,
            chart_type="line",
            x_field="team", y_field="avg_clt", color_field=None,
            caption="Alpha team leads at 12.",
            yaml="name: text2view\ntype: chart\n",
        )

    monkeypatch.setattr(
        "api.services.explore_chat.text2query_yaml", fake_text2query,
    )
    monkeypatch.setattr(
        "api.services.explore_chat.text2view_yaml", fake_text2view,
    )
    # Skip view validation — saved view YAML is already validated.
    monkeypatch.setattr(
        "api.services.explore_chat.view_service.validate_inline_view",
        lambda *a, **k: {"valid": True, "errors": []},
    )

    result = chat_turn(
        model_dir=Path("/fake/m"), message="show me dora clt by team",
        llm_client=FakeLLMClient(), executor=_FakeExecutor([]),
    )

    assert result.success
    # Chart shape from the SAVED VIEW, not text2view's pick:
    assert result.chart_type == "bar"
    assert result.x_field == "team"
    assert result.y_field == "avg_clt"
    # Caption still from text2view (data-aware):
    assert result.content == "Alpha team leads at 12."
    # View YAML is the saved one, not text2view's:
    assert "dora_clt_by_team" in result.view_yaml


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
