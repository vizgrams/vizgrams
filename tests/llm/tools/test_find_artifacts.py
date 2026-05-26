# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the find_artifacts Tool handler (Epic 20 VG-232)."""

from __future__ import annotations

from dataclasses import dataclass, field

from semantic.llm.embeddings.search import SearchHit
from semantic.llm.tools.find_artifacts import FIND_ARTIFACTS
from semantic.llm.tools.registry import ToolContext


@dataclass
class _FakeSearch:
    """Minimal ``SemanticSearch`` stand-in — records calls + returns canned hits."""

    hits: list[SearchHit] = None
    raises: Exception | None = None
    received_calls: list[dict] = None

    def __post_init__(self):
        self.hits = self.hits or []
        self.received_calls = []

    def find(self, query, *, model_id, kinds=None, top_k=5, max_distance=None):
        self.received_calls.append({
            "query": query, "model_id": model_id,
            "kinds": kinds, "top_k": top_k,
        })
        if self.raises:
            raise self.raises
        return self.hits


def _hit(kind: str, name: str, description: str, distance: float) -> SearchHit:
    return SearchHit(kind=kind, name=name, description=description, distance=distance)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_handler_returns_matches_formatted_for_llm():
    search = _FakeSearch(hits=[
        _hit("query", "pr_throughput", "weekly count of merged PRs", 0.12),
        _hit("view", "pr_trend", "line chart of PR throughput", 0.18),
    ])
    ctx = ToolContext(model_id="demo", search=search)

    result = FIND_ARTIFACTS.handler({"query": "weekly PR throughput"}, ctx)

    assert result.success
    assert result.payload["count"] == 2
    assert result.payload["matches"] == [
        {"kind": "query", "name": "pr_throughput",
         "description": "weekly count of merged PRs", "distance": 0.12},
        {"kind": "view", "name": "pr_trend",
         "description": "line chart of PR throughput", "distance": 0.18},
    ]
    # Args forwarded to SemanticSearch correctly
    assert search.received_calls == [{
        "query": "weekly PR throughput", "model_id": "demo",
        "kinds": None, "top_k": 5,
    }]


def test_handler_rounds_distance_to_3_decimal_places():
    search = _FakeSearch(hits=[_hit("query", "x", "...", 0.12345678)])
    ctx = ToolContext(model_id="demo", search=search)
    result = FIND_ARTIFACTS.handler({"query": "x"}, ctx)
    assert result.payload["matches"][0]["distance"] == 0.123


def test_handler_passes_kind_filter_to_search():
    search = _FakeSearch(hits=[])
    ctx = ToolContext(model_id="demo", search=search)
    FIND_ARTIFACTS.handler({"query": "x", "kind": "view"}, ctx)
    assert search.received_calls[0]["kinds"] == ["view"]


def test_handler_ignores_invalid_kind_silently():
    """The JSON schema enforces the enum at the LLM layer; the handler
    defends against malformed kinds by treating them as 'no filter'."""
    search = _FakeSearch(hits=[])
    ctx = ToolContext(model_id="demo", search=search)
    FIND_ARTIFACTS.handler({"query": "x", "kind": "mapper"}, ctx)
    assert search.received_calls[0]["kinds"] is None


def test_handler_honours_top_k():
    search = _FakeSearch(hits=[])
    ctx = ToolContext(model_id="demo", search=search)
    FIND_ARTIFACTS.handler({"query": "x", "top_k": 3}, ctx)
    assert search.received_calls[0]["top_k"] == 3


def test_handler_defaults_top_k_to_5():
    search = _FakeSearch(hits=[])
    ctx = ToolContext(model_id="demo", search=search)
    FIND_ARTIFACTS.handler({"query": "x"}, ctx)
    assert search.received_calls[0]["top_k"] == 5


# ---------------------------------------------------------------------------
# Degraded paths
# ---------------------------------------------------------------------------


def test_handler_returns_warning_when_search_not_configured():
    """No ctx.search → chat should still work; tool returns empty matches + warning."""
    result = FIND_ARTIFACTS.handler({"query": "anything"}, ToolContext(model_id="demo"))
    assert result.success
    assert result.payload["matches"] == []
    assert "warning" in result.payload
    assert "not configured" in result.payload["warning"]


def test_handler_returns_failure_on_empty_query():
    ctx = ToolContext(model_id="demo", search=_FakeSearch())
    result = FIND_ARTIFACTS.handler({"query": ""}, ctx)
    assert not result.success
    assert result.payload["matches"] == []
    assert "empty query" in result.payload["error"]


def test_handler_strips_whitespace_query():
    ctx = ToolContext(model_id="demo", search=_FakeSearch())
    result = FIND_ARTIFACTS.handler({"query": "   \n  "}, ctx)
    assert not result.success
    assert "empty query" in result.payload["error"]


def test_handler_swallows_search_failures():
    """A flaky CH / embedder must not crash a chat turn."""
    search = _FakeSearch(raises=RuntimeError("CH unavailable"))
    ctx = ToolContext(model_id="demo", search=search)
    result = FIND_ARTIFACTS.handler({"query": "x"}, ctx)
    assert not result.success
    assert "CH unavailable" in result.payload["error"]
    assert result.payload["matches"] == []


# ---------------------------------------------------------------------------
# Tool metadata
# ---------------------------------------------------------------------------


def test_tool_definition_carries_expected_metadata():
    assert FIND_ARTIFACTS.name == "find_artifacts"
    assert "query_authoring" in FIND_ARTIFACTS.tags  # exposed to text2query
    assert "catalog" in FIND_ARTIFACTS.tags
    assert FIND_ARTIFACTS.terminal is False
    schema = FIND_ARTIFACTS.parameters_schema
    assert "query" in schema["required"]
    assert set(schema["properties"]["kind"]["enum"]) == {
        "query", "view", "feature", "entity", "application",
    }


def test_default_registry_includes_find_artifacts():
    """Regression: the default registry must expose find_artifacts so
    text2query sees it alongside build_and_run_query."""
    from semantic.llm.tools import build_default_registry
    reg = build_default_registry()
    assert reg.get("find_artifacts") is not None
    qa_names = {t.name for t in reg.list(tags=("query_authoring",))}
    assert {"build_and_run_query", "find_artifacts"} <= qa_names


# ---------------------------------------------------------------------------
# Shape enrichment — view/query hits carry chart_type / root / measures so
# the LLM can judge fit before reusing. Without these, run_saved_view fires
# on weak name matches (the failure mode the 2026-05-26 eval surfaced).
# ---------------------------------------------------------------------------


@dataclass
class _FakeView:
    type: str
    query: str
    visualization: dict = field(default_factory=dict)


@dataclass
class _FakeQuery:
    entity: str
    metrics: dict = field(default_factory=dict)
    parameters: list = field(default_factory=list)


def test_view_hit_enriched_with_chart_type_root_and_measures(monkeypatch, tmp_path):
    """A bar-chart view rooted on PullRequest with one measure surfaces all three."""
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_view",
        lambda name, views_dir: _FakeView(
            type="chart", query="pr_trend_q",
            visualization={"chart_type": "bar", "x": "team", "y": ["pr_count"]},
        ),
    )
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query",
        lambda name, queries_dir: _FakeQuery(
            entity="PullRequest", metrics={"pr_count": object()},
        ),
    )
    search = _FakeSearch(hits=[_hit("view", "pr_trend", "...", 0.12)])
    ctx = ToolContext(model_id="demo", search=search, model_dir=tmp_path)

    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]

    assert match["chart_type"] == "bar"
    assert match["root"] == "PullRequest"
    assert match["measures"] == ["pr_count"]


def test_view_hit_metric_type_surfaces_as_kpi(monkeypatch, tmp_path):
    """View.type='metric' flattens to chart_type='kpi' so the LLM can see it."""
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_view",
        lambda name, views_dir: _FakeView(type="metric", query="total_q"),
    )
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query",
        lambda name, queries_dir: _FakeQuery(entity="PullRequest", metrics={"total": object()}),
    )
    ctx = ToolContext(model_id="demo", search=_FakeSearch(hits=[_hit("view", "kpi_v", "...", 0.1)]),
                      model_dir=tmp_path)
    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]
    assert match["chart_type"] == "kpi"


def test_view_hit_table_type_surfaces_as_table(monkeypatch, tmp_path):
    """View.type='table' flattens to chart_type='table' so the LLM stops reusing
    tables for questions that imply a chart."""
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_view",
        lambda name, views_dir: _FakeView(type="table", query="rows_q"),
    )
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query",
        lambda name, queries_dir: _FakeQuery(entity="PullRequest"),
    )
    ctx = ToolContext(model_id="demo", search=_FakeSearch(hits=[_hit("view", "t", "...", 0.1)]),
                      model_dir=tmp_path)
    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]
    assert match["chart_type"] == "table"


def test_query_hit_enriched_with_root_measures_and_has_params(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query",
        lambda name, queries_dir: _FakeQuery(
            entity="Person", metrics={"pr_count": object()}, parameters=["lookback_weeks"],
        ),
    )
    ctx = ToolContext(model_id="demo", search=_FakeSearch(hits=[_hit("query", "people", "...", 0.1)]),
                      model_dir=tmp_path)
    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]
    assert match["root"] == "Person"
    assert match["measures"] == ["pr_count"]
    assert match["has_params"] is True


def test_enrichment_skipped_when_no_model_dir():
    """Tests without a model_dir continue to see the bare hit payload."""
    search = _FakeSearch(hits=[_hit("view", "pr_trend", "...", 0.1)])
    ctx = ToolContext(model_id="demo", search=search)  # no model_dir
    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]
    assert "chart_type" not in match
    assert "root" not in match


def test_enrichment_degrades_when_yaml_loader_returns_none(monkeypatch, tmp_path):
    """Missing artifact (loader returns None) → no extra keys, no crash."""
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_view",
        lambda name, views_dir: None,
    )
    ctx = ToolContext(model_id="demo", search=_FakeSearch(hits=[_hit("view", "gone", "...", 0.1)]),
                      model_dir=tmp_path)
    match = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"][0]
    assert match == {"kind": "view", "name": "gone", "description": "...", "distance": 0.1}


def test_enrichment_swallows_loader_exceptions(monkeypatch, tmp_path):
    """A broken model dir must not crash a chat turn."""
    def _boom(*args, **kwargs):
        raise RuntimeError("disk full")
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query", _boom,
    )
    ctx = ToolContext(model_id="demo", search=_FakeSearch(hits=[_hit("query", "bad", "...", 0.1)]),
                      model_dir=tmp_path)
    result = FIND_ARTIFACTS.handler({"query": "x"}, ctx)
    assert result.success
    assert result.payload["matches"][0]["name"] == "bad"
    assert "root" not in result.payload["matches"][0]


def test_non_view_non_query_kinds_not_enriched(monkeypatch, tmp_path):
    """feature/entity/application hits don't need shape info — keep payload lean."""
    called = []
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_view",
        lambda *a, **kw: called.append("view") or None,
    )
    monkeypatch.setattr(
        "semantic.llm.tools.find_artifacts.YAMLAdapter.load_query",
        lambda *a, **kw: called.append("query") or None,
    )
    ctx = ToolContext(
        model_id="demo",
        search=_FakeSearch(hits=[
            _hit("feature", "f1", "...", 0.1),
            _hit("entity", "e1", "...", 0.1),
            _hit("application", "a1", "...", 0.1),
        ]),
        model_dir=tmp_path,
    )
    matches = FIND_ARTIFACTS.handler({"query": "x"}, ctx).payload["matches"]
    assert called == []
    for m in matches:
        assert set(m.keys()) == {"kind", "name", "description", "distance"}
