# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the find_artifacts Tool handler (Epic 20 VG-232)."""

from __future__ import annotations

from dataclasses import dataclass

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
