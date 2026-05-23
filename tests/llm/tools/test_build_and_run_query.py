# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the build_and_run_query Tool handler (Epic 20 VG-231)."""

from __future__ import annotations

from semantic.llm.text2query import QueryExecutionResult
from semantic.llm.tools.build_and_run_query import BUILD_AND_RUN_QUERY
from semantic.llm.tools.registry import ToolContext


class _FakeExecutor:
    def __init__(self, result: QueryExecutionResult):
        self.result = result
        self.received_queries = []

    def execute(self, query):
        self.received_queries.append(query)
        return self.result


def test_handler_returns_success_payload_and_extras_on_happy_path():
    executor = _FakeExecutor(QueryExecutionResult(
        success=True, rows=[[42]], columns=["n"], row_count=1,
        sql="SELECT COUNT(...) FROM ...",
    ))
    ctx = ToolContext(executor=executor)
    args = {
        "root_entity": "PullRequest",
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }

    result = BUILD_AND_RUN_QUERY.handler(args, ctx)

    assert result.success
    # LLM-visible payload — rows and columns; no SQL or QueryDef
    assert result.payload["rows"] == [[42]]
    assert result.payload["columns"] == ["n"]
    assert "sql" not in result.payload
    # Orchestrator-only extras carry the QueryDef + YAML + SQL
    assert result.extras["querydef"].entity == "PullRequest"
    assert "name: text2query" in result.extras["querydef_yaml"]
    assert result.extras["sql"].startswith("SELECT")
    assert executor.received_queries[0].entity == "PullRequest"


def test_handler_returns_failure_when_executor_fails():
    executor = _FakeExecutor(QueryExecutionResult(
        success=False, error="Entity 'X' not found",
    ))
    ctx = ToolContext(executor=executor)
    args = {"root_entity": "X"}

    result = BUILD_AND_RUN_QUERY.handler(args, ctx)

    assert not result.success
    assert "Entity 'X' not found" in result.payload["error"]


def test_handler_returns_failure_when_args_invalid():
    """Bad measure names (with spaces) are caught at build_querydef time."""
    executor = _FakeExecutor(QueryExecutionResult(success=True))
    ctx = ToolContext(executor=executor)
    args = {
        "root_entity": "PullRequest",
        "measures": [{"name": "PR Count", "field": "pull_request_key", "rollup": "count"}],
    }

    result = BUILD_AND_RUN_QUERY.handler(args, ctx)

    assert not result.success
    assert "PR Count" in result.payload["error"]


def test_handler_fails_cleanly_when_no_executor_in_context():
    result = BUILD_AND_RUN_QUERY.handler({"root_entity": "X"}, ToolContext())
    assert not result.success
    assert "no executor" in result.payload["error"]


def test_tool_definition_carries_expected_metadata():
    assert BUILD_AND_RUN_QUERY.name == "build_and_run_query"
    assert "query_authoring" in BUILD_AND_RUN_QUERY.tags
    assert BUILD_AND_RUN_QUERY.terminal is False
    schema = BUILD_AND_RUN_QUERY.parameters_schema
    assert "root_entity" in schema["properties"]
    assert "root_entity" in schema["required"]
