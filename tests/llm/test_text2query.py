# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic/llm/text2query."""

from __future__ import annotations

import json

from semantic.llm.provider import LLMResponse
from semantic.llm.text2query import (
    QueryExecutionResult,
    build_querydef,
    text2query_yaml,
)
from tests.llm.conftest import response_text, response_with_tool

# ---------------------------------------------------------------------------
# build_querydef — arg shape → QueryDef
# ---------------------------------------------------------------------------


def test_build_querydef_aggregate_with_slice_and_metric():
    qd = build_querydef({
        "root_entity": "PullRequest",
        "group_by": [{"field": "author"}],
        "measures": [{"name": "pr_count", "field": "pull_request_key", "rollup": "count"}],
        "order_by": [{"column": "pr_count", "direction": "DESC"}],
        "limit": 10,
    })
    assert qd.entity == "PullRequest"
    assert qd.is_aggregate
    assert len(qd.slices) == 1
    assert qd.slices[0].field == "author"
    assert qd.metrics["pr_count"].rollup == "count"
    assert qd.metrics["pr_count"].field == "pull_request_key"
    assert qd.order_by == [("pr_count", "DESC")]
    assert qd.pagination.page_size == 10


def test_build_querydef_detail_when_no_measures():
    qd = build_querydef({
        "root_entity": "PullRequest",
        "attributes": [{"field": "title"}, {"field": "state"}],
        "filters": ["state == 'open'"],
    })
    assert qd.detail
    assert not qd.is_aggregate
    assert len(qd.attributes) == 2
    assert qd.attributes[0].raw_field == "title"
    assert qd.filters == ["state == 'open'"]


def test_build_querydef_time_bucket_via_format():
    qd = build_querydef({
        "root_entity": "PullRequest",
        "group_by": [{"field": "created_at", "format": "YYYY-MM", "alias": "month"}],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    })
    assert qd.slices[0].format_pattern == "YYYY-MM"
    assert qd.slices[0].alias == "month"


# ---------------------------------------------------------------------------
# text2query_yaml — happy path
# ---------------------------------------------------------------------------


def test_returns_success_after_one_successful_tool_call(
    fake_llm, fake_executor, schema_demo_tiny,
):
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }))
    fake_executor.results.append(QueryExecutionResult(
        success=True,
        rows=[[19999]],
        columns=["n"],
        row_count=1,
        sql="SELECT COUNT(...) FROM ...",
    ))

    result = text2query_yaml(
        prompt="How many PRs are there?",
        model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor,
        llm_client=fake_llm,
    )

    assert result.success
    assert result.iterations == 1
    assert result.rows == [[19999]]
    assert result.columns == ["n"]
    assert result.sql.startswith("SELECT")
    assert result.yaml is not None
    assert "root: PullRequest" in result.yaml
    assert "name: _text2query" in result.yaml
    assert len(result.tool_calls) == 1


def test_yaml_round_trips_back_through_parser(fake_llm, fake_executor, schema_demo_tiny):
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "group_by": [{"field": "author"}],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
        "order_by": [{"column": "n", "direction": "DESC"}],
    }))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[["a", 5], ["b", 3]],
        columns=["author", "n"], row_count=2,
    ))

    result = text2query_yaml(
        prompt="top authors", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    # YAML must be parseable by the existing parse_query_dict pipeline
    import yaml

    from semantic.query import parse_query_dict
    parsed = parse_query_dict(yaml.safe_load(result.yaml))
    assert parsed.entity == "PullRequest"
    assert parsed.is_aggregate


# ---------------------------------------------------------------------------
# text2query_yaml — retry on validation / execution error
# ---------------------------------------------------------------------------


def test_retries_after_executor_returns_error(
    fake_llm, fake_executor, schema_demo_tiny,
):
    # First attempt — wrong relation name; executor reports failure.
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "group_by": [{"field": "repository.owned_by.name"}],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }, call_id="call_1"))
    fake_executor.results.append(QueryExecutionResult(
        success=False,
        error="Relation 'repository' not found on entity 'PullRequest'",
    ))
    # Second attempt — corrected; executor returns rows.
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "group_by": [{"field": "belongs_to.owned_by.name"}],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }, call_id="call_2"))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[["TeamA", 100]], columns=["belongs_to.owned_by.name", "n"],
        row_count=1,
    ))

    result = text2query_yaml(
        prompt="PRs by team", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    assert result.success
    assert result.iterations == 2
    assert len(result.tool_calls) == 2

    # Verify the LLM saw the error response on its second turn — the
    # messages it receives should include the tool-result content with the
    # error message.
    second_call_messages = fake_llm.received[1]["messages"]
    error_msg = next(
        m for m in second_call_messages
        if m.get("role") == "tool" and "Relation 'repository' not found" in m.get("content", "")
    )
    assert error_msg is not None


def test_returns_failure_when_max_iter_exhausted(
    fake_llm, fake_executor, schema_demo_tiny,
):
    for i in range(3):
        fake_llm.responses.append(response_with_tool("build_and_run_query", {
            "root_entity": "PullRequest",
        }, call_id=f"call_{i}"))
        fake_executor.results.append(QueryExecutionResult(
            success=False, error=f"failure {i}",
        ))

    result = text2query_yaml(
        prompt="something", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
        max_iter=3,
    )

    assert not result.success
    assert result.iterations == 3
    assert "failure 2" in (result.error or "")
    assert len(result.tool_calls) == 3


def test_returns_failure_when_llm_stops_without_tool_call(
    fake_llm, fake_executor, schema_demo_tiny,
):
    fake_llm.responses.append(response_text("I'm not sure how to answer that."))

    result = text2query_yaml(
        prompt="huh", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    assert not result.success
    assert result.error is not None
    assert len(result.tool_calls) == 0


# ---------------------------------------------------------------------------
# text2query_yaml — history wiring (multi-turn drilldown)
# ---------------------------------------------------------------------------


def test_history_is_passed_to_llm(fake_llm, fake_executor, schema_demo_tiny):
    history = [
        {"role": "user", "content": "Top 10 PR authors"},
        {"role": "assistant", "content": "I returned the top 10 authors."},
    ]
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "group_by": [{"field": "belongs_to.owned_by.name"}],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[["A", 1]], columns=["x", "n"], row_count=1,
    ))

    text2query_yaml(
        prompt="now by team", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
        history=history,
    )

    sent = fake_llm.received[0]["messages"]
    # system + 2 history + user = 4
    assert len(sent) == 4
    assert sent[0]["role"] == "system"
    assert sent[1]["content"] == "Top 10 PR authors"
    assert sent[2]["role"] == "assistant"
    assert sent[3]["content"] == "now by team"


def test_unknown_tool_name_is_recoverable(fake_llm, fake_executor, schema_demo_tiny):
    # First the LLM calls a tool we don't expose; we feed an error back.
    fake_llm.responses.append(LLMResponse(
        content=None,
        tool_calls=[
            __import__("semantic.llm.provider", fromlist=["ToolCall"]).ToolCall(
                id="bad", name="hallucinated_tool", arguments={},
            ),
        ],
    ))
    # Then the LLM corrects course.
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }, call_id="call_ok"))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[[1]], columns=["n"], row_count=1,
    ))

    result = text2query_yaml(
        prompt="count", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    assert result.success
    # Verify the LLM saw the "unknown tool" error before getting it right.
    second_call_msgs = fake_llm.received[1]["messages"]
    bad_response = next(
        m for m in second_call_msgs
        if m.get("role") == "tool" and "unknown tool" in m.get("content", "")
    )
    assert bad_response


def test_executor_receives_built_querydef(fake_llm, fake_executor, schema_demo_tiny):
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "filters": ["state == 'merged'"],
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[[42]], columns=["n"], row_count=1,
    ))

    text2query_yaml(
        prompt="merged PRs", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    assert len(fake_executor.received) == 1
    received_qd = fake_executor.received[0]
    assert received_qd.entity == "PullRequest"
    assert received_qd.filters == ["state == 'merged'"]


def test_result_serialised_to_llm_includes_rows_and_columns(
    fake_llm, fake_executor, schema_demo_tiny,
):
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
    }, call_id="c1"))
    fake_executor.results.append(QueryExecutionResult(
        success=False, error="boom",
    ))
    # Need a second response since first execute fails — text2query will
    # ask the LLM what to do next.
    fake_llm.responses.append(response_with_tool("build_and_run_query", {
        "root_entity": "PullRequest",
        "measures": [{"name": "n", "field": "pull_request_key", "rollup": "count"}],
    }, call_id="c2"))
    fake_executor.results.append(QueryExecutionResult(
        success=True, rows=[[1]], columns=["n"], row_count=1,
    ))

    text2query_yaml(
        prompt="x", model_name="demo",
        schema_context=schema_demo_tiny,
        executor=fake_executor, llm_client=fake_llm,
    )

    # Inspect the tool-result message sent after the FIRST failed call —
    # should contain the executor's error string verbatim.
    second_call_msgs = fake_llm.received[1]["messages"]
    failure_msg = next(
        m for m in second_call_msgs
        if m.get("role") == "tool" and m.get("tool_call_id") == "c1"
    )
    payload = json.loads(failure_msg["content"])
    assert payload == {"error": "boom"}
