# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic/llm/text2view."""

from __future__ import annotations

import yaml

from semantic.llm.provider import LLMResponse
from semantic.llm.text2view import text2view_yaml, view_yaml
from tests.llm.conftest import response_text, response_with_tool


def test_returns_chart_spec_from_present_view_call(fake_llm, default_registry):
    fake_llm.responses.append(response_with_tool("present_view", {
        "title": "PR count by author",
        "chart_type": "bar",
        "x_field": "author",
        "y_field": "pr_count",
        "caption": "dependabot leads with 7,444 PRs",
    }))

    result = text2view_yaml(
        columns=["author", "pr_count"],
        rows=[["dependabot", 7444], ["mdwiag", 829]],
        user_intent="top PR authors",
        llm_client=fake_llm, registry=default_registry,
    )

    assert result.success
    assert result.chart_type == "bar"
    assert result.x_field == "author"
    assert result.y_field == "pr_count"
    assert result.caption == "dependabot leads with 7,444 PRs"
    assert result.yaml is not None


def test_kpi_chart_emits_type_metric(fake_llm, default_registry):
    fake_llm.responses.append(response_with_tool("present_view", {
        "title": "Total PRs",
        "chart_type": "kpi",
        "y_field": "n",
        "caption": "19,999 PRs total",
    }))

    result = text2view_yaml(
        columns=["n"], rows=[[19999]],
        llm_client=fake_llm, registry=default_registry,
    )

    assert result.success
    body = yaml.safe_load(result.yaml)
    # kpi maps onto the schema's `type: metric` shape; measure carries the column.
    assert body["type"] == "metric"
    assert body["measure"] == "n"
    assert body["visualization"] == {}


def test_user_intent_and_rows_reach_the_llm(fake_llm, default_registry):
    fake_llm.responses.append(response_with_tool("present_view", {
        "title": "Monthly throughput",
        "chart_type": "line",
        "x_field": "month",
        "y_field": "n",
        "caption": "peaked in October",
    }))

    text2view_yaml(
        columns=["month", "n"],
        rows=[["2025-01", 100], ["2025-02", 200]],
        user_intent="monthly throughput",
        llm_client=fake_llm, registry=default_registry,
    )

    user_payload = fake_llm.received[0]["messages"][-1]["content"]
    assert "monthly throughput" in user_payload
    assert "2025-01" in user_payload
    assert "month" in user_payload


def test_truncates_rows_sent_to_llm(fake_llm, default_registry):
    fake_llm.responses.append(response_with_tool("present_view", {
        "title": "Rows",
        "chart_type": "table",
        "caption": "...",
    }))

    rows = [[i, f"row_{i}"] for i in range(50)]
    text2view_yaml(
        columns=["i", "label"], rows=rows,
        llm_client=fake_llm, registry=default_registry, rows_to_llm=10,
    )

    payload = fake_llm.received[0]["messages"][-1]["content"]
    assert "row_9" in payload
    assert "row_10" not in payload
    assert "row_count_total" in payload


def test_returns_failure_when_llm_does_not_call_tool(fake_llm, default_registry):
    fake_llm.responses.append(response_text("I cannot generate a chart for this"))

    result = text2view_yaml(
        columns=["n"], rows=[[1]],
        llm_client=fake_llm, registry=default_registry,
    )

    assert not result.success
    assert result.error is not None
    assert "present_view" in result.error


def test_returns_failure_when_chart_type_missing(fake_llm, default_registry):
    fake_llm.responses.append(response_with_tool("present_view", {
        "title": "x",
        "caption": "no chart type provided",
    }))

    result = text2view_yaml(
        columns=["n"], rows=[[1]],
        llm_client=fake_llm, registry=default_registry,
    )

    assert not result.success
    # Handler now reports any of the required-field misses together;
    # chart_type is the one missing in this test.
    assert "chart_type" in (result.error or "")


def test_returns_failure_when_wrong_tool_called(fake_llm, default_registry):
    fake_llm.responses.append(LLMResponse(
        content=None,
        tool_calls=[
            __import__("semantic.llm.provider", fromlist=["ToolCall"]).ToolCall(
                id="x", name="not_present_view", arguments={"chart_type": "bar", "caption": "x"},
            ),
        ],
    ))

    result = text2view_yaml(
        columns=["n"], rows=[[1]],
        llm_client=fake_llm, registry=default_registry,
    )

    assert not result.success
    assert "not_present_view" in (result.error or "")


# ---------------------------------------------------------------------------
# view_yaml — pure serialisation
# ---------------------------------------------------------------------------


def test_view_yaml_chart_emits_type_chart_and_axes():
    out = view_yaml(
        name="my_view", query_name="my_query",
        chart_type="bar", x_field="a", y_field="b", color_field="c",
        caption="x",
    )
    body = yaml.safe_load(out)
    assert body["name"] == "my_view"
    assert body["type"] == "chart"
    assert body["query"] == "my_query"
    assert body["visualization"]["chart_type"] == "bar"
    assert body["visualization"]["x"] == "a"
    assert body["visualization"]["y"] == ["b"]
    assert body["visualization"]["color"] == "c"


def test_view_yaml_kpi_emits_type_metric():
    out = view_yaml(
        name="v", query_name="q",
        chart_type="kpi", x_field=None, y_field="value", color_field=None,
        caption="single",
    )
    body = yaml.safe_load(out)
    assert body["type"] == "metric"
    assert body["measure"] == "value"


def test_view_yaml_table_emits_columns_list():
    out = view_yaml(
        name="v", query_name="q",
        chart_type="table", x_field=None, y_field=None, color_field=None,
        caption="x",
        columns=["a", "b", "c"],
    )
    body = yaml.safe_load(out)
    assert body["type"] == "table"
    assert body["visualization"]["columns"] == ["a", "b", "c"]
