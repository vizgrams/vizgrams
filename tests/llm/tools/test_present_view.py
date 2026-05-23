# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the present_view Tool handler (Epic 20 VG-231)."""

from __future__ import annotations

from semantic.llm.tools.present_view import PRESENT_VIEW
from semantic.llm.tools.registry import ToolContext


def test_handler_returns_terminal_success_with_chart_and_caption():
    args = {
        "chart_type": "bar",
        "x_field": "author",
        "y_field": "pr_count",
        "caption": "dependabot leads with 7,444 PRs",
    }
    result = PRESENT_VIEW.handler(args, ToolContext())

    assert result.success
    assert result.terminate is True
    assert result.payload["chart_type"] == "bar"
    assert result.payload["x_field"] == "author"
    assert result.payload["y_field"] == "pr_count"
    assert result.payload["caption"] == "dependabot leads with 7,444 PRs"


def test_handler_omits_unset_optional_axes():
    args = {"chart_type": "kpi", "y_field": "n", "caption": "42 widgets"}
    result = PRESENT_VIEW.handler(args, ToolContext())
    assert result.success
    assert result.payload["x_field"] is None
    assert result.payload["color_field"] is None


def test_handler_fails_when_chart_type_missing():
    result = PRESENT_VIEW.handler({"caption": "x"}, ToolContext())
    assert not result.success
    assert result.terminate is False


def test_handler_fails_when_caption_missing():
    result = PRESENT_VIEW.handler({"chart_type": "bar"}, ToolContext())
    assert not result.success


def test_tool_definition_marked_terminal():
    assert PRESENT_VIEW.name == "present_view"
    assert PRESENT_VIEW.terminal is True
    assert "view_selection" in PRESENT_VIEW.tags
    assert "chart_type" in PRESENT_VIEW.parameters_schema["required"]
    assert "caption" in PRESENT_VIEW.parameters_schema["required"]
