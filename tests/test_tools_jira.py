# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Jira tool with mocked API responses."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tools.jira.tool import JiraTool, _jql_date

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def jira_tool():
    """Create a JiraTool with a mocked Jira client (bypass __init__)."""
    tool = JiraTool.__new__(JiraTool)
    tool.jira = MagicMock()
    return tool


def test_list_commands(jira_tool):
    cmds = jira_tool.list_commands()
    assert "boards" in cmds
    assert "sprints" in cmds
    assert "issues" in cmds
    assert "search" in cmds
    assert "projects" in cmds
    assert "fields" in cmds


def test_boards(jira_tool):
    fixture = _load_fixture("jira_boards.json")
    jira_tool.jira.get_all_agile_boards.return_value = fixture

    boards = list(jira_tool.run("boards"))
    assert len(boards) == 2
    assert boards[0]["id"] == 1
    assert boards[0]["name"] == "Engineering Board"
    assert boards[1]["type"] == "kanban"


def test_boards_pagination(jira_tool):
    page1 = {"values": [{"id": 1}], "isLast": False}
    page2 = {"values": [{"id": 2}], "isLast": True}
    jira_tool.jira.get_all_agile_boards.side_effect = [page1, page2]

    boards = list(jira_tool.run("boards"))
    assert len(boards) == 2
    assert jira_tool.jira.get_all_agile_boards.call_count == 2


def test_sprints(jira_tool):
    fixture = _load_fixture("jira_sprints.json")
    jira_tool.jira.get_all_sprint.return_value = fixture

    sprints = list(jira_tool.run("sprints", {"board_id": 1}))
    assert len(sprints) == 2
    assert sprints[0]["name"] == "Sprint 1"
    assert sprints[1]["state"] == "active"


def test_sprints_requires_board_id(jira_tool):
    with pytest.raises(ValueError, match="board_id"):
        list(jira_tool.run("sprints"))


def test_search(jira_tool):
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    issues = list(jira_tool.run("search", {"jql": "project = ENG", "expand": "changelog"}))
    assert len(issues) == 2
    assert issues[0]["key"] == "ENG-101"
    assert issues[0]["fields"]["summary"] == "Implement login page"

    jira_tool.jira.enhanced_jql.assert_called_once_with(
        "project = ENG", limit=50, expand="changelog", nextPageToken=None
    )


def test_issues(jira_tool):
    """Issues command should fetch all issues for a project."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    issues = list(jira_tool.run("issues", {"project": "ENG", "expand": "changelog"}))
    assert len(issues) == 2
    assert issues[0]["key"] == "ENG-101"
    assert issues[0]["fields"]["story_points"] == 5.0
    assert issues[0]["fields"]["sprint"]["name"] == "Sprint 1"
    assert issues[0]["changelog"]["histories"][0]["items"][0]["field"] == "status"

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert called_jql == "project = ENG ORDER BY updated DESC"


def test_issues_requires_project(jira_tool):
    with pytest.raises(ValueError, match="project"):
        list(jira_tool.run("issues"))


def test_issues_since(jira_tool):
    """Issues with since should inject updated >= into JQL."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    list(jira_tool.run("issues", {"project": "ENG", "since": "2025-01-01"}))

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert 'updated >= "2025-01-01"' in called_jql
    assert called_jql.startswith("project = ENG AND")


def test_resolve_wildcard_project(jira_tool):
    jira_tool.jira.projects.return_value = [
        {"key": "ENG", "name": "Engineering"},
        {"key": "OPS", "name": "Operations"},
    ]
    keys = jira_tool.resolve_wildcard("project", "*")
    assert keys == ["ENG", "OPS"]


def test_resolve_wildcard_board_id(jira_tool):
    fixture = _load_fixture("jira_boards.json")
    jira_tool.jira.get_all_agile_boards.return_value = fixture

    board_ids = jira_tool.resolve_wildcard("board_id", "*")
    assert board_ids == ["1", "2"]


def test_unknown_command(jira_tool):
    with pytest.raises(ValueError, match="Unknown Jira command"):
        list(jira_tool.run("nonexistent"))


# --- since filtering ---

def test_sprints_since_filters_old(jira_tool):
    """Sprints with endDate before since should be filtered out."""
    fixture = _load_fixture("jira_sprints.json")
    jira_tool.jira.get_all_sprint.return_value = fixture

    # Sprint 1 ends 2025-01-20, Sprint 2 ends 2025-02-03
    # since=2025-01-25 should filter out Sprint 1
    sprints = list(jira_tool.run("sprints", {"board_id": 1, "since": "2025-01-25"}))
    assert len(sprints) == 1
    assert sprints[0]["name"] == "Sprint 2"


def test_sprints_since_keeps_all_when_old(jira_tool):
    """A since date before all sprints should keep everything."""
    fixture = _load_fixture("jira_sprints.json")
    jira_tool.jira.get_all_sprint.return_value = fixture

    sprints = list(jira_tool.run("sprints", {"board_id": 1, "since": "2024-01-01"}))
    assert len(sprints) == 2


def test_search_since_appends_jql(jira_tool):
    """Since should be injected into JQL as an updated >= clause."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    list(jira_tool.run("search", {
        "jql": "project = ENG",
        "since": "2025-01-01",
    }))

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert 'updated >= "2025-01-01"' in called_jql
    assert called_jql.startswith("project = ENG AND")


def test_search_since_without_existing_jql(jira_tool):
    """When jql is empty, since should be the only clause."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    list(jira_tool.run("search", {"since": "2025-01-01"}))

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert called_jql == 'updated >= "2025-01-01"'


# --- _jql_date ---

def test_jql_date_plain_date_passthrough():
    assert _jql_date("2025-10-01") == "2025-10-01"


def test_jql_date_iso8601_with_tz():
    assert _jql_date("2026-02-18T15:30:00.123456+00:00") == "2026-02-18 15:30"


def test_jql_date_iso8601_no_microseconds():
    assert _jql_date("2026-02-18T15:30:00+00:00") == "2026-02-18 15:30"


def test_jql_date_iso8601_naive():
    assert _jql_date("2026-02-18T15:30:00.123456") == "2026-02-18 15:30"


def test_issues_since_iso8601_is_converted(jira_tool):
    """ISO 8601 since from DB should be converted to JQL-safe format."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    list(jira_tool.run("issues", {"project": "ENG", "since": "2026-02-18T15:30:00.123456+00:00"}))

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert 'updated >= "2026-02-18 15:30"' in called_jql


def test_search_since_iso8601_is_converted(jira_tool):
    """ISO 8601 since from DB should be converted to JQL-safe format in search."""
    fixture = _load_fixture("jira_search.json")
    jira_tool.jira.enhanced_jql.return_value = fixture

    list(jira_tool.run("search", {"jql": "project = ENG", "since": "2026-02-18T15:30:00.123456+00:00"}))

    called_jql = jira_tool.jira.enhanced_jql.call_args[0][0]
    assert 'updated >= "2026-02-18 15:30"' in called_jql
