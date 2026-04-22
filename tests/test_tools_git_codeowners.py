# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the GitHub CODEOWNERS tool with mocked requests.Session calls."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def _mock_resp(data, status_code=200):
    """Build a mock requests.Response for use in tests."""
    resp = MagicMock()
    resp.status_code = status_code
    if isinstance(data, str):
        data = json.loads(data) if data.strip() else []
    resp.json.return_value = data
    resp.text = ""
    _headers = {"Link": "", "X-RateLimit-Remaining": "60", "X-RateLimit-Reset": "0"}
    resp.headers = MagicMock()
    resp.headers.get = lambda k, d="": _headers.get(k, d)
    return resp


@pytest.fixture
def codeowners_tool():
    """Create a CodeownersTool with a mocked session, bypassing __init__."""
    from tools.git_codeowners.tool import CodeownersTool
    tool = CodeownersTool.__new__(CodeownersTool)
    tool.default_org = "myorg"
    tool.host = "github.com"
    tool._rest_base = "https://api.github.com"
    tool._session = MagicMock()
    yield tool


def test_list_commands(codeowners_tool):
    cmds = codeowners_tool.list_commands()
    assert cmds == ["codeowners"]


def test_unknown_command(codeowners_tool):
    with pytest.raises(ValueError, match="Unknown codeowners command"):
        list(codeowners_tool.run("nonexistent"))


def test_parse_codeowners_skips_comments_and_blanks():
    from tools.git_codeowners.tool import _parse_codeowners

    content = """
# This is a comment
* @org/platform-team

# Another comment

*.js @org/frontend-team
"""
    pairs = _parse_codeowners(content)
    assert len(pairs) == 2
    assert ("*", "@org/platform-team") in pairs
    assert ("*.js", "@org/frontend-team") in pairs


def test_parse_codeowners_multi_owner_expansion():
    from tools.git_codeowners.tool import _parse_codeowners

    content = "*.js @org/frontend-team @alice @bob"
    pairs = _parse_codeowners(content)
    assert len(pairs) == 3
    assert pairs[0] == ("*.js", "@org/frontend-team")
    assert pairs[1] == ("*.js", "@alice")
    assert pairs[2] == ("*.js", "@bob")


def test_codeowners_fetches_from_dotgithub_first(codeowners_tool):
    """CODEOWNERS found at .github/CODEOWNERS — no fallback needed."""
    repos_data = [{"name": "my-repo"}]
    codeowners_data = json.loads(_load_fixture("github_codeowners_api.json"))

    def side_effect(method, url, **kwargs):
        if "/repos?per_page=100" in url:
            return _mock_resp(repos_data)
        if ".github/CODEOWNERS" in url:
            return _mock_resp(codeowners_data)
        return _mock_resp([])

    codeowners_tool._session.request.side_effect = side_effect

    records = list(codeowners_tool.run("codeowners", {"org": "myorg"}))

    # The fixture has 5 owner entries:
    # * @org/platform-team
    # *.js @org/frontend-team, *.js @alice
    # *.css @org/frontend-team
    # /src/api/ @org/backend-team
    assert len(records) == 5
    assert records[0] == {"repo": "my-repo", "pattern": "*", "owner": "@org/platform-team"}
    assert records[1] == {"repo": "my-repo", "pattern": "*.js", "owner": "@org/frontend-team"}
    assert records[2] == {"repo": "my-repo", "pattern": "*.js", "owner": "@alice"}


def test_codeowners_falls_back_to_root(codeowners_tool):
    """.github/CODEOWNERS returns 404, falls back to root CODEOWNERS."""
    repos_data = [{"name": "my-repo"}]
    codeowners_data = json.loads(_load_fixture("github_codeowners_api.json"))

    def side_effect(method, url, **kwargs):
        if "/repos?per_page=100" in url:
            return _mock_resp(repos_data)
        if ".github/CODEOWNERS" in url:
            return _mock_resp([], status_code=404)
        if "contents/CODEOWNERS" in url:
            return _mock_resp(codeowners_data)
        return _mock_resp([])

    codeowners_tool._session.request.side_effect = side_effect

    records = list(codeowners_tool.run("codeowners", {"org": "myorg"}))
    assert len(records) == 5
    assert records[0]["repo"] == "my-repo"


def test_codeowners_404_skips_repo(codeowners_tool):
    """Repos with no CODEOWNERS at either path are silently skipped."""
    repos_data = [{"name": "no-owners-repo"}]

    def side_effect(method, url, **kwargs):
        if "/repos?per_page=100" in url:
            return _mock_resp(repos_data)
        if "CODEOWNERS" in url:
            return _mock_resp([], status_code=404)
        return _mock_resp([])

    codeowners_tool._session.request.side_effect = side_effect

    records = list(codeowners_tool.run("codeowners", {"org": "myorg"}))
    assert records == []


def test_resolve_wildcard_org(codeowners_tool):
    """org='*' resolves via /user/orgs."""
    orgs_data = [
        {"login": "org1", "id": 1},
        {"login": "org2", "id": 2},
    ]
    codeowners_tool._session.request.return_value = _mock_resp(orgs_data)

    orgs = codeowners_tool.resolve_wildcard("org", "*")
    assert orgs == ["org1", "org2"]

    call_args = codeowners_tool._session.request.call_args
    _, url = call_args[0]
    assert "/user/orgs" in url
