# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the GitHub tool with mocked requests.Session calls."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def _mock_resp(data, status_code=200, link=""):
    """Build a mock requests.Response for use in tests."""
    resp = MagicMock()
    resp.status_code = status_code
    if isinstance(data, str):
        data = json.loads(data) if data.strip() else []
    resp.json.return_value = data
    resp.text = ""
    _headers = {"Link": link, "X-RateLimit-Remaining": "60", "X-RateLimit-Reset": "0"}
    resp.headers = MagicMock()
    resp.headers.get = lambda k, d="": _headers.get(k, d)
    return resp


@pytest.fixture
def github_tool():
    """Create a GitHubTool with a mocked session, bypassing __init__."""
    from tools.git.tool import GitHubTool
    tool = GitHubTool.__new__(GitHubTool)
    tool.default_org = "myorg"
    tool.host = "github.com"
    tool._rest_base = "https://api.github.com"
    tool._graphql_url = "https://api.github.com/graphql"
    tool._session = MagicMock()
    yield tool


def test_list_commands(github_tool):
    cmds = github_tool.list_commands()
    assert "repos" in cmds
    assert "teams" in cmds
    assert "team_members" in cmds
    assert "orgs" in cmds
    assert "commits" in cmds
    assert "pull_requests" in cmds
    assert "releases" in cmds
    assert "workflow_runs" in cmds
    assert "tags" in cmds
    assert "pr_timeline" in cmds


def test_repos(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_repos.json"))
    )
    repos = list(github_tool.run("repos", {"org": "myorg"}))
    assert len(repos) == 2
    assert repos[0]["name"] == "api-gateway"
    assert repos[1]["language"] == "TypeScript"

    call_args = github_tool._session.request.call_args
    method, url = call_args[0]
    assert method == "GET"
    assert "/orgs/myorg/repos" in url


def test_teams(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_teams.json"))
    )
    teams = list(github_tool.run("teams", {"org": "myorg"}))
    assert len(teams) == 2
    assert teams[0]["slug"] == "backend-engineers"
    assert teams[1]["members_count"] == 4


def test_team_members_single_team_via_rest(github_tool):
    """team_slug param: falls through to REST, returns members with team fields injected."""
    members_data = [
        {"login": "alice", "id": 1},
        {"login": "bob", "id": 2},
    ]
    github_tool._session.request.return_value = _mock_resp(members_data)

    members = list(github_tool.run("team_members", {
        "org": "myorg",
        "team_slug": "backend-engineers",
    }))
    assert len(members) == 2
    assert members[0]["login"] == "alice"
    assert members[0]["_team_slug"] == "backend-engineers"


def _graphql_teams_response(teams):
    """Build a minimal GraphQL response for the teams query."""
    return {
        "data": {
            "organization": {
                "teams": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": teams,
                }
            }
        }
    }


def test_team_members_all_teams_graphql(github_tool):
    """No team_slug: uses GraphQL, yields one record per (team, member) pair."""
    github_tool._session.post.return_value = _mock_resp(_graphql_teams_response([
        {
            "databaseId": 10,
            "name": "Backend",
            "slug": "backend",
            "members": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {"databaseId": 1, "login": "alice", "name": "Alice"},
                    {"databaseId": 2, "login": "bob", "name": "Bob"},
                ],
            },
        },
        {
            "databaseId": 20,
            "name": "Frontend",
            "slug": "frontend",
            "members": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {"databaseId": 3, "login": "carol", "name": "Carol"},
                ],
            },
        },
    ]))

    records = list(github_tool.run("team_members", {"org": "myorg"}))

    assert len(records) == 3
    backend = [r for r in records if r["_team_slug"] == "backend"]
    assert len(backend) == 2
    assert {r["login"] for r in backend} == {"alice", "bob"}
    frontend = [r for r in records if r["_team_slug"] == "frontend"]
    assert frontend[0]["login"] == "carol"
    assert frontend[0]["_team_id"] == 20
    assert frontend[0]["_team_name"] == "Frontend"


def test_team_members_graphql_member_pagination(github_tool):
    """Teams with >100 members trigger a follow-up per-team member query."""
    # First call: teams page with one team whose members page has hasNextPage=True
    teams_resp = _graphql_teams_response([{
        "databaseId": 10,
        "name": "Big Team",
        "slug": "big-team",
        "members": {
            "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
            "nodes": [{"databaseId": 1, "login": "user1", "name": None}],
        },
    }])
    # Second call: follow-up member pagination for "big-team"
    members_resp = {
        "data": {
            "organization": {
                "team": {
                    "members": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [{"databaseId": 2, "login": "user2", "name": None}],
                    }
                }
            }
        }
    }
    github_tool._session.post.side_effect = [
        _mock_resp(teams_resp),
        _mock_resp(members_resp),
    ]

    records = list(github_tool.run("team_members", {"org": "myorg"}))

    assert len(records) == 2
    assert {r["login"] for r in records} == {"user1", "user2"}
    assert all(r["_team_slug"] == "big-team" for r in records)


def test_team_members_all_teams_rest_fallback(github_tool):
    """Falls back to REST when GraphQL returns None."""
    teams_data = [
        {"id": 10, "name": "Backend", "slug": "backend"},
        {"id": 20, "name": "Frontend", "slug": "frontend"},
    ]
    alice = {"id": 1, "login": "alice", "name": "Alice"}
    carol = {"id": 3, "login": "carol", "name": "Carol"}

    # GraphQL POST returns failure, REST GET returns teams then members
    github_tool._session.post.return_value = _mock_resp({}, status_code=500)
    github_tool._session.request.side_effect = [
        _mock_resp(teams_data),          # list teams
        _mock_resp([alice]),             # backend members
        _mock_resp([carol]),             # frontend members
    ]

    records = list(github_tool.run("team_members", {"org": "myorg"}))

    assert len(records) == 2
    assert records[0]["_team_slug"] == "backend"
    assert records[0]["login"] == "alice"
    assert records[1]["_team_slug"] == "frontend"
    assert records[1]["login"] == "carol"


def test_orgs(github_tool):
    orgs_data = [
        {"login": "myorg", "id": 500},
        {"login": "otherorg", "id": 501},
    ]
    github_tool._session.request.return_value = _mock_resp(orgs_data)

    orgs = list(github_tool.run("orgs"))
    assert len(orgs) == 2
    assert orgs[0]["login"] == "myorg"


def test_repos_uses_default_org(github_tool):
    github_tool._session.request.return_value = _mock_resp([])
    list(github_tool.run("repos"))  # No org param — should use default

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/orgs/myorg/" in url


def test_repos_requires_org():
    """Without default_org and no param, should raise."""
    from tools.git.tool import GitHubTool
    tool = GitHubTool.__new__(GitHubTool)
    tool.default_org = None
    tool.host = None
    tool._rest_base = "https://api.github.com"
    tool._graphql_url = "https://api.github.com/graphql"
    tool._session = MagicMock()
    with pytest.raises(ValueError, match="org"):
        list(tool.run("repos"))


def test_unknown_command(github_tool):
    with pytest.raises(ValueError, match="Unknown GitHub command"):
        list(github_tool.run("nonexistent"))


def test_resolve_wildcard_org(github_tool):
    orgs_data = [
        {"login": "org1", "id": 1},
        {"login": "org2", "id": 2},
    ]
    github_tool._session.request.return_value = _mock_resp(orgs_data)

    orgs = github_tool.resolve_wildcard("org", "*")
    assert orgs == ["org1", "org2"]


# --- since filtering ---

def test_repos_since_adds_query_param(github_tool):
    github_tool._session.request.return_value = _mock_resp([])
    list(github_tool.run("repos", {"org": "myorg", "since": "2025-01-01"}))

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "since=2025-01-01" in url


def test_repos_without_since_no_param(github_tool):
    github_tool._session.request.return_value = _mock_resp([])
    list(github_tool.run("repos", {"org": "myorg"}))

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "since=" not in url


# --- New commands: commits ---

def test_commits(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_commits.json"))
    )
    commits = list(github_tool.run("commits", {"repo": "api-gateway"}))
    assert len(commits) == 2
    assert commits[0]["sha"] == "abc123def456"
    assert commits[1]["author"]["login"] == "bob"

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/repos/myorg/api-gateway/commits" in url


def test_commits_since(github_tool):
    github_tool._session.request.return_value = _mock_resp([])
    list(github_tool.run("commits", {"repo": "api-gateway", "since": "2025-01-01T00:00:00Z"}))

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "since=2025-01-01T00:00:00Z" in url


def test_commits_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("commits"))


# --- New commands: pull_requests ---

def test_pull_requests(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_pull_requests.json"))
    )
    prs = list(github_tool.run("pull_requests", {"repo": "api-gateway"}))
    assert len(prs) == 2
    assert prs[0]["number"] == 42
    assert prs[0]["user"]["login"] == "alice"
    assert prs[1]["draft"] is True

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/repos/myorg/api-gateway/pulls" in url
    assert "state=all" in url


def test_pull_requests_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("pull_requests"))


# --- New commands: releases ---

def test_releases(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_releases.json"))
    )
    releases = list(github_tool.run("releases", {"repo": "api-gateway"}))
    assert len(releases) == 2
    assert releases[0]["tag_name"] == "v1.2.0"
    assert releases[1]["prerelease"] is True

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/repos/myorg/api-gateway/releases" in url


def test_releases_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("releases"))


# --- New commands: workflow_runs ---

def test_workflow_runs(github_tool):
    fixture = json.loads(_load_fixture("github_workflow_runs.json"))
    # Returns full response dict; result_key="workflow_runs" unwraps it
    github_tool._session.request.return_value = _mock_resp(fixture)

    runs = list(github_tool.run("workflow_runs", {"repo": "api-gateway"}))
    assert len(runs) == 2
    assert runs[0]["id"] == 4001
    assert runs[0]["conclusion"] == "success"
    assert runs[1]["triggering_actor"]["login"] == "bob"

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/repos/myorg/api-gateway/actions/runs" in url


def test_workflow_runs_since(github_tool):
    github_tool._session.request.return_value = _mock_resp({"workflow_runs": []})
    list(github_tool.run("workflow_runs", {"repo": "api-gateway", "since": "2025-01-01"}))

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "created=>=" in url


def test_workflow_runs_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("workflow_runs"))


# --- New commands: tags ---

def test_tags(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_tags.json"))
    )
    tags = list(github_tool.run("tags", {"repo": "api-gateway"}))
    assert len(tags) == 2
    assert tags[0]["name"] == "v1.2.0"
    assert tags[1]["commit"]["sha"] == "old999sha888"

    call_args = github_tool._session.request.call_args
    _, url = call_args[0]
    assert "/repos/myorg/api-gateway/tags" in url


def test_tags_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("tags"))


# --- Repo wildcard resolution ---

def test_resolve_wildcard_repo(github_tool):
    github_tool._session.request.return_value = _mock_resp(
        json.loads(_load_fixture("github_repos.json"))
    )
    repos = github_tool.resolve_wildcard("repo", "*")
    assert repos == ["api-gateway", "frontend-app"]


def test_resolve_wildcard_repo_excludes_archived(github_tool):
    repos_data = [
        {"name": "active-repo", "archived": False},
        {"name": "old-repo", "archived": True},
        {"name": "another-active", "archived": False},
    ]
    github_tool._session.request.return_value = _mock_resp(repos_data)
    repos = github_tool.resolve_wildcard("repo", "*")
    assert repos == ["active-repo", "another-active"]


def test_resolve_wildcard_unsupported(github_tool):
    with pytest.raises(NotImplementedError):
        github_tool.resolve_wildcard("team", "*")


# --- pr_timeline ---

def test_pr_timeline(github_tool):
    """pr_timeline fetches PRs then their timeline events, injecting _pr_number."""
    prs_data = [
        {"number": 10, "title": "PR ten", "updated_at": "2025-02-01T10:00:00Z"},
        {"number": 20, "title": "PR twenty", "updated_at": "2025-01-15T08:00:00Z"},
    ]
    timeline_10 = [
        {"event": "closed", "actor": {"login": "alice"}, "created_at": "2025-02-01T10:00:00Z"},
    ]
    timeline_20 = [
        {"event": "merged", "actor": {"login": "bob"}, "created_at": "2025-02-02T12:00:00Z"},
        {"event": "closed", "actor": {"login": "bob"}, "created_at": "2025-02-02T12:00:00Z"},
    ]

    def side_effect(method, url, **kwargs):
        if "/pulls?" in url:
            return _mock_resp(prs_data)
        if "/issues/10/timeline" in url:
            return _mock_resp(timeline_10)
        if "/issues/20/timeline" in url:
            return _mock_resp(timeline_20)
        return _mock_resp([])

    github_tool._session.request.side_effect = side_effect

    events = list(github_tool.run("pr_timeline", {"repo": "api-gateway"}))
    assert len(events) == 3

    assert events[0]["_pr_number"] == 10
    assert events[0]["event"] == "closed"

    assert events[1]["_pr_number"] == 20
    assert events[1]["event"] == "merged"
    assert events[2]["_pr_number"] == 20
    assert events[2]["event"] == "closed"


def test_pr_timeline_empty_prs(github_tool):
    """pr_timeline with no PRs yields no events."""
    github_tool._session.request.return_value = _mock_resp([])
    events = list(github_tool.run("pr_timeline", {"repo": "api-gateway"}))
    assert events == []


def test_pr_timeline_since_filters_old_prs(github_tool):
    """When since is set, PRs older than the cutoff are skipped (early stop)."""
    prs_data = [
        {"number": 10, "updated_at": "2025-02-10T12:00:00Z"},
        {"number": 20, "updated_at": "2025-02-05T08:00:00Z"},
        {"number": 30, "updated_at": "2025-01-01T00:00:00Z"},  # older than since
    ]
    timeline_10 = [
        {"event": "merged", "actor": {"login": "alice"}, "created_at": "2025-02-10T12:00:00Z"},
    ]
    timeline_20 = [
        {"event": "closed", "actor": {"login": "bob"}, "created_at": "2025-02-05T08:00:00Z"},
    ]

    def side_effect(method, url, **kwargs):
        if "/pulls?" in url:
            return _mock_resp(prs_data)
        if "/issues/10/timeline" in url:
            return _mock_resp(timeline_10)
        if "/issues/20/timeline" in url:
            return _mock_resp(timeline_20)
        if "/issues/30/timeline" in url:
            raise AssertionError("Should not fetch timeline for PR #30")
        return _mock_resp([])

    github_tool._session.request.side_effect = side_effect

    events = list(github_tool.run("pr_timeline", {
        "repo": "api-gateway",
        "since": "2025-02-01T00:00:00Z",
    }))
    assert len(events) == 2
    assert events[0]["_pr_number"] == 10
    assert events[1]["_pr_number"] == 20


def test_pr_timeline_empty_timeline(github_tool):
    """pr_timeline with PRs but no timeline events yields nothing."""
    prs_data = [{"number": 5, "title": "A PR", "updated_at": "2025-02-01T00:00:00Z"}]

    def side_effect(method, url, **kwargs):
        if "/pulls?" in url:
            return _mock_resp(prs_data)
        return _mock_resp([])

    github_tool._session.request.side_effect = side_effect

    events = list(github_tool.run("pr_timeline", {"repo": "api-gateway"}))
    assert events == []


def test_pr_timeline_requires_repo(github_tool):
    with pytest.raises(ValueError, match="repo"):
        list(github_tool.run("pr_timeline"))


# --- _parse_next_link ---

def test_parse_next_link_present():
    from tools.git.tool import _parse_next_link
    link = (
        '<https://api.github.com/orgs/foo/repos?page=2>; rel="next", '
        '<https://api.github.com/orgs/foo/repos?page=5>; rel="last"'
    )
    assert _parse_next_link(link) == "https://api.github.com/orgs/foo/repos?page=2"


def test_parse_next_link_absent():
    from tools.git.tool import _parse_next_link
    link = '<https://api.github.com/orgs/foo/repos?page=1>; rel="first"'
    assert _parse_next_link(link) is None


def test_parse_next_link_empty():
    from tools.git.tool import _parse_next_link
    assert _parse_next_link("") is None


# --- pagination ---

def test_repos_pagination_follows_link(github_tool):
    """_rest follows Link: next until no more pages."""
    page1 = [{"name": "repo-a"}]
    page2 = [{"name": "repo-b"}]

    next_url = "https://api.github.com/orgs/myorg/repos?page=2&per_page=100"

    call_count = [0]

    def side_effect(method, url, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return _mock_resp(page1, link=f'<{next_url}>; rel="next"')
        return _mock_resp(page2, link="")

    github_tool._session.request.side_effect = side_effect

    repos = list(github_tool.run("repos", {"org": "myorg"}))
    assert len(repos) == 2
    assert repos[0]["name"] == "repo-a"
    assert repos[1]["name"] == "repo-b"
    assert call_count[0] == 2
