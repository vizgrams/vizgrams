# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""GitHub tool: connects to GitHub via REST and GraphQL APIs using requests."""

import logging
import re
import time
from collections.abc import Iterator

import requests

from tools.base import BaseTool

logger = logging.getLogger(__name__)

COMMANDS = [
    "repos", "teams", "team_members", "orgs",
    "commits", "pull_requests", "releases", "workflow_runs", "tags",
    "pr_timeline", "pr_commits", "pr_reviews", "pr_review_comments",
    "issue_comments", "requested_reviewers",
    "deployments", "users",
]


def _parse_next_link(link_header: str) -> str | None:
    """Extract the 'next' URL from a Link header, or None."""
    if not link_header:
        return None
    for part in link_header.split(","):
        m = re.match(r'\s*<([^>]+)>;\s*rel="next"', part.strip())
        if m:
            return m.group(1)
    return None


class GitHubTool(BaseTool):
    """GitHub data source using the REST and GraphQL APIs."""

    def __init__(self, config: dict, **_kwargs):
        """config: resolved tool config dict from models/<model>/config.yaml."""
        from core.model_config import resolve_credential

        self.default_org: str | None = config.get("org")
        self.host: str | None = config.get("host")

        token_raw = config.get("token", "")
        token = resolve_credential(token_raw) if token_raw else None

        effective_host = self.host or "github.com"
        if effective_host != "github.com":
            self._rest_base = f"https://{effective_host}/api/v3"
            self._graphql_url = f"https://{effective_host}/api/graphql"
        else:
            self._rest_base = "https://api.github.com"
            self._graphql_url = "https://api.github.com/graphql"

        self._session = requests.Session()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.headers["Accept"] = "application/vnd.github+json"
        self._session.headers["X-GitHub-Api-Version"] = "2022-11-28"

    # ------------------------------------------------------------------
    # HTTP primitives
    # ------------------------------------------------------------------

    def _is_rate_limited(self, resp: requests.Response) -> bool:
        if resp.status_code == 429:
            return True
        if resp.status_code == 403:
            try:
                return int(resp.headers.get("X-RateLimit-Remaining", "1")) == 0
            except ValueError:
                return False
        return False

    def _rate_limit_wait_from_resp(self, resp: requests.Response) -> int:
        try:
            reset_epoch = int(resp.headers.get("X-RateLimit-Reset", "0"))
            if reset_epoch:
                return max(reset_epoch - int(time.time()), 0) + 5
        except ValueError:
            pass
        return 60

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response | None:
        """Send an HTTP request with rate-limit handling and retries.

        Returns None if the endpoint is not found (404), access is forbidden
        (non-rate-limited 403), or actions are disabled.  Raises on
        unrecoverable errors after retries.
        """
        max_retries = 3
        for attempt in range(max_retries + 1):
            resp = self._session.request(method, url, timeout=30, **kwargs)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                logger.info("%s %s: 404, treating as empty", method, url)
                return None
            if self._is_rate_limited(resp):
                wait = self._rate_limit_wait_from_resp(resp)
                logger.warning("REST rate limited, waiting %ds", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                logger.warning("%s %s: 403 forbidden — check token permissions", method, url)
                return None
            if "actions is not enabled" in resp.text.lower():
                logger.info("%s %s: actions not enabled, treating as empty", method, url)
                return None
            if attempt < max_retries:
                delay = 2 ** (attempt + 1)
                logger.warning(
                    "%s %s: %d (attempt %d/%d), retrying in %ds",
                    method, url, resp.status_code, attempt + 1, max_retries, delay,
                )
                time.sleep(delay)
                continue
            resp.raise_for_status()
        return None

    def _rest(
        self,
        endpoint: str,
        paginate: bool = False,
        result_key: str | None = None,
    ) -> list[dict]:
        """Call a GitHub REST endpoint and return a flat list of records.

        endpoint: path (e.g. /orgs/foo/repos?per_page=100) or full URL.
        paginate: if True, follow Link: next headers until exhausted.
        result_key: unwrap the response dict by this key (e.g. "workflow_runs").
        """
        url = endpoint if endpoint.startswith("http") else self._rest_base + endpoint
        all_items: list[dict] = []
        while url:
            resp = self._request_with_retry("GET", url)
            if resp is None:
                break
            data = resp.json()
            if result_key and isinstance(data, dict):
                items = data.get(result_key, [])
            elif isinstance(data, list):
                items = data
            else:
                items = [data]
            all_items.extend(items)
            if not paginate:
                break
            url = _parse_next_link(resp.headers.get("Link", ""))
        return all_items

    def _graphql(self, query: str, variables: dict) -> dict | None:
        """POST a GraphQL query and return the parsed response, or None on failure.

        Handles RATE_LIMITED errors in the response body with automatic retry.
        Returns None if GraphQL is unavailable, which triggers REST fallback.
        """
        payload = {"query": query, "variables": variables}
        max_retries = 3
        for _attempt in range(max_retries + 1):
            try:
                resp = self._session.post(self._graphql_url, json=payload, timeout=60)
            except requests.RequestException as exc:
                logger.debug("GraphQL request error: %s", exc)
                return None
            if resp.status_code not in (200, 400):
                if self._is_rate_limited(resp):
                    wait = self._rate_limit_wait_from_resp(resp)
                    logger.warning("GraphQL rate limited (HTTP %d), waiting %ds", resp.status_code, wait)
                    time.sleep(wait)
                    continue
                logger.debug("GraphQL request failed: HTTP %d", resp.status_code)
                return None
            data = resp.json()
            errors = data.get("errors", [])
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                wait = self._rate_limit_wait_from_resp(resp)
                logger.warning("GraphQL rate limited, waiting %ds", wait)
                time.sleep(wait)
                continue
            return data
        return None

    # ------------------------------------------------------------------
    # BaseTool interface
    # ------------------------------------------------------------------

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        params = params or {}
        handler = {
            "repos": self._repos,
            "teams": self._teams,
            "team_members": self._team_members,
            "orgs": self._orgs,
            "commits": self._commits,
            "pull_requests": self._pull_requests,
            "releases": self._releases,
            "workflow_runs": self._workflow_runs,
            "tags": self._tags,
            "pr_timeline": self._pr_timeline,
            "pr_commits": self._pr_commits,
            "pr_reviews": self._pr_reviews,
            "pr_review_comments": self._pr_review_comments,
            "issue_comments": self._issue_comments,
            "requested_reviewers": self._requested_reviewers,
            "deployments": self._deployments,
            "users": self._users,
        }.get(command)
        if handler is None:
            raise ValueError(f"Unknown GitHub command: {command!r}")
        yield from handler(params)

    def resolve_wildcard(self, param_name: str, param_value: str) -> list:
        if param_name == "org" and param_value == "*":
            return [o.get("login") for o in self._orgs({}) if o.get("login")]
        if param_name == "repo" and param_value == "*":
            return [r["name"] for r in self._repos({}) if not r.get("archived")]
        raise NotImplementedError(f"Wildcard not supported for {param_name!r}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_org(self, params: dict) -> str:
        org = params.get("org") or self.default_org
        if not org:
            raise ValueError(
                "GitHub org not set. Either pass 'org' in extractor YAML params "
                "or set 'org' in the git tool config in models/<model>/config.yaml"
            )
        return org

    def _get_repo(self, params: dict) -> str:
        repo = params.get("repo")
        if not repo:
            raise ValueError(
                "repo param is required. Pass 'repo' in extractor YAML params "
                "or use wildcard repo: '*' to iterate all repos."
            )
        return repo

    # ------------------------------------------------------------------
    # REST handlers
    # ------------------------------------------------------------------

    def _repos(self, params: dict) -> Iterator[dict]:
        """Yield all repos for an org."""
        org = self._get_org(params)
        endpoint = f"/orgs/{org}/repos?per_page=100"
        since = params.get("since")
        if since:
            endpoint += f"&since={since}"
        yield from self._rest(endpoint, paginate=True)

    def _teams(self, params: dict) -> Iterator[dict]:
        """Yield all teams for an org."""
        org = self._get_org(params)
        yield from self._rest(f"/orgs/{org}/teams?per_page=100", paginate=True)

    def _team_members(self, params: dict) -> Iterator[dict]:
        """Yield team-member records for an org.

        Without ``team_slug``: uses GraphQL to fetch all teams and their
        members in O(ceil(N_teams/25)) requests (25 teams × 100 members per
        page).  Teams with >100 members are handled via per-team member
        pagination.  Falls back to REST if GraphQL is unavailable.

        With ``team_slug``: fetches members of that one team via REST.
        """
        org = self._get_org(params)
        team_slug = params.get("team_slug")
        if team_slug:
            for member in self._rest(
                f"/orgs/{org}/teams/{team_slug}/members?per_page=100",
                paginate=True,
            ):
                member["_team_slug"] = team_slug
                member["_team_name"] = team_slug
                member["_team_id"] = None
                yield member
            return

        records = self._team_members_graphql(org)
        if records is None:
            records = self._team_members_rest(org)
        yield from records

    def _team_members_graphql(self, org: str) -> list[dict] | None:
        """Fetch all team memberships via GraphQL.

        Fetches 25 teams per page with up to 100 members each — a single query
        covers an org with up to 2 500 team-member pairs.  Teams that hit the
        100-member limit are re-fetched with per-team member pagination.

        Returns a flat list of dicts with ``_team_id``, ``_team_name``,
        ``_team_slug``, ``id``, ``login``, ``name``, or None if GraphQL is
        unavailable.
        """
        teams_query = """
        query($org: String!, $cursor: String) {
          organization(login: $org) {
            teams(first: 25, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                databaseId
                name
                slug
                members(first: 100) {
                  pageInfo { hasNextPage endCursor }
                  nodes {
                    databaseId
                    login
                    name
                  }
                }
              }
            }
          }
        }
        """
        members_query = """
        query($org: String!, $slug: String!, $cursor: String) {
          organization(login: $org) {
            team(slug: $slug) {
              members(first: 100, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  databaseId
                  login
                  name
                }
              }
            }
          }
        }
        """
        all_records: list[dict] = []
        teams_cursor: str | None = None

        while True:
            data = self._graphql(teams_query, {"org": org, "cursor": teams_cursor})
            if data is None:
                return None
            org_data = data.get("data", {}).get("organization")
            if not org_data:
                errors = data.get("errors", [])
                logger.warning(
                    "GraphQL team_members: org %r not accessible%s",
                    org,
                    f": {errors[0].get('message')}" if errors else "",
                )
                return None

            teams_page = org_data["teams"]
            for team in teams_page["nodes"]:
                team_id = team["databaseId"]
                team_name = team["name"]
                team_slug = team["slug"]

                # Emit the first page of members
                for member in team["members"]["nodes"]:
                    all_records.append({
                        "_team_id": team_id,
                        "_team_name": team_name,
                        "_team_slug": team_slug,
                        "id": member["databaseId"],
                        "login": member["login"],
                        "name": member.get("name"),
                    })

                # If this team has >100 members, paginate them separately
                members_page_info = team["members"]["pageInfo"]
                members_cursor: str | None = members_page_info["endCursor"]
                while members_page_info["hasNextPage"]:
                    m_data = self._graphql(
                        members_query,
                        {"org": org, "slug": team_slug, "cursor": members_cursor},
                    )
                    if m_data is None:
                        logger.warning(
                            "GraphQL team_members: failed to paginate members for team %r", team_slug
                        )
                        break
                    team_data = (m_data.get("data", {}).get("organization") or {}).get("team")
                    if not team_data:
                        break
                    m_page = team_data["members"]
                    for member in m_page["nodes"]:
                        all_records.append({
                            "_team_id": team_id,
                            "_team_name": team_name,
                            "_team_slug": team_slug,
                            "id": member["databaseId"],
                            "login": member["login"],
                            "name": member.get("name"),
                        })
                    members_page_info = m_page["pageInfo"]
                    members_cursor = members_page_info["endCursor"]

            if not teams_page["pageInfo"]["hasNextPage"]:
                break
            teams_cursor = teams_page["pageInfo"]["endCursor"]

        return all_records

    def _team_members_rest(self, org: str) -> list[dict]:
        """Fetch all team memberships via REST (fallback).

        Makes 1 list-teams call + 1 list-members call per team.
        """
        teams = self._rest(f"/orgs/{org}/teams?per_page=100", paginate=True)
        records = []
        for team in teams:
            team_id = team.get("id")
            team_name = team.get("name", "")
            team_slug = team.get("slug", "")
            members = self._rest(
                f"/orgs/{org}/teams/{team_slug}/members?per_page=100",
                paginate=True,
            )
            for member in members:
                records.append({
                    "_team_id": team_id,
                    "_team_name": team_name,
                    "_team_slug": team_slug,
                    "id": member.get("id"),
                    "login": member.get("login"),
                    "name": member.get("name"),
                })
        return records

    def _orgs(self, params: dict) -> Iterator[dict]:
        """Yield orgs the authenticated user belongs to."""
        yield from self._rest("/user/orgs", paginate=True)

    def _commits(self, params: dict) -> Iterator[dict]:
        """Yield commits for a repo."""
        org = self._get_org(params)
        repo = self._get_repo(params)
        endpoint = f"/repos/{org}/{repo}/commits?per_page=100"
        since = params.get("since")
        if since:
            endpoint += f"&since={since}"
        yield from self._rest(endpoint, paginate=True)

    def _pull_requests(self, params: dict) -> Iterator[dict]:
        """Yield pull requests for a repo."""
        org = self._get_org(params)
        repo = self._get_repo(params)
        endpoint = f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100"
        yield from self._rest(endpoint, paginate=True)

    def _releases(self, params: dict) -> Iterator[dict]:
        """Yield releases for a repo."""
        org = self._get_org(params)
        repo = self._get_repo(params)
        yield from self._rest(f"/repos/{org}/{repo}/releases?per_page=100", paginate=True)

    def _workflow_runs(self, params: dict) -> Iterator[dict]:
        """Yield workflow runs for a repo."""
        org = self._get_org(params)
        repo = self._get_repo(params)
        endpoint = f"/repos/{org}/{repo}/actions/runs?per_page=100"
        since = params.get("since")
        if since:
            endpoint += f"&created=>={since}"
        yield from self._rest(endpoint, paginate=True, result_key="workflow_runs")

    def _tags(self, params: dict) -> Iterator[dict]:
        """Yield tags for a repo."""
        org = self._get_org(params)
        repo = self._get_repo(params)
        yield from self._rest(f"/repos/{org}/{repo}/tags?per_page=100", paginate=True)

    def _pr_timeline(self, params: dict) -> Iterator[dict]:
        """Yield timeline events for recent pull requests in a repo.

        When ``since`` is present in params, only PRs with ``updated_at``
        >= that timestamp are processed.  PRs are sorted by updated desc,
        so once we hit a PR older than the cutoff we can stop early.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")
        prs = self._rest(
            f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
            paginate=True,
        )
        for pr in prs:
            if since and pr.get("updated_at", "") < since:
                break
            number = pr["number"]
            events = self._rest(
                f"/repos/{org}/{repo}/issues/{number}/timeline?per_page=100",
                paginate=True,
            )
            for event in events:
                event["_pr_number"] = number
                # Some event types lack created_at (GitHub omits it).
                # Provide a fallback so it is usable as a non-Nullable PK column.
                if not event.get("created_at"):
                    et = event.get("event", "")
                    if et == "committed":
                        event["created_at"] = (
                            (event.get("committer") or {}).get("date")
                            or (event.get("author") or {}).get("date")
                        )
                    elif et == "reviewed":
                        event["created_at"] = event.get("submitted_at")
                yield event

    def _issue_comments(self, params: dict) -> Iterator[dict]:
        """Yield issue/PR body comments for a repo via the bulk comments endpoint.

        The bulk endpoint natively supports a ``since`` filter, so no per-PR
        calls are needed. ``_pr_number`` is extracted from the ``issue_url``
        field on each comment.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        endpoint = (
            f"/repos/{org}/{repo}/issues/comments"
            "?per_page=100&sort=created&direction=desc"
        )
        since = params.get("since")
        if since:
            endpoint += f"&since={since}"
        for comment in self._rest(endpoint, paginate=True):
            issue_url = comment.get("issue_url", "")
            try:
                pr_number = int(issue_url.rsplit("/", 1)[-1]) if issue_url else None
            except (ValueError, IndexError):
                pr_number = None
            comment["_pr_number"] = pr_number
            yield comment

    # ------------------------------------------------------------------
    # GraphQL batch handlers (REST fallbacks below each)
    # ------------------------------------------------------------------

    def _pr_commits(self, params: dict) -> Iterator[dict]:
        """Yield commits for each PR, using GraphQL batch fetch with REST fallback.

        GraphQL fetches 20 PRs per page with up to 100 commits each, including
        additions and deletions — O(ceil(N/20)) requests instead of O(N).
        The REST fallback does not include additions/deletions.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")
        records = self._pr_commits_graphql(org, repo, since)
        if records is None:
            records = self._pr_commits_rest(org, repo, since)
        yield from records

    def _pr_commits_graphql(self, org: str, repo: str, since: str | None) -> list[dict] | None:
        """Fetch PR commits via GraphQL (20 PRs per page, 100 commits per PR).

        Includes additions and deletions, which are unavailable on the REST
        list-commits endpoint. Returns None if GraphQL is unavailable.
        """
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(first: 20, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                number
                updatedAt
                commits(first: 100) {
                  nodes {
                    commit {
                      oid
                      author {
                        name
                        user { login }
                      }
                      message
                      committedDate
                      additions
                      deletions
                    }
                  }
                }
              }
            }
          }
        }
        """
        all_records: list[dict] = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"owner": org, "repo": repo, "cursor": cursor})
            if data is None:
                return None
            repo_data = data.get("data", {}).get("repository")
            if not repo_data:
                return None
            prs_page = repo_data["pullRequests"]
            done = False
            for pr in prs_page["nodes"]:
                if since and pr["updatedAt"] < since:
                    done = True
                    break
                for node in pr["commits"]["nodes"]:
                    c = node["commit"]
                    all_records.append({
                        "_pr_number": pr["number"],
                        "sha": c["oid"],
                        "author": {"login": (c["author"].get("user") or {}).get("login")},
                        "commit": {
                            "author": {"name": c["author"].get("name")},
                            "message": c["message"],
                            "committer": {"date": c["committedDate"]},
                        },
                        "additions": c["additions"],
                        "deletions": c["deletions"],
                    })
            if done or not prs_page["pageInfo"]["hasNextPage"]:
                break
            cursor = prs_page["pageInfo"]["endCursor"]
        return all_records

    def _pr_commits_rest(self, org: str, repo: str, since: str | None) -> list[dict]:
        """REST fallback: fetch commits per PR. Does not include additions/deletions."""
        prs = self._rest(
            f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
            paginate=True,
        )
        records = []
        for pr in prs:
            if since and pr.get("updated_at", "") < since:
                break
            number = pr["number"]
            for commit in self._rest(
                f"/repos/{org}/{repo}/pulls/{number}/commits?per_page=100",
                paginate=True,
            ):
                commit["_pr_number"] = number
                records.append(commit)
        return records

    def _pr_reviews(self, params: dict) -> Iterator[dict]:
        """Yield reviews for each PR, using GraphQL batch fetch with REST fallback.

        GraphQL fetches 50 PRs per page with up to 100 reviews each —
        O(ceil(N/50)) requests instead of O(N) per-PR REST calls.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")
        records = self._pr_reviews_graphql(org, repo, since)
        if records is None:
            records = self._pr_reviews_rest(org, repo, since)
        yield from records

    def _pr_reviews_graphql(self, org: str, repo: str, since: str | None) -> list[dict] | None:
        """Fetch all PR reviews via GraphQL (50 PRs per page, 100 reviews per PR).

        Returns flat records shaped to match the extractor YAML json_paths,
        or None if GraphQL is unavailable.
        """
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(first: 50, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                number
                updatedAt
                reviews(first: 100) {
                  nodes {
                    databaseId
                    author { login }
                    state
                    submittedAt
                  }
                }
              }
            }
          }
        }
        """
        all_records: list[dict] = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"owner": org, "repo": repo, "cursor": cursor})
            if data is None:
                return None
            repo_data = data.get("data", {}).get("repository")
            if not repo_data:
                return None
            prs_page = repo_data["pullRequests"]
            done = False
            for pr in prs_page["nodes"]:
                if since and pr["updatedAt"] < since:
                    done = True
                    break
                for review in pr["reviews"]["nodes"]:
                    all_records.append({
                        "_pr_number": pr["number"],
                        "id": review["databaseId"],
                        "user": {"login": (review.get("author") or {}).get("login")},
                        "state": review["state"],
                        "submitted_at": review.get("submittedAt"),
                    })
            if done or not prs_page["pageInfo"]["hasNextPage"]:
                break
            cursor = prs_page["pageInfo"]["endCursor"]
        return all_records

    def _pr_reviews_rest(self, org: str, repo: str, since: str | None) -> list[dict]:
        """REST fallback: fetch reviews per PR."""
        prs = self._rest(
            f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
            paginate=True,
        )
        records = []
        for pr in prs:
            if since and pr.get("updated_at", "") < since:
                break
            number = pr["number"]
            for review in self._rest(
                f"/repos/{org}/{repo}/pulls/{number}/reviews?per_page=100",
                paginate=True,
            ):
                review["_pr_number"] = number
                records.append(review)
        return records

    def _pr_review_comments(self, params: dict) -> Iterator[dict]:
        """Yield inline review comments for each PR, using GraphQL batch fetch with REST fallback.

        GraphQL fetches 30 PRs per page with up to 100 review threads (10 comments
        each) — O(ceil(N/30)) requests instead of O(N) per-PR REST calls.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")
        records = self._pr_review_comments_graphql(org, repo, since)
        if records is None:
            records = self._pr_review_comments_rest(org, repo, since)
        yield from records

    def _pr_review_comments_graphql(self, org: str, repo: str, since: str | None) -> list[dict] | None:
        """Fetch inline review comments via GraphQL (30 PRs, 100 threads, 10 comments per page).

        Traverses reviewThreads → comments. The limits cover all but extraordinarily
        large PRs. Returns None if GraphQL is unavailable.
        """
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(first: 30, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                number
                updatedAt
                reviewThreads(first: 100) {
                  nodes {
                    comments(first: 10) {
                      nodes {
                        databaseId
                        pullRequestReview { databaseId }
                        author { login }
                        createdAt
                        path
                        originalPosition
                        body
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        all_records: list[dict] = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"owner": org, "repo": repo, "cursor": cursor})
            if data is None:
                return None
            repo_data = data.get("data", {}).get("repository")
            if not repo_data:
                return None
            prs_page = repo_data["pullRequests"]
            done = False
            for pr in prs_page["nodes"]:
                if since and pr["updatedAt"] < since:
                    done = True
                    break
                for thread in pr["reviewThreads"]["nodes"]:
                    for comment in thread["comments"]["nodes"]:
                        all_records.append({
                            "_pr_number": pr["number"],
                            "id": comment["databaseId"],
                            "pull_request_review_id": (comment.get("pullRequestReview") or {}).get("databaseId"),
                            "user": {"login": (comment.get("author") or {}).get("login")},
                            "created_at": comment["createdAt"],
                            "path": comment["path"],
                            "position": comment.get("originalPosition"),
                            "body": comment["body"],
                        })
            if done or not prs_page["pageInfo"]["hasNextPage"]:
                break
            cursor = prs_page["pageInfo"]["endCursor"]
        return all_records

    def _pr_review_comments_rest(self, org: str, repo: str, since: str | None) -> list[dict]:
        """REST fallback: fetch inline review comments per PR."""
        prs = self._rest(
            f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
            paginate=True,
        )
        records = []
        for pr in prs:
            if since and pr.get("updated_at", "") < since:
                break
            number = pr["number"]
            for comment in self._rest(
                f"/repos/{org}/{repo}/pulls/{number}/comments?per_page=100",
                paginate=True,
            ):
                comment["_pr_number"] = number
                records.append(comment)
        return records

    def _requested_reviewers(self, params: dict) -> Iterator[dict]:
        """Yield requested reviewers for each PR, using GraphQL batch fetch with REST fallback.

        GraphQL fetches 50 PRs per page with up to 50 review requests each.
        Note: GitHub removes a reviewer from this list once they submit their
        review, so this reflects pending requests at extraction time only.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")
        records = self._requested_reviewers_graphql(org, repo, since)
        if records is None:
            records = self._requested_reviewers_rest(org, repo, since)
        yield from records

    def _requested_reviewers_graphql(self, org: str, repo: str, since: str | None) -> list[dict] | None:
        """Fetch requested reviewers via GraphQL (50 PRs per page, 50 requests per PR).

        Uses inline fragments to handle both User and Team reviewer types.
        Returns None if GraphQL is unavailable.
        """
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(first: 50, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                number
                updatedAt
                reviewRequests(first: 50) {
                  nodes {
                    requestedReviewer {
                      ... on User { login }
                      ... on Team { slug }
                    }
                  }
                }
              }
            }
          }
        }
        """
        all_records: list[dict] = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"owner": org, "repo": repo, "cursor": cursor})
            if data is None:
                return None
            repo_data = data.get("data", {}).get("repository")
            if not repo_data:
                return None
            prs_page = repo_data["pullRequests"]
            done = False
            for pr in prs_page["nodes"]:
                if since and pr["updatedAt"] < since:
                    done = True
                    break
                for req in pr["reviewRequests"]["nodes"]:
                    reviewer = req.get("requestedReviewer") or {}
                    login = reviewer.get("login") or reviewer.get("slug")
                    if not login:
                        continue
                    all_records.append({
                        "_pr_number": pr["number"],
                        "_reviewer_login": login,
                        "_reviewer_type": "USER" if "login" in reviewer else "TEAM",
                    })
            if done or not prs_page["pageInfo"]["hasNextPage"]:
                break
            cursor = prs_page["pageInfo"]["endCursor"]
        return all_records

    def _requested_reviewers_rest(self, org: str, repo: str, since: str | None) -> list[dict]:
        """REST fallback: fetch requested reviewers per PR."""
        prs = self._rest(
            f"/repos/{org}/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
            paginate=True,
        )
        records = []
        for pr in prs:
            if since and pr.get("updated_at", "") < since:
                break
            number = pr["number"]
            data = self._rest(
                f"/repos/{org}/{repo}/pulls/{number}/requested_reviewers",
            )
            resp = (data[0] if isinstance(data, list) else data) if data else {}
            for user in resp.get("users", []):
                records.append({"_pr_number": number, "_reviewer_login": user["login"], "_reviewer_type": "USER"})
            for team in resp.get("teams", []):
                records.append({"_pr_number": number, "_reviewer_login": team["slug"], "_reviewer_type": "TEAM"})
        return records

    def _deployments(self, params: dict) -> Iterator[dict]:
        """Yield deployments for a repo with latest status via GraphQL.

        Uses a single GraphQL query per repo to fetch deployments and
        their latest status together, avoiding N+1 REST API calls.
        Falls back to REST (without statuses) if GraphQL fails.

        When ``since`` is present, deployments with ``updated_at``
        before the cutoff are skipped.
        """
        org = self._get_org(params)
        repo = self._get_repo(params)
        since = params.get("since")

        deployments = self._deployments_graphql(org, repo)
        if deployments is None:
            deployments = self._deployments_rest(org, repo)

        for dep in deployments:
            if since and dep.get("updated_at", "") < since:
                break
            yield dep

    def _deployments_graphql(self, org: str, repo: str) -> list[dict] | None:
        """Fetch deployments + latest status in a single GraphQL query per page.

        Returns a list of flat dicts matching the REST shape, or None if
        GraphQL is unavailable.
        """
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            deployments(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                databaseId
                commitOid
                ref { name }
                environment
                description
                creator { login }
                createdAt
                updatedAt
                latestStatus {
                  state
                  createdAt
                }
              }
            }
          }
        }
        """
        all_deps = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"owner": org, "repo": repo, "cursor": cursor})
            if data is None:
                return None
            repo_data = data.get("data", {}).get("repository")
            if not repo_data:
                return None
            deploys = repo_data["deployments"]
            for node in deploys["nodes"]:
                latest = node.get("latestStatus") or {}
                all_deps.append({
                    "id": node["databaseId"],
                    "sha": node["commitOid"],
                    "ref": (node.get("ref") or {}).get("name"),
                    "environment": node["environment"],
                    "description": node.get("description"),
                    "creator": {"login": (node.get("creator") or {}).get("login")},
                    "created_at": node["createdAt"],
                    "updated_at": node["updatedAt"],
                    "_status": latest.get("state", "").lower() if latest.get("state") else None,
                    "_status_at": latest.get("createdAt"),
                })
            if not deploys["pageInfo"]["hasNextPage"]:
                break
            cursor = deploys["pageInfo"]["endCursor"]
        return all_deps

    def _deployments_rest(self, org: str, repo: str) -> list[dict]:
        """Fetch deployments via REST without statuses (fallback)."""
        deployments = self._rest(
            f"/repos/{org}/{repo}/deployments?per_page=100",
            paginate=True,
        )
        for dep in deployments:
            dep["_status"] = None
            dep["_status_at"] = None
        return deployments

    def _users(self, params: dict) -> Iterator[dict]:
        """Yield enriched profiles for all members of an org.

        Uses GraphQL to fetch login, name, email, and company in a single
        paginated query (ceil(N/100) requests) instead of 1 + N REST calls.
        Falls back to the REST members list + per-user profile calls if
        GraphQL is unavailable.
        """
        org = self._get_org(params)
        profiles = self._users_graphql(org)
        if profiles is None:
            profiles = self._users_rest(org)
        yield from profiles

    def _users_graphql(self, org: str) -> list[dict] | None:
        """Fetch all org member profiles via GraphQL.

        Returns a list of flat dicts with id/login/name/email/company,
        or None if GraphQL is unavailable.
        """
        query = """
        query($org: String!, $cursor: String) {
          organization(login: $org) {
            membersWithRole(first: 100, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                databaseId
                login
                name
                email
                company
              }
            }
          }
        }
        """
        all_users = []
        cursor: str | None = None
        while True:
            data = self._graphql(query, {"org": org, "cursor": cursor})
            if data is None:
                logger.warning("GraphQL users query failed for org %r — check token permissions (read:org scope)", org)
                return None
            org_data = data.get("data", {}).get("organization")
            if not org_data:
                errors = data.get("errors", [])
                logger.warning(
                    "GraphQL users: org %r not accessible%s — check token permissions (read:org scope)",
                    org,
                    f": {errors[0].get('message')}" if errors else "",
                )
                return None
            members = org_data["membersWithRole"]
            for node in members["nodes"]:
                all_users.append({
                    "id": node["databaseId"],
                    "login": node["login"],
                    "name": node.get("name"),
                    "email": node.get("email"),
                    "company": node.get("company"),
                })
            if not members["pageInfo"]["hasNextPage"]:
                break
            cursor = members["pageInfo"]["endCursor"]
        return all_users

    def _users_rest(self, org: str) -> list[dict]:
        """Fetch org member profiles via REST (fallback).

        Makes 1 + N requests: one paginated members list, then one
        /users/{login} call per member to retrieve name and email.
        """
        members = self._rest(f"/orgs/{org}/members?per_page=100", paginate=True)
        if not members:
            logger.warning(
                "REST users: no members returned for org %r — check token permissions (read:org scope)", org
            )
        profiles = []
        for member in members:
            login = member.get("login")
            if not login:
                continue
            result = self._rest(f"/users/{login}")
            if result:
                profile = result[0] if isinstance(result, list) else result
                profiles.append({
                    "id": profile.get("id"),
                    "login": profile.get("login"),
                    "name": profile.get("name"),
                    "email": profile.get("email"),
                    "company": profile.get("company"),
                })
        return profiles
