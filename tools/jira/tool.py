# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Jira tool: connects to Jira Cloud and yields raw API records."""

import logging
from collections.abc import Iterator
from datetime import datetime

from atlassian import Jira

from core.retry import retry_on_transient
from tools.base import BaseTool

logger = logging.getLogger(__name__)

COMMANDS = ["boards", "sprints", "sprint_reports", "issues", "search", "projects", "fields", "users"]


def _jql_date(value: str) -> str:
    """Convert a date/datetime string to Jira JQL-safe format (yyyy-MM-dd HH:mm).

    Jira JQL does not accept ISO 8601 with 'T' separator or timezone offsets.
    """
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    # Already a plain date like "2025-10-01" — pass through
    return value


class JiraTool(BaseTool):
    """Jira Cloud data source using atlassian-python-api."""

    def __init__(self, config: dict, **_kwargs):
        """config: resolved tool config dict from models/<model>/config.yaml."""
        self.jira = Jira(
            url=config["server"],
            username=config["email"],
            password=config["api_token"],
            cloud=True,
        )

        # The atlassian library logs at ERROR when it receives an empty response
        # body (e.g. sprint report endpoints for Kanban boards). We handle these
        # cases ourselves, so suppress the library's noise.
        logging.getLogger("atlassian").setLevel(logging.WARNING)

    def _call(self, fn, *args, **kwargs):
        """Wrap a Jira API call with retry on transient errors."""
        return retry_on_transient(fn, *args, **kwargs)

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        params = params or {}
        handler = {
            "boards": self._boards,
            "sprints": self._sprints,
            "sprint_reports": self._sprint_reports,
            "issues": self._issues,
            "search": self._search,
            "projects": self._projects,
            "fields": self._fields,
            "users": self._users,
        }.get(command)
        if handler is None:
            raise ValueError(f"Unknown Jira command: {command!r}")
        yield from handler(params)

    def resolve_wildcard(self, param_name: str, param_value: str) -> list:
        if param_name == "board_id" and param_value == "*":
            # Return strings so context column values match the TEXT column type.
            return [str(b["id"]) for b in self._boards({})]
        if param_name == "project" and param_value == "*":
            return [p["key"] for p in self._projects({}) if p.get("key")]
        raise NotImplementedError(f"Wildcard not supported for {param_name!r}")

    def _boards(self, params: dict) -> Iterator[dict]:
        """Yield all agile boards with offset-based pagination."""
        start = 0
        while True:
            response = self._call(self.jira.get_all_agile_boards, start=start, limit=50)
            values = response.get("values", [])
            if not values:
                break
            yield from values
            if response.get("isLast", True):
                break
            start += len(values)

    def _sprints(self, params: dict) -> Iterator[dict]:
        """Yield sprints for a given board_id. Skips boards that don't support sprints."""
        board_id = params.get("board_id")
        if not board_id:
            raise ValueError("sprints command requires board_id param")
        since = params.get("since")
        start = 0
        try:
            while True:
                response = self._call(self.jira.get_all_sprint, board_id, start=start, limit=50)
                values = response.get("values", [])
                if not values:
                    break
                for sprint in values:
                    if since and sprint.get("endDate") and sprint["endDate"] < since:
                        continue
                    yield sprint
                if response.get("isLast", True):
                    break
                start += len(values)
        except Exception as e:
            if "does not support sprints" in str(e):
                logger.debug("Board %s does not support sprints, skipping", board_id)
                return
            raise

    def _sprint_reports(self, params: dict) -> Iterator[dict]:
        """Yield raw sprint report responses for all sprints of a given board.

        Uses the GreenHopper internal endpoint — the public Agile REST API has
        no sprint report endpoint.  Yields the raw response dict unchanged.
        """
        board_id = params.get("board_id")
        if not board_id:
            raise ValueError("sprint_reports command requires board_id param")

        sprint_params = {"board_id": board_id}
        if params.get("since"):
            sprint_params["since"] = params["since"]

        try:
            for sprint in self._sprints(sprint_params):
                sprint_id = sprint.get("id")
                if sprint_id is None:
                    continue
                try:
                    report = self._call(
                        self.jira.get,
                        "rest/greenhopper/1.0/rapid/charts/sprintreport",
                        params={"rapidViewId": board_id, "sprintId": sprint_id},
                    )
                    if not report:
                        continue

                    yield report
                except Exception as e:
                    msg = str(e)
                    if "404" in msg or "Expecting value" in msg:
                        logger.debug(
                            "Sprint report not available board=%s sprint=%s: %s",
                            board_id, sprint_id, e,
                        )
                    else:
                        logger.warning(
                            "Sprint report fetch failed board=%s sprint=%s: %s",
                            board_id, sprint_id, e,
                        )
        except Exception as e:
            if "does not support sprints" in str(e):
                return
            raise

    def _issues(self, params: dict) -> Iterator[dict]:
        """Yield all issues for a project via JQL search."""
        project = params.get("project")
        if not project:
            raise ValueError("issues command requires project param")

        jql = f"project = {project} ORDER BY updated DESC"
        since = params.get("since")
        if since:
            jql = f'project = {project} AND updated >= "{_jql_date(since)}" ORDER BY updated DESC'
        expand = params.get("expand")
        next_page_token = None

        while True:
            response = self._call(self.jira.enhanced_jql,
                jql,
                limit=50,
                expand=expand,
                nextPageToken=next_page_token,
            )
            issues = response.get("issues", [])
            if not issues:
                break
            yield from issues
            if response.get("isLast", True):
                break
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    def _search(self, params: dict) -> Iterator[dict]:
        """Yield issues matching a JQL query with token-based pagination."""
        jql = params.get("jql", "")
        since = params.get("since")
        if since:
            safe = _jql_date(since)
            jql = f'{jql} AND updated >= "{safe}"' if jql else f'updated >= "{safe}"'
        expand = params.get("expand")
        next_page_token = None

        while True:
            response = self._call(self.jira.enhanced_jql,
                jql,
                limit=50,
                expand=expand,
                nextPageToken=next_page_token,
            )
            issues = response.get("issues", [])
            if not issues:
                break
            yield from issues
            if response.get("isLast", True):
                break
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    def _projects(self, params: dict) -> Iterator[dict]:
        """Yield all projects."""
        projects = self._call(self.jira.projects, included_archived=None)
        if isinstance(projects, list):
            yield from projects

    def _fields(self, params: dict) -> Iterator[dict]:
        """Yield all field definitions."""
        fields = self._call(self.jira.get_all_fields)
        if isinstance(fields, list):
            yield from fields

    def _users(self, params: dict) -> Iterator[dict]:
        """Yield active human Jira users via the bulk listing endpoint.

        Filters to accountType=atlassian and active=true to exclude bots,
        JSM portal customers, and deactivated/deleted accounts ("former user").
        The endpoint has no date filter so this is always a full refresh.
        """
        start = 0
        limit = 200
        while True:
            response = self._call(
                self.jira.get,
                "rest/api/3/users",
                params={"startAt": start, "maxResults": limit},
            )
            if not response:
                break
            for user in response:
                if user.get("active") and user.get("accountType") == "atlassian":
                    yield user
            if len(response) < limit:
                break
            start += len(response)
