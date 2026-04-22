# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""GitHub CODEOWNERS tool: extracts CODEOWNERS data from repos via the REST API."""

import base64
import logging
import re
import time
from collections.abc import Iterator

import requests

from tools.base import BaseTool

logger = logging.getLogger(__name__)

COMMANDS = ["codeowners"]

# Paths to check for CODEOWNERS, in order of priority
CODEOWNERS_PATHS = [".github/CODEOWNERS", "CODEOWNERS"]


def _parse_next_link(link_header: str) -> str | None:
    """Extract the 'next' URL from a Link header, or None."""
    if not link_header:
        return None
    for part in link_header.split(","):
        m = re.match(r'\s*<([^>]+)>;\s*rel="next"', part.strip())
        if m:
            return m.group(1)
    return None


def _parse_codeowners(content: str) -> list[tuple[str, str]]:
    """Parse CODEOWNERS file content into (pattern, owner) pairs.

    Multi-owner lines are expanded into multiple pairs.
    """
    pairs = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        pattern = parts[0]
        for owner in parts[1:]:
            pairs.append((pattern, owner))
    return pairs


class CodeownersTool(BaseTool):
    """GitHub CODEOWNERS data source using the REST API."""

    def __init__(self, config: dict, **_kwargs):
        """config: resolved tool config dict (inherits org/host from the git config block)."""
        from core.model_config import resolve_credential

        self.default_org: str | None = config.get("org")
        self.host: str | None = config.get("host")

        token_raw = config.get("token", "")
        token = resolve_credential(token_raw) if token_raw else None

        effective_host = self.host or "github.com"
        if effective_host != "github.com":
            self._rest_base = f"https://{effective_host}/api/v3"
        else:
            self._rest_base = "https://api.github.com"

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

    def _rate_limit_wait(self, resp: requests.Response) -> int:
        try:
            reset_epoch = int(resp.headers.get("X-RateLimit-Reset", "0"))
            if reset_epoch:
                return max(reset_epoch - int(time.time()), 0) + 5
        except ValueError:
            pass
        return 60

    def _rest(self, endpoint: str, paginate: bool = False) -> list[dict]:
        """Call a GitHub REST endpoint and return a flat list of records.

        Returns [] on 404 or non-rate-limited 403 (file not found / no access).
        """
        url = endpoint if endpoint.startswith("http") else self._rest_base + endpoint
        all_items: list[dict] = []
        max_retries = 3
        while url:
            for attempt in range(max_retries + 1):
                resp = self._session.request("GET", url, timeout=30)
                if resp.status_code == 200:
                    break
                if resp.status_code == 404:
                    return all_items
                if self._is_rate_limited(resp):
                    wait = self._rate_limit_wait(resp)
                    logger.warning("REST rate limited, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 403:
                    logger.warning("GET %s: 403 forbidden — check token permissions", url)
                    return all_items
                if attempt < max_retries:
                    delay = 2 ** (attempt + 1)
                    logger.warning("GET %s: %d, retrying in %ds", url, resp.status_code, delay)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
            else:
                break

            data = resp.json()
            if isinstance(data, list):
                all_items.extend(data)
            else:
                all_items.append(data)

            if not paginate:
                break
            url = _parse_next_link(resp.headers.get("Link", ""))
        return all_items

    # ------------------------------------------------------------------
    # BaseTool interface
    # ------------------------------------------------------------------

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        params = params or {}
        if command != "codeowners":
            raise ValueError(f"Unknown codeowners command: {command!r}")
        yield from self._codeowners(params)

    def resolve_wildcard(self, param_name: str, param_value: str) -> list:
        if param_name == "org" and param_value == "*":
            return [o.get("login") for o in self._rest("/user/orgs", paginate=True) if o.get("login")]
        raise NotImplementedError(f"Wildcard not supported for {param_name!r}")

    def _get_org(self, params: dict) -> str:
        org = params.get("org") or self.default_org
        if not org:
            raise ValueError(
                "GitHub org not set. Either pass 'org' in extractor YAML params "
                "or set 'org' in the git tool config in models/<model>/config.yaml"
            )
        return org

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _codeowners(self, params: dict) -> Iterator[dict]:
        """Yield one record per (pattern, owner) pair across all repos in an org."""
        org = self._get_org(params)
        repos = self._rest(f"/orgs/{org}/repos?per_page=100", paginate=True)
        total = len(repos)

        for idx, repo_data in enumerate(repos, 1):
            repo_name = repo_data["name"]
            logger.info("  [%d/%d] %s", idx, total, repo_name)
            content = self._fetch_codeowners(org, repo_name)
            if content is None:
                logger.debug("No CODEOWNERS found for %s/%s", org, repo_name)
                continue

            for pattern, owner in _parse_codeowners(content):
                yield {"repo": repo_name, "pattern": pattern, "owner": owner}

    def _fetch_codeowners(self, org: str, repo: str) -> str | None:
        """Try to fetch CODEOWNERS from known paths; return decoded content or None."""
        for path in CODEOWNERS_PATHS:
            data = self._rest(f"/repos/{org}/{repo}/contents/{path}")
            if data and data[0].get("content"):
                return base64.b64decode(data[0]["content"]).decode("utf-8")
        return None
