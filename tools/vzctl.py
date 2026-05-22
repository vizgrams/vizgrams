# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""vzctl — Vizgrams model-as-code sync CLI.

Walks a local model directory and PUTs each artifact (entity, mapper,
feature, query, view, application, extractor) to a Vizgrams API.
Authenticates with a service-account token via ``X-API-Key``.

Designed for CI:
  - environment-driven configuration (``VZ_API_URL``, ``VZ_API_KEY``)
  - non-zero exit on any failure
  - ``--dry-run`` for plan-only mode
  - human-readable + machine-parsable per-artifact output

Usage::

    export VZ_API_URL=https://vizgrams.com
    export VZ_API_KEY=vzsa_...
    vzctl sync <model> [--model-dir <path>] [--dry-run] [--prune]

The ``--prune`` flag (full-sync semantics — delete server artifacts
absent locally) is parsed but currently rejected with a clear error.
The API DELETE endpoints it needs land in VG-135.
"""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import click
import requests
import yaml

# ---------------------------------------------------------------------------
# Per-artifact-type registry
# ---------------------------------------------------------------------------


@dataclass
class ArtifactSpec:
    """How to find a kind of artifact locally and address it via the API.

    Most artifact kinds are uniformly named after their filename stem and
    addressed by ``/<kind>/<name>``. Features and extractors are the odd
    ones out: features carry their name in the YAML body (``feature_id``)
    and need the entity in the URL; extractors strip a filename prefix.
    """
    kind: str
    local_dir: str
    filename_glob: str
    name_from_path: Callable[[Path], str]
    # GET / PUT URL builders (suffix only; client prepends model prefix).
    get_url: Callable[[str, dict], str]
    put_url: Callable[[str, dict], str]
    # Optional: derive the API name from YAML content rather than the path.
    name_from_content: Callable[[dict, Path], str] | None = None
    # Optional: derive additional URL path parts (e.g. {"entity": "Issue"}) from YAML.
    extra_path_parts: Callable[[dict], dict] | None = None


def _stem(path: Path) -> str:
    return path.stem


def _stem_strip_extractor(path: Path) -> str:
    return path.stem.removeprefix("extractor_")


# Synced in the order listed: entities first (so mappers / features / queries
# that reference them validate cleanly).
SPECS: list[ArtifactSpec] = [
    ArtifactSpec(
        kind="entity",
        local_dir="ontology",
        filename_glob="*.yaml",
        name_from_path=_stem,
        get_url=lambda n, _e: f"/entity/{n}",
        put_url=lambda n, _e: f"/entity/{n}/yaml",
    ),
    ArtifactSpec(
        kind="mapper",
        local_dir="mappers",
        filename_glob="*.yaml",
        name_from_path=_stem,
        get_url=lambda n, _e: f"/mapper/{n}",
        put_url=lambda n, _e: f"/mapper/{n}",
    ),
    ArtifactSpec(
        kind="feature",
        local_dir="features",
        filename_glob="*.yaml",
        name_from_path=_stem,
        name_from_content=lambda d, p: d.get("feature_id", p.stem),
        extra_path_parts=lambda d: {"entity": d.get("entity_type")},
        get_url=lambda n, e: f"/entity/{e['entity']}/feature/{n}",
        put_url=lambda n, e: f"/entity/{e['entity']}/feature/{n}",
    ),
    ArtifactSpec(
        kind="query",
        local_dir="queries",
        filename_glob="*.yaml",
        name_from_path=_stem,
        get_url=lambda n, _e: f"/query/{n}",
        put_url=lambda n, _e: f"/query/{n}",
    ),
    ArtifactSpec(
        kind="view",
        local_dir="views",
        filename_glob="*.yaml",
        name_from_path=_stem,
        get_url=lambda n, _e: f"/view/{n}",
        put_url=lambda n, _e: f"/view/{n}",
    ),
    ArtifactSpec(
        kind="application",
        local_dir="applications",
        filename_glob="*.yaml",
        name_from_path=_stem,
        get_url=lambda n, _e: f"/application/{n}",
        put_url=lambda n, _e: f"/application/{n}",
    ),
    ArtifactSpec(
        kind="extractor",
        local_dir="extractors",
        filename_glob="extractor_*.yaml",
        name_from_path=_stem_strip_extractor,
        get_url=lambda n, _e: f"/tool/{n}/extract",
        put_url=lambda n, _e: f"/tool/{n}/extract",
    ),
]


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class ApiClient:
    """Thin requests wrapper that pins the model and adds the SA header."""

    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.session = requests.Session()
        self.session.headers["X-API-Key"] = api_key
        self.session.headers["Content-Type"] = "application/json"

    def _url(self, suffix: str) -> str:
        return f"{self.base_url}/api/v1/model/{self.model}{suffix}"

    def get(self, suffix: str) -> requests.Response:
        return self.session.get(self._url(suffix), timeout=30)

    def put(self, suffix: str, body: dict) -> requests.Response:
        return self.session.put(self._url(suffix), json=body, timeout=30)


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------


@dataclass
class Action:
    """One artifact's sync outcome."""
    kind: str
    name: str
    status: str   # "created" | "updated" | "unchanged" | "failed"
    error: str = ""


@dataclass
class SyncResult:
    actions: list[Action] = field(default_factory=list)

    def _by_status(self, status: str) -> list[Action]:
        return [a for a in self.actions if a.status == status]

    @property
    def created(self) -> list[Action]:    return self._by_status("created")
    @property
    def updated(self) -> list[Action]:    return self._by_status("updated")
    @property
    def unchanged(self) -> list[Action]:  return self._by_status("unchanged")
    @property
    def failed(self) -> list[Action]:     return self._by_status("failed")


def _hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


def sync_artifact(
    client: ApiClient, spec: ArtifactSpec, path: Path, dry_run: bool,
) -> Action:
    """Sync a single artifact file and return what happened."""
    fallback_name = spec.name_from_path(path)
    try:
        content = path.read_text()
        parsed = yaml.safe_load(content)
        if not isinstance(parsed, dict):
            return Action(spec.kind, fallback_name, "failed",
                          "YAML root is not a mapping")

        name = (
            spec.name_from_content(parsed, path)
            if spec.name_from_content else fallback_name
        )
        extras = spec.extra_path_parts(parsed) if spec.extra_path_parts else {}
        for k, v in extras.items():
            if not v:
                return Action(spec.kind, name, "failed",
                              f"missing required {k!r} in YAML")

        # GET current server state for unchanged detection. Network failures
        # fall through silently — the subsequent PUT will surface the issue
        # with a clearer error.
        existing_content = None
        try:
            r = client.get(spec.get_url(name, extras))
            if r.status_code == 200:
                body = r.json() or {}
                existing_content = body.get("raw_yaml")
        except (requests.RequestException, ValueError):
            existing_content = None

        if existing_content and _hash(existing_content) == _hash(content):
            return Action(spec.kind, name, "unchanged")

        new_status = "updated" if existing_content else "created"
        if dry_run:
            return Action(spec.kind, name, new_status)

        r = client.put(spec.put_url(name, extras), {"content": content})
        if r.status_code in (200, 201, 202):
            return Action(spec.kind, name, new_status)

        # Try to extract a useful error message from the body.
        try:
            detail = r.json().get("detail")
        except ValueError:
            detail = r.text[:200]
        return Action(spec.kind, name, "failed",
                      f"HTTP {r.status_code}: {detail}")

    except Exception as exc:  # noqa: BLE001 — surface the error string
        return Action(spec.kind, fallback_name, "failed", str(exc))


def sync_model(
    client: ApiClient, model_dir: Path, *, dry_run: bool = False,
) -> SyncResult:
    """Sync every artifact found in *model_dir*. Additive — never deletes."""
    result = SyncResult()
    for spec in SPECS:
        subdir = model_dir / spec.local_dir
        if not subdir.is_dir():
            continue
        for path in sorted(subdir.glob(spec.filename_glob)):
            result.actions.append(sync_artifact(client, spec, path, dry_run))
    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

_STATUS_MARKER = {
    "created":   "+",
    "updated":   "~",
    "unchanged": ".",
    "failed":    "x",
}


def print_report(result: SyncResult, *, dry_run: bool = False) -> None:
    """Print a per-artifact summary to stdout and totals to stderr."""
    for a in result.actions:
        line = f"  {_STATUS_MARKER[a.status]} {a.kind}/{a.name}"
        if a.error:
            line += f"  — {a.error}"
        click.echo(line)
    suffix = " (DRY RUN — nothing applied)" if dry_run else ""
    click.echo(
        f"\n{len(result.created)} created · {len(result.updated)} updated · "
        f"{len(result.unchanged)} unchanged · {len(result.failed)} failed"
        + suffix,
        err=True,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
def cli() -> None:
    """vzctl — Vizgrams model-as-code CLI."""


@cli.command()
@click.argument("model")
@click.option(
    "--api-url", envvar="VZ_API_URL", required=True,
    help="Base URL of the Vizgrams API (e.g. https://vizgrams.com). "
         "Env: VZ_API_URL.",
)
@click.option(
    "--api-key", envvar="VZ_API_KEY", required=True,
    help="Service-account token scoped to MODEL. Env: VZ_API_KEY.",
)
@click.option(
    "--model-dir", default=".", show_default=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Local directory containing ontology/, mappers/, queries/, etc.",
)
@click.option(
    "--dry-run", is_flag=True,
    help="Plan only — print actions, do not write anything.",
)
@click.option(
    "--prune", is_flag=True,
    help="Full-sync semantics: delete server artifacts not present locally. "
         "Not yet implemented (VG-135).",
)
def sync(
    model: str,
    api_url: str,
    api_key: str,
    model_dir: str,
    dry_run: bool,
    prune: bool,
) -> None:
    """Sync a local model directory to a Vizgrams API.

    Walks MODEL_DIR's standard subdirectories (ontology, mappers, features,
    queries, views, applications, extractors) and PUTs each artifact to the
    API under /api/v1/model/MODEL/...
    """
    if prune:
        click.echo(
            "ERROR: --prune is not yet supported. Server-side DELETE "
            "endpoints land in VG-135. Run without --prune for additive sync.",
            err=True,
        )
        sys.exit(2)

    client = ApiClient(api_url, api_key, model)
    result = sync_model(client, Path(model_dir), dry_run=dry_run)
    print_report(result, dry_run=dry_run)
    if result.failed:
        sys.exit(1)


if __name__ == "__main__":
    cli()
