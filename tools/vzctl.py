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


# ---------------------------------------------------------------------------
# Backup / restore — DuckDB-backed models only
# ---------------------------------------------------------------------------

def _load_duckdb_backend(model_dir_path: Path):
    """Return a connected DuckDBBackend for the model at ``model_dir_path``.

    Raises SystemExit with a useful message if the model is configured for
    a different backend.
    """
    from core.db import DuckDBBackend, get_backend
    backend = get_backend(model_dir_path)
    if not isinstance(backend, DuckDBBackend):
        click.echo(
            f"ERROR: model at {model_dir_path} uses backend "
            f"{type(backend).__name__}, not DuckDB. Backup / restore via "
            "vzctl is only implemented for DuckDB-backed models.",
            err=True,
        )
        sys.exit(2)
    backend.connect()
    return backend


def _maybe_configure_s3(backend, region: str | None) -> None:
    """Wire up the AWS credential chain on the backend when URI is s3://.

    Explicit key/secret aren't accepted on the CLI; if you need them, set
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the env and the
    credential-chain provider picks them up.
    """
    backend.configure_s3_credentials(
        use_credential_chain=True,
        region=region,
    )


@cli.command()
@click.argument("uri")
@click.option(
    "--model-dir", default=".", show_default=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Local directory containing the model's config.yaml.",
)
@click.option(
    "--region", envvar="AWS_REGION",
    help="AWS region for the S3 bucket. Env: AWS_REGION.",
)
def backup(uri: str, model_dir: str, region: str | None) -> None:
    """Back up a DuckDB-backed model to URI (local path or s3://bucket/prefix).

    Exports every table in the model's data DB as parquet via DuckDB's
    EXPORT DATABASE. On S3, expects credentials available via the standard
    AWS credential chain (env vars, ~/.aws/credentials, or instance/task role).

    Restore later with: vzctl restore URI
    """
    backend = _load_duckdb_backend(Path(model_dir))
    try:
        if uri.startswith("s3://"):
            _maybe_configure_s3(backend, region)
        click.echo(f"Backing up to {uri} …", err=True)
        backend.export_database(uri)
        click.echo("Backup complete.", err=True)
    finally:
        backend.close()


@cli.command()
@click.argument("uri")
@click.option(
    "--model-dir", default=".", show_default=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Local directory containing the model's config.yaml.",
)
@click.option(
    "--region", envvar="AWS_REGION",
    help="AWS region for the S3 bucket. Env: AWS_REGION.",
)
def restore(uri: str, model_dir: str, region: str | None) -> None:
    """Restore a DuckDB-backed model from URI (the output of vzctl backup).

    Re-runs IMPORT DATABASE against the backend, recreating every table
    captured at backup time. Existing tables of the same name are
    replaced — back up first if you need to keep them.
    """
    backend = _load_duckdb_backend(Path(model_dir))
    try:
        if uri.startswith("s3://"):
            _maybe_configure_s3(backend, region)
        click.echo(f"Restoring from {uri} …", err=True)
        backend.import_database(uri)
        click.echo("Restore complete.", err=True)
    finally:
        backend.close()


@cli.command()
@click.argument("model")
@click.argument("message")
@click.option(
    "--model-dir", default=None,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Path to the model directory. Defaults to $VZ_MODELS_DIR/<MODEL>.",
)
@click.option(
    "--format", "output_format", default="human",
    type=click.Choice(["human", "json", "events"]),
    help="Output format. 'human' is a readable summary; 'json' is the full "
         "ChatTurnResult; 'events' is the raw AG-UI event stream (one JSON per line).",
)
@click.option(
    "--history", "history_path", default=None,
    type=click.Path(exists=False, file_okay=True, dir_okay=False, writable=True),
    help="Path to a JSON file with prior turns. Each turn's ChatTurnResult "
         "(as emitted by --format json) is appended to the file; on the next "
         "invocation, --history <same file> replays the prior context. Enables "
         "multi-turn conversation from the CLI (agents like Claude Code can "
         "keep a session going by re-passing the same file).",
)
def chat(
    model: str,
    message: str,
    model_dir: str | None,
    output_format: str,
    history_path: str | None,
) -> None:
    """Run one chat turn against MODEL with MESSAGE, print the result.

    Bypasses the HTTP server — calls the agentic loop directly. Useful for
    agents (Claude Code, cron, CI) that need to interrogate the chat
    capability without a running API. Reads .env for LLM keys the same way
    the API does.

    Exits non-zero on tool failure or LLM error so a wrapping script can
    detect problems reliably.

    Examples::

        vzctl chat oliverfenton "top 5 activities by tss"
        vzctl chat iagai "dora clt trend last 12w" --format json
        vzctl chat oliverfenton "summarise my training" --format events

    Multi-turn::

        # Turn 1 — new session
        vzctl chat oliverfenton "summarise my training" --history session.json
        # Turn 2 — same file replays prior context
        vzctl chat oliverfenton "chart the summary" --history session.json
        # Turn 3
        vzctl chat oliverfenton "explain this" --history session.json
    """
    import json
    import os
    from pathlib import Path

    # Load .env so keys land in os.environ — the API's lifespan does this,
    # but the CLI runs standalone.
    try:
        from dotenv import load_dotenv
        for candidate in [Path(".env"), Path(__file__).resolve().parents[1] / ".env"]:
            if candidate.is_file():
                load_dotenv(candidate)
                break
    except ImportError:
        pass

    from api.services.chat.agui_stream import stream_turn
    from api.services.chat.service import chat_turn

    resolved_dir = Path(model_dir) if model_dir else (
        Path(os.environ["VZ_MODELS_DIR"]) / model
        if os.environ.get("VZ_MODELS_DIR")
        else Path("models") / model
    )
    if not resolved_dir.is_dir():
        click.echo(f"Model directory not found: {resolved_dir}", err=True)
        sys.exit(2)

    prior_history = _load_chat_history(history_path)

    if output_format == "events":
        # Emit each AG-UI event as one line of JSON — same shape the /chat/stream
        # endpoint sends over SSE, minus the "data: " prefix. Agents can parse
        # this to see tool calls stream in.
        for event in stream_turn(
            model_dir=resolved_dir, message=message, thread_id="cli",
            history=prior_history,
        ):
            click.echo(event.model_dump_json())
        return

    try:
        result = chat_turn(model_dir=resolved_dir, message=message,
                           history=prior_history)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"chat_turn raised: {type(exc).__name__}: {exc}", err=True)
        sys.exit(1)

    # Persist to the history file so the NEXT invocation replays context.
    if history_path:
        _append_chat_history(history_path, message, result)

    if output_format == "json":
        # Full ChatTurnResult as a dict — for agent consumption.
        payload = {
            "success": result.success,
            "error": result.error,
            "iterations": result.iterations,
            "title": result.title,
            "saved_view": result.saved_view,
            "inline_view": result.inline_view,
            "query_yaml": result.query_yaml,
            "view_yaml": result.view_yaml,
            "sql": result.sql,
            "trace": [
                {"name": t.name, "arguments": t.arguments, "success": t.success,
                 "summary": t.summary, "payload": t.payload}
                for t in result.trace
            ],
        }
        click.echo(json.dumps(payload, indent=2, default=str))
        sys.exit(0 if result.success else 1)

    # Human format — the default. Print a compact readable summary.
    click.echo(f"success   : {result.success}")
    click.echo(f"iterations: {result.iterations}")
    if result.title:
        click.echo(f"title     : {result.title}")
    click.echo(f"trace ({len(result.trace)}):")
    for step in result.trace:
        marker = "✓" if step.success else "✗"
        summary = step.summary.replace("\n", " ")[:80]
        click.echo(f"  {marker} {step.name}: {summary}")
    if result.error:
        click.echo(f"error     : {result.error}")
    if result.saved_view:
        click.echo(f"saved_view: {result.saved_view.get('name')}")
    if result.inline_view:
        vy = (result.inline_view.get("view_yaml") or "").splitlines()[:5]
        click.echo("inline_view (first 5 lines of view_yaml):")
        for line in vy:
            click.echo(f"  {line}")
    sys.exit(0 if result.success else 1)


def _load_chat_history(history_path: str | None) -> list[dict]:
    """Read prior turns from a session file. Missing file → empty history
    (first turn); malformed file → hard fail so a corrupt session doesn't
    silently amnesia."""
    import json
    if not history_path:
        return []
    path = Path(history_path)
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        turns = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        click.echo(f"History file {history_path} is not valid JSON: {exc}", err=True)
        sys.exit(2)
    if not isinstance(turns, list):
        click.echo(f"History file {history_path} must contain a JSON array.", err=True)
        sys.exit(2)
    return turns


def _append_chat_history(history_path: str, user_message: str, result) -> None:
    """Extend the session file with the turn we just ran, in the shape
    ``chat_turn`` expects on the next invocation. The full trace goes in
    so the LLM sees the same context it saw in-session — including the
    terminal tool's view payload that anchors "current view" logic."""
    import json
    import uuid
    path = Path(history_path)
    existing = _load_chat_history(history_path)

    existing.append({"role": "user", "content": user_message})

    tool_call_ids = [f"tc_{uuid.uuid4().hex[:12]}" for _ in result.trace]
    if result.trace:
        existing.append({
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": tc_id,
                    "type": "function",
                    "function": {"name": step.name,
                                 "arguments": json.dumps(step.arguments)},
                }
                for tc_id, step in zip(tool_call_ids, result.trace)
            ],
        })
        for tc_id, step in zip(tool_call_ids, result.trace):
            # Embed the view payload on the terminal tool's result so the
            # next turn's _extract_current_view finds it — same shape the
            # streaming layer emits over SSE.
            is_terminal = (
                step is result.trace[-1]
                and step.name in ("present_view", "run_saved_view")
                and (result.saved_view or result.inline_view)
            )
            if is_terminal:
                payload = (
                    {"kind": "saved_view", "payload": result.saved_view}
                    if result.saved_view
                    else {"kind": "inline_view", "payload": result.inline_view}
                )
                content = json.dumps(payload)
            else:
                content = step.summary or ("ok" if step.success else "failed")
            existing.append({"role": "tool", "tool_call_id": tc_id,
                             "content": content})
    # Final assistant text turn — always append even if empty, so the
    # role ordering (user → assistant tool_calls → tool → assistant text)
    # mirrors what the streaming loop produces mid-turn.
    caption = ""
    if result.saved_view:
        caption = result.saved_view.get("caption") or ""
    elif result.inline_view:
        caption = result.inline_view.get("caption") or ""
    existing.append({"role": "assistant", "content": caption})

    path.write_text(json.dumps(existing, indent=2, default=str))


if __name__ == "__main__":
    cli()
