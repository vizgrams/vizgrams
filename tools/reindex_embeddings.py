# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""reindex_embeddings — rebuild artifact embeddings (Epic 20 VG-230).

The indexer fires on save; this CLI handles backfill and recovery:

  - Onboarding a new model whose artifacts pre-date the indexer.
  - Forced re-embed after changing the embedding model or text builder.
  - Recovery after CH unavailability dropped some hook calls.

Usage::

    poetry run python -m tools.reindex_embeddings --model example
    poetry run python -m tools.reindex_embeddings --all
    poetry run python -m tools.reindex_embeddings --model example --kinds query view --force

``--force`` re-embeds even artifacts whose content hash is unchanged
(used after swapping the text builder so previously-skipped rows get
fresh embeddings).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import click

from core import metadata_db
from semantic.llm.embeddings import get_default_provider
from semantic.llm.embeddings.index import index_now
from semantic.llm.embeddings.store import INDEXED_ARTIFACT_TYPES, EmbeddingsStore


def _list_models() -> list[str]:
    base = Path(os.environ.get("VZ_MODELS_DIR", "models"))
    if not base.is_dir():
        return []
    return sorted(p.name for p in base.iterdir() if p.is_dir())


def _model_dir(model: str) -> Path:
    return Path(os.environ.get("VZ_MODELS_DIR", "models")) / model


def _list_artifacts(model_dir: Path, kinds: list[str]) -> list[tuple[str, str, str]]:
    """Return (kind, name, content) for every current artifact of the given kinds."""
    out: list[tuple[str, str, str]] = []
    for kind in kinds:
        for name in metadata_db.list_artifact_names(model_dir, kind):
            content = metadata_db.get_current_content(model_dir, kind, name)
            if content:
                out.append((kind, name, content))
    return out


@click.command()
@click.option("--model", multiple=True, help="Model id (repeatable). Use --all for every model.")
@click.option("--all", "all_models", is_flag=True, help="Reindex every model under VZ_MODELS_DIR.")
@click.option(
    "--kinds", multiple=True, default=INDEXED_ARTIFACT_TYPES, show_default=True,
    help="Artifact kinds to reindex.",
)
@click.option("--force", is_flag=True, help="Re-embed even when content hash is unchanged.")
@click.option("--verbose/--quiet", default=True)
def main(model, all_models, kinds, force, verbose):
    """Backfill / re-embed artifacts into the embeddings store."""
    provider = get_default_provider()
    if provider is None:
        click.echo("No embedding provider configured (set OPENAI_API_KEY).", err=True)
        sys.exit(2)
    store = EmbeddingsStore()
    try:
        store.ensure_schema()
    except Exception as exc:
        click.echo(f"ClickHouse store unavailable: {exc}", err=True)
        sys.exit(2)

    if all_models:
        models = _list_models()
    else:
        models = list(model)
    if not models:
        click.echo("No models selected — pass --model NAME or --all.", err=True)
        sys.exit(2)

    totals = {"indexed": 0, "skipped": 0, "failed": 0}

    for model_id in models:
        model_dir = _model_dir(model_id)
        if not model_dir.is_dir():
            click.echo(f"  WARN: {model_id} not found at {model_dir}", err=True)
            continue
        if verbose:
            click.echo(f"\n── {model_id} ──────────────────────────────")
        artifacts = _list_artifacts(model_dir, list(kinds))

        for kind, name, content in artifacts:
            if force:
                # Bypass the same-hash skip by deleting the existing row first.
                store.delete(model_id=model_id, artifact_type=kind, artifact_name=name)
            try:
                changed = index_now(
                    model_dir=model_dir, artifact_type=kind, name=name, content=content,
                    provider=provider, store=store,
                )
            except Exception as exc:
                totals["failed"] += 1
                click.echo(f"  ✗ {kind}/{name}: {exc}", err=True)
                continue
            if changed:
                totals["indexed"] += 1
                if verbose:
                    click.echo(f"  ✓ {kind}/{name}")
            else:
                totals["skipped"] += 1

    click.echo(
        f"\n{totals['indexed']} indexed · "
        f"{totals['skipped']} unchanged · {totals['failed']} failed",
        err=True,
    )
    if totals["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
