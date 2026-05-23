# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Self-heal stale embeddings on app startup.

Each row in ``artifact_embeddings`` carries a ``text_builder_version``.
When ``TEXT_BUILDER_VERSION`` (in ``index.py``) is bumped — because we
changed how some artifact kind renders to embedding text — every row
written under the old version is now stale: the LLM would see
descriptions that don't match what new save-time embeds produce, so
search relevance drifts.

``reconcile_model`` asks the store for stale rows and re-embeds them
through ``index_now``. Cheap (one CH query per model + only the stale
artifacts hit OpenAI). Called once per model on app startup; subsequent
startups find nothing stale and skip.

Best-effort throughout: any failure is logged, never propagated — the
app must come up cleanly even if OpenAI is rate-limiting or CH is
unreachable for the reconciler.
"""

from __future__ import annotations

import logging
from pathlib import Path

from core import metadata_db
from semantic.llm.embeddings.index import TEXT_BUILDER_VERSION, index_now
from semantic.llm.embeddings.provider import EmbeddingProvider
from semantic.llm.embeddings.store import EmbeddingsStore

logger = logging.getLogger(__name__)


def reconcile_model(
    model_id: str,
    model_dir: Path,
    *,
    provider: EmbeddingProvider,
    store: EmbeddingsStore,
    current_version: int = TEXT_BUILDER_VERSION,
) -> dict:
    """Re-embed every artifact in ``model_id`` whose version is < ``current_version``.

    Returns a small report dict: ``{stale, reindexed, failed}``.
    """
    try:
        stale = store.find_outdated(
            model_id=model_id, embed_model=provider.model,
            current_version=current_version,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "reconcile_model(%s): could not query stale rows: %s",
            model_id, exc,
        )
        return {"stale": 0, "reindexed": 0, "failed": 0}

    if not stale:
        return {"stale": 0, "reindexed": 0, "failed": 0}

    logger.info(
        "Reconciling %d stale embedding(s) for model %r (text_builder_version → %d)",
        len(stale), model_id, current_version,
    )

    reindexed = 0
    failed = 0
    for kind, name in stale:
        try:
            content = metadata_db.get_current_content(model_dir, kind, name)
        except Exception:  # noqa: BLE001
            content = None
        if not content:
            # Artifact has been deleted in api.db but its embedding row
            # still exists. Best-effort cleanup.
            try:
                store.delete(
                    model_id=model_id, artifact_type=kind, artifact_name=name,
                )
            except Exception:  # noqa: BLE001
                failed += 1
            continue
        try:
            # force=True: bypass the hash short-circuit. Stale-by-version
            # rows whose rendered text happens to match the stored hash
            # still need to be re-written so the version stamp moves
            # forward. One OpenAI call per stale row is cheap pennies for
            # the typical ~100-artifact catalog.
            if index_now(
                model_dir=model_dir, artifact_type=kind, name=name,
                content=content, provider=provider, store=store, force=True,
            ):
                reindexed += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "reconcile_model(%s): re-embed %s/%s failed: %s",
                model_id, kind, name, exc,
            )
            failed += 1

    return {"stale": len(stale), "reindexed": reindexed, "failed": failed}


def reconcile_all_models(
    models_dir: Path,
    *,
    provider: EmbeddingProvider,
    store: EmbeddingsStore,
    current_version: int = TEXT_BUILDER_VERSION,
) -> dict:
    """Run ``reconcile_model`` for every model under ``models_dir``.

    Used by app startup. Returns aggregate totals plus a per-model
    breakdown so the lifespan logger can show a useful summary.
    """
    if not models_dir.is_dir():
        return {"total_stale": 0, "total_reindexed": 0, "models": {}}

    per_model: dict[str, dict] = {}
    total_stale = 0
    total_reindexed = 0
    total_failed = 0
    for child in sorted(models_dir.iterdir()):
        if not child.is_dir():
            continue
        report = reconcile_model(
            child.name, child,
            provider=provider, store=store, current_version=current_version,
        )
        per_model[child.name] = report
        total_stale += report.get("stale", 0)
        total_reindexed += report.get("reindexed", 0)
        total_failed += report.get("failed", 0)
    return {
        "total_stale": total_stale,
        "total_reindexed": total_reindexed,
        "total_failed": total_failed,
        "models": per_model,
    }
