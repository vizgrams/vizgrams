# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Background indexer — wires ``metadata_db.record_version`` to the store.

Why background-thread instead of inline:
  - Each embed call is ~50-200ms (OpenAI round-trip). Synchronous would
    add that latency to every artifact-save endpoint, including bulk
    imports.
  - OpenAI / CH unavailability shouldn't fail the user's save. Indexing
    is best-effort; failures are logged and the artifact can be
    re-indexed later via ``tools/reindex_embeddings.py``.

The hook signature matches ``metadata_db._INDEX_HOOK`` (see
``metadata_db.set_index_hook``). It's a no-op until ``configure(...)``
is called at app startup with concrete provider + store implementations.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

from semantic.llm.embeddings.provider import EmbeddingProvider
from semantic.llm.embeddings.store import INDEXED_ARTIFACT_TYPES, EmbeddingsStore, content_hash

logger = logging.getLogger(__name__)

# Module-level state set up at app startup. Tests pass providers directly
# via ``index_now()``; production wires them once via ``configure()``.
_PROVIDER: EmbeddingProvider | None = None
_STORE: EmbeddingsStore | None = None
_EXECUTOR: ThreadPoolExecutor | None = None


def configure(*, provider: EmbeddingProvider, store: EmbeddingsStore, max_workers: int = 2) -> None:
    """Wire the indexer to a provider + store. Idempotent."""
    global _PROVIDER, _STORE, _EXECUTOR
    _PROVIDER = provider
    _STORE = store
    if _EXECUTOR is None:
        _EXECUTOR = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="embed-idx")


def shutdown() -> None:
    """Stop accepting new indexing work and wait for in-flight jobs."""
    global _EXECUTOR
    if _EXECUTOR is not None:
        _EXECUTOR.shutdown(wait=True)
        _EXECUTOR = None


# ---------------------------------------------------------------------------
# Per-kind text builders
# ---------------------------------------------------------------------------


def _safe_load(content: str) -> dict:
    try:
        body = yaml.safe_load(content)
    except yaml.YAMLError:
        return {}
    return body if isinstance(body, dict) else {}


def _measure_names(body: dict) -> list[str]:
    """Extract measure aliases from a query YAML body (list-of-dicts or dict shape)."""
    raw = body.get("measures")
    names: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                names.extend(str(k) for k in item)
    elif isinstance(raw, dict):
        names.extend(str(k) for k in raw)
    return names


def _measure_descriptors(body: dict) -> list[str]:
    """Extract ``alias=expr`` descriptors for each measure in a query YAML body.

    The bare alias isn't enough for an LLM consumer: when copying a
    measure from a catalog match, it needs to know the underlying field
    (which is in the ``expr``, not the alias). Renders as
    ``avg_clt_prd=avg(change_lead_time_prd)`` so the LLM can lift both
    the alias and the field cleanly.
    """
    raw = body.get("measures")
    out: list[str] = []

    def _render(alias: str, definition) -> str:
        if isinstance(definition, dict):
            expr = definition.get("expr")
            if expr:
                return f"{alias}={expr}"
        return str(alias)

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                for alias, definition in item.items():
                    out.append(_render(str(alias), definition))
    elif isinstance(raw, dict):
        for alias, definition in raw.items():
            out.append(_render(str(alias), definition))
    return out


def _entity_attr_names(body: dict) -> list[str]:
    """Pull key attribute names from an entity YAML body."""
    out: list[str] = []
    for section in ("identity", "attributes"):
        block = body.get(section) or {}
        if isinstance(block, dict):
            out.extend(block.keys())
    return out


def _query_text(name: str, body: dict) -> str:
    parts = [f"query {name}"]
    if d := body.get("description"):
        parts.append(str(d))
    if root := (body.get("root") or body.get("entity")):
        parts.append(f"root entity {root}")
    if measures := _measure_descriptors(body):
        parts.append(f"measures: {', '.join(measures)}")
    where = body.get("where")
    if isinstance(where, list) and where:
        parts.append(f"filters: {'; '.join(str(w) for w in where)}")
    return ". ".join(parts)


def _view_text(name: str, body: dict) -> str:
    parts = [f"view {name}"]
    if d := body.get("description") or body.get("caption"):
        parts.append(str(d))
    if t := body.get("type"):
        parts.append(f"type {t}")
    if q := body.get("query"):
        parts.append(f"on query {q}")
    viz = body.get("visualization") or {}
    if isinstance(viz, dict):
        if ct := viz.get("chart_type"):
            parts.append(f"chart {ct}")
        if cols := viz.get("columns"):
            parts.append(f"columns: {', '.join(str(c) for c in cols)}")
    return ". ".join(parts)


def _feature_text(name: str, body: dict) -> str:
    fid = body.get("feature_id") or name
    parts = [f"feature {fid}"]
    if d := body.get("description"):
        parts.append(str(d))
    if e := body.get("entity_type"):
        parts.append(f"on entity {e}")
    if expr := (body.get("expr") or body.get("raw_sql") or body.get("expression")):
        parts.append(f"expression {expr}")
    return ". ".join(parts)


def _entity_text(name: str, body: dict) -> str:
    parts = [f"entity {name}"]
    if d := body.get("description"):
        parts.append(str(d))
    if attrs := _entity_attr_names(body):
        parts.append(f"attributes: {', '.join(attrs)}")
    rels = body.get("relations")
    if isinstance(rels, dict) and rels:
        parts.append(f"relations: {', '.join(rels.keys())}")
    return ". ".join(parts)


def _application_text(name: str, body: dict) -> str:
    parts = [f"application {name}"]
    if d := body.get("description"):
        parts.append(str(d))
    views = body.get("views")
    if isinstance(views, list) and views:
        parts.append(f"views: {', '.join(str(v) for v in views)}")
    return ". ".join(parts)


_TEXT_BUILDERS = {
    "query": _query_text,
    "view": _view_text,
    "feature": _feature_text,
    "entity": _entity_text,
    "application": _application_text,
}


def build_embedding_text(artifact_type: str, name: str, content: str) -> str:
    """Render the embedding-ready text for one artifact.

    Exposed so the reindex CLI + tests can produce the same text the
    indexer would, without needing a real embedding provider.
    """
    body = _safe_load(content)
    builder = _TEXT_BUILDERS.get(artifact_type)
    if not builder:
        return f"{artifact_type} {name}"
    return builder(name, body)


# ---------------------------------------------------------------------------
# Indexing entry points
# ---------------------------------------------------------------------------


def index_now(
    *,
    model_dir: Path | str,
    artifact_type: str,
    name: str,
    content: str,
    provider: EmbeddingProvider | None = None,
    store: EmbeddingsStore | None = None,
) -> bool:
    """Synchronous index of one artifact. Returns True on success.

    Used by the reindex CLI and by tests; production goes via
    ``index_artifact_async``. Both end up here.
    """
    provider = provider or _PROVIDER
    store = store or _STORE
    if provider is None or store is None:
        logger.debug("Embeddings not configured — skipping index of %s/%s", artifact_type, name)
        return False
    if artifact_type not in INDEXED_ARTIFACT_TYPES:
        return False

    model_id = Path(model_dir).name
    text = build_embedding_text(artifact_type, name, content)
    text_hash = content_hash(text)

    try:
        existing = store.current_hash(
            model_id=model_id, artifact_type=artifact_type,
            artifact_name=name, embed_model=provider.model,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not check existing embedding hash: %s", exc)
        existing = None

    if existing == text_hash:
        return False  # unchanged — skip re-embed

    try:
        embedding = provider.embed_one(text).vector
        store.upsert(
            model_id=model_id, artifact_type=artifact_type,
            artifact_name=name, description=text,
            content_hash_val=text_hash, embed_model=provider.model,
            embedding=embedding,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to index %s/%s for model %s: %s",
            artifact_type, name, model_id, exc,
        )
        return False


def index_artifact_async(
    model_dir: Path | str, artifact_type: str, name: str, content: str,
) -> None:
    """Async hook fired by ``metadata_db.record_version``. Fire-and-forget."""
    if _EXECUTOR is None or _PROVIDER is None or _STORE is None:
        return
    _EXECUTOR.submit(
        index_now,
        model_dir=model_dir, artifact_type=artifact_type,
        name=name, content=content,
    )


def remove_artifact(*, model_id: str, artifact_type: str, name: str) -> None:
    """Delete every embedding row for an artifact. Sync (rare, small)."""
    if _STORE is None:
        return
    try:
        _STORE.delete(model_id=model_id, artifact_type=artifact_type, artifact_name=name)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to delete embedding rows for %s/%s: %s", artifact_type, name, exc)
