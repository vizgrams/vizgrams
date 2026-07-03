# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""DuckDB-backed store for artifact embeddings (Epic 20 VG-230).

Single cross-model table in a dedicated file (``embeddings.duckdb`` next
to ``batch.db`` / ``api.db``, path overridable via ``VZ_EMBEDDINGS_DB``):

  artifact_embeddings (
    model_id       TEXT,
    artifact_type  TEXT,       -- query / view / feature / entity / application
    artifact_name  TEXT,
    content_hash   TEXT,       -- sha256(embedding_text); skips re-embed when unchanged
    embed_model    TEXT,
    embed_dim      SMALLINT,
    embedding      FLOAT[],
    description    TEXT,       -- denormalised one-liner the LLM sees
    text_builder_version SMALLINT DEFAULT 1,
    indexed_at     TIMESTAMP,
    PRIMARY KEY (model_id, artifact_type, artifact_name, embed_model)
  )

Idempotent upserts via ``INSERT … ON CONFLICT DO UPDATE`` on the PK. This
replaces the ClickHouse ReplacingMergeTree design (which required a
running CH cluster we no longer operate). DuckDB is single-writer, so
concurrent embedding writes from different worker threads all funnel
through the same connection — this store owns one and callers serialise
via a lock. Since embeddings are indexed on a lightweight background
thread (see ``index.py``) with a single-worker pool by default, the
contention is minimal in practice.

Search uses DuckDB's ``list_cosine_similarity`` — brute force, linear in
catalog size. Fine for the ~10k-artifact scale we care about today. If
that ceiling becomes a problem, the schema is index-ready via DuckDB's
``vss`` extension (HNSW on FLOAT[] columns).
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# All artifact types that get embedded. Keep in sync with the per-kind
# text builders in ``index.py``.
INDEXED_ARTIFACT_TYPES = ("query", "view", "feature", "entity", "application")


@dataclass
class StoredEmbedding:
    """One row from the embeddings table."""

    model_id: str
    artifact_type: str
    artifact_name: str
    description: str
    distance: float | None = None  # populated on search; None on raw fetch


def content_hash(text: str) -> str:
    """SHA-256 of the embedding text — used as the dedupe key for upserts."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _default_db_path() -> Path:
    """Resolve where ``embeddings.duckdb`` lives.

    Order:
      1. ``VZ_EMBEDDINGS_DB`` env var — explicit override for tests / deploys.
      2. ``VZ_BASE_DIR/data/embeddings.duckdb`` — matches ``api.db`` layout.
      3. Repo-relative fallback for local dev.
    """
    env = os.environ.get("VZ_EMBEDDINGS_DB")
    if env:
        return Path(env)
    base = os.environ.get("VZ_BASE_DIR")
    if base:
        return Path(base) / "data" / "embeddings.duckdb"
    return Path(__file__).resolve().parents[3] / "data" / "embeddings.duckdb"


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class EmbeddingsStore:
    """DuckDB-backed ``artifact_embeddings`` table.

    Owns one connection and serialises writes through a lock — DuckDB is
    single-writer per connection, and the indexer runs on a background
    thread pool that could otherwise race.

    Methods are sync; callers that want async should wrap (the indexer
    runs ``upsert`` on a background thread; ``find`` is on the request
    path and is fast enough — ~10 ms for 10 k rows on the local file).
    """

    # Kept for compatibility with call sites that logged/read these.
    # Unused in the SQL now (DuckDB is a file, no database.table
    # qualification needed) but the reconcile + API startup log them.
    DATABASE = "vizgrams_meta"
    TABLE = "artifact_embeddings"

    def __init__(self, *, db_path: Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path is not None else _default_db_path()
        self._conn = None
        # ``upsert`` is called from a background thread; ``find`` and the
        # reconciler run on other threads. All of them touch one DuckDB
        # connection, so wrap every access.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_connected(self) -> None:
        if self._conn is not None:
            return
        import duckdb  # noqa: PLC0415
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path))

    def ensure_schema(self) -> None:
        """Create the embeddings table if it doesn't exist.

        Idempotent. Old rows written before a new text-builder version
        exists get their ``text_builder_version`` populated as 1 via the
        column default, which the reconciler picks up.
        """
        with self._lock:
            self._ensure_connected()
            self._conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
                "  model_id TEXT NOT NULL,"
                "  artifact_type TEXT NOT NULL,"
                "  artifact_name TEXT NOT NULL,"
                "  content_hash TEXT NOT NULL,"
                "  embed_model TEXT NOT NULL,"
                "  embed_dim SMALLINT NOT NULL,"
                "  embedding FLOAT[] NOT NULL,"
                "  description TEXT NOT NULL,"
                "  text_builder_version SMALLINT NOT NULL DEFAULT 1,"
                "  indexed_at TIMESTAMP NOT NULL,"
                "  PRIMARY KEY (model_id, artifact_type, artifact_name, embed_model)"
                ")"
            )

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def upsert(
        self,
        *,
        model_id: str,
        artifact_type: str,
        artifact_name: str,
        description: str,
        content_hash_val: str,
        embed_model: str,
        embedding: list[float],
        text_builder_version: int = 1,
    ) -> None:
        """Insert or replace one embedding row.

        On PK conflict (same model + type + name + embed_model), update
        every non-key column so a re-embed with a new text_builder or
        model swap fully replaces the row.
        """
        with self._lock:
            self._ensure_connected()
            now = datetime.now(UTC).replace(tzinfo=None)
            self._conn.execute(
                f"INSERT INTO {self.TABLE} "
                "(model_id, artifact_type, artifact_name, content_hash, "
                " embed_model, embed_dim, embedding, description, "
                " text_builder_version, indexed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (model_id, artifact_type, artifact_name, embed_model) "
                "DO UPDATE SET "
                "  content_hash = excluded.content_hash,"
                "  embed_dim = excluded.embed_dim,"
                "  embedding = excluded.embedding,"
                "  description = excluded.description,"
                "  text_builder_version = excluded.text_builder_version,"
                "  indexed_at = excluded.indexed_at",
                [
                    model_id, artifact_type, artifact_name, content_hash_val,
                    embed_model, len(embedding), embedding, description,
                    int(text_builder_version), now,
                ],
            )

    def current_hash(
        self, *, model_id: str, artifact_type: str, artifact_name: str, embed_model: str,
    ) -> str | None:
        """Return the current ``content_hash`` for an artifact, or None."""
        with self._lock:
            self._ensure_connected()
            row = self._conn.execute(
                f"SELECT content_hash FROM {self.TABLE} "
                "WHERE model_id = ? AND artifact_type = ? "
                "AND artifact_name = ? AND embed_model = ?",
                [model_id, artifact_type, artifact_name, embed_model],
            ).fetchone()
        return row[0] if row else None

    def find_outdated(
        self, *, model_id: str, embed_model: str, current_version: int,
    ) -> list[tuple[str, str]]:
        """Return ``(artifact_type, artifact_name)`` for rows older than ``current_version``.

        Used by ``reconcile.reconcile_model`` on startup to find embeddings
        that were written by an earlier text-builder and need refreshing.
        """
        with self._lock:
            self._ensure_connected()
            rows = self._conn.execute(
                f"SELECT artifact_type, artifact_name FROM {self.TABLE} "
                "WHERE model_id = ? AND embed_model = ? "
                "AND text_builder_version < ?",
                [model_id, embed_model, int(current_version)],
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def delete(
        self, *, model_id: str, artifact_type: str, artifact_name: str,
    ) -> None:
        """Hard-delete every embedding row for an artifact (all embed models)."""
        with self._lock:
            self._ensure_connected()
            self._conn.execute(
                f"DELETE FROM {self.TABLE} "
                "WHERE model_id = ? AND artifact_type = ? AND artifact_name = ?",
                [model_id, artifact_type, artifact_name],
            )

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def find(
        self,
        *,
        model_id: str,
        query_embedding: list[float],
        embed_model: str,
        artifact_types: list[str] | None = None,
        top_k: int = 5,
        max_distance: float | None = None,
    ) -> list[StoredEmbedding]:
        """Return the ``top_k`` artifacts closest to ``query_embedding``.

        Cosine distance = ``1 - cos(θ)``, so the range is 0 (identical
        direction) to 2 (opposite). ``max_distance`` filters out poor
        matches before returning; the orchestrator uses this as the
        "strong match" threshold for the reuse decision.
        """
        with self._lock:
            self._ensure_connected()

            # DuckDB's ``list_cosine_similarity`` returns cos(θ); distance
            # is 1 - similarity so smaller = more similar (matches the CH
            # backend's cosineDistance behaviour, so callers don't have to
            # flip the comparison after the port).
            #
            # Two positions bind ``query_embedding``: once in the SELECT
            # for the distance, once in the WHERE if ``max_distance`` is
            # set. Both need to see the same vector — we bind it twice
            # explicitly rather than trying to alias via a CTE, because
            # DuckDB's optimizer inlines short vectors anyway.
            select_bindings: list = [query_embedding]
            where_parts = ["model_id = ?", "embed_model = ?"]
            where_bindings: list = [model_id, embed_model]

            if artifact_types:
                # All values are trusted internal constants
                # (INDEXED_ARTIFACT_TYPES).
                placeholders = ", ".join("?" for _ in artifact_types)
                where_parts.append(f"artifact_type IN ({placeholders})")
                where_bindings.extend(list(artifact_types))

            if max_distance is not None:
                where_parts.append(
                    "1 - list_cosine_similarity(embedding, ?::FLOAT[]) <= ?"
                )
                where_bindings.append(query_embedding)
                where_bindings.append(float(max_distance))

            sql = (
                "SELECT artifact_type, artifact_name, description, "
                "       1 - list_cosine_similarity(embedding, ?::FLOAT[]) AS dist "
                f"FROM {self.TABLE} "
                f"WHERE {' AND '.join(where_parts)} "
                "ORDER BY dist ASC LIMIT ?"
            )
            all_params = select_bindings + where_bindings + [int(top_k)]
            rows = self._conn.execute(sql, all_params).fetchall()

        return [
            StoredEmbedding(
                model_id=model_id,
                artifact_type=r[0],
                artifact_name=r[1],
                description=r[2],
                distance=float(r[3]),
            )
            for r in rows
        ]
