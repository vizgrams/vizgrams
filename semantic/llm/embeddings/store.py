# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""ClickHouse-backed store for artifact embeddings (Epic 20 VG-230).

Single cross-model table in a dedicated system database
(``vizgrams_meta``):

  artifact_embeddings (
    model_id       String,
    artifact_type  LowCardinality(String),   -- query / view / feature / entity / application
    artifact_name  String,
    content_hash   String,                    -- sha256(embedding_text); skips re-embed when unchanged
    embed_model    LowCardinality(String),
    embed_dim      UInt16,
    embedding      Array(Float32),
    description    String,                    -- denormalised one-liner the LLM sees
    indexed_at     DateTime
  ) ENGINE = ReplacingMergeTree(indexed_at)
  ORDER BY (model_id, artifact_type, artifact_name, embed_model);

ReplacingMergeTree gives us idempotent upserts: re-embedding an artifact
replaces the previous row, with eventual de-dup via the engine. Reads use
``FINAL`` to guarantee a single row per artifact during the read window.

Search uses ClickHouse's exact ``cosineDistance`` function. Brute force,
linear in catalog size — fine for the ~10k-artifact scale we care about
today. The schema is index-ready (HNSW via ``vector_similarity``) when
that ceiling becomes a problem; the search query doesn't change.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime

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


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class EmbeddingsStore:
    """ClickHouse-backed ``artifact_embeddings`` table.

    Owns its own ``ClickHouseBackend`` connection — the cross-model
    ``vizgrams_meta`` database isn't tied to any specific model_dir, so
    we can't piggyback on ``core.db.get_backend``.

    Methods are sync; callers that want async should wrap (the indexer
    runs ``upsert`` on a background thread; ``find`` is on the request
    path and is fast enough — ~10ms for 10k rows on a local CH).
    """

    DATABASE = "vizgrams_meta"
    TABLE = "artifact_embeddings"

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        # Defaults match the rest of the stack — env vars first, sensible
        # local-dev defaults second.
        self.host = host or os.environ.get("CLICKHOUSE_HOST", "localhost")
        self.port = int(port or os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.username = username or os.environ.get("CLICKHOUSE_USER", "default")
        self.password = password or os.environ.get("CLICKHOUSE_PASSWORD", "")
        self._backend = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_connected(self) -> None:
        if self._backend is not None:
            return
        from core.db import ClickHouseBackend
        backend = ClickHouseBackend(
            host=self.host, port=self.port, database=self.DATABASE,
            username=self.username, password=self.password,
        )
        backend.connect()
        self._backend = backend

    def ensure_schema(self) -> None:
        """Create the embeddings table if it doesn't exist. Idempotent."""
        self._ensure_connected()
        sql = (
            f"CREATE TABLE IF NOT EXISTS `{self.DATABASE}`.`{self.TABLE}` ("
            "  model_id String,"
            "  artifact_type LowCardinality(String),"
            "  artifact_name String,"
            "  content_hash String,"
            "  embed_model LowCardinality(String),"
            "  embed_dim UInt16,"
            "  embedding Array(Float32),"
            "  description String,"
            "  indexed_at DateTime"
            ") ENGINE = ReplacingMergeTree(indexed_at) "
            "ORDER BY (model_id, artifact_type, artifact_name, embed_model)"
        )
        self._backend.execute(sql)

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()
            self._backend = None

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
    ) -> None:
        """Insert one embedding row. ReplacingMergeTree dedupes on the ORDER BY key."""
        self._ensure_connected()
        client = self._backend._client  # noqa: SLF001 — adapter is intentionally thin
        client.insert(
            self.TABLE,
            [[
                model_id, artifact_type, artifact_name, content_hash_val,
                embed_model, len(embedding), embedding, description,
                datetime.now(UTC).replace(tzinfo=None),
            ]],
            column_names=[
                "model_id", "artifact_type", "artifact_name", "content_hash",
                "embed_model", "embed_dim", "embedding", "description",
                "indexed_at",
            ],
        )

    def current_hash(
        self, *, model_id: str, artifact_type: str, artifact_name: str, embed_model: str,
    ) -> str | None:
        """Return the most recently indexed ``content_hash`` for an artifact, or None."""
        self._ensure_connected()
        client = self._backend._client  # noqa: SLF001
        rows = client.query(
            f"SELECT content_hash FROM `{self.DATABASE}`.`{self.TABLE}` FINAL "
            "WHERE model_id = {m:String} AND artifact_type = {t:String} "
            "AND artifact_name = {n:String} AND embed_model = {em:String} LIMIT 1",
            parameters={"m": model_id, "t": artifact_type, "n": artifact_name, "em": embed_model},
        ).result_rows
        return rows[0][0] if rows else None

    def delete(
        self, *, model_id: str, artifact_type: str, artifact_name: str,
    ) -> None:
        """Hard-delete every embedding row for an artifact (all models)."""
        self._ensure_connected()
        client = self._backend._client  # noqa: SLF001
        # ALTER TABLE … DELETE is async; for our scale that's fine.
        client.command(
            f"ALTER TABLE `{self.DATABASE}`.`{self.TABLE}` DELETE "
            "WHERE model_id = {m:String} AND artifact_type = {t:String} "
            "AND artifact_name = {n:String}",
            parameters={"m": model_id, "t": artifact_type, "n": artifact_name},
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

        ``max_distance`` (cosine, 0..2 — smaller = more similar) filters
        out poor matches before returning. The orchestrator uses this as
        the "strong match" threshold for the reuse decision.
        """
        self._ensure_connected()
        client = self._backend._client  # noqa: SLF001

        kind_clause = ""
        params: dict = {
            "m": model_id, "em": embed_model,
            "k": int(top_k), "vec": query_embedding,
        }
        if artifact_types:
            kind_clause = "AND artifact_type IN {types:Array(String)}"
            params["types"] = list(artifact_types)
        dist_clause = ""
        if max_distance is not None:
            dist_clause = "AND cosineDistance(embedding, {vec:Array(Float32)}) <= {maxd:Float64}"
            params["maxd"] = float(max_distance)

        sql = (
            "SELECT artifact_type, artifact_name, description, "
            "       cosineDistance(embedding, {vec:Array(Float32)}) AS dist "
            f"FROM `{self.DATABASE}`.`{self.TABLE}` FINAL "
            "WHERE model_id = {m:String} AND embed_model = {em:String} "
            f"{kind_clause} {dist_clause} "
            "ORDER BY dist ASC LIMIT {k:UInt32}"
        )
        rows = client.query(sql, parameters=params).result_rows
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
