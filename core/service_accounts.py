# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""service_accounts.py — service account credentials for machine-to-machine auth.

Service accounts let CI systems and other automated clients write to a single
model's artifacts without going through the interactive OIDC flow.  Each
account is scoped to one model and identified by a long random token; only
the SHA-256 hash of the token is stored.  The plaintext is returned exactly
once at creation.

DB: shares `api.db` with `metadata_db` and `vizgrams_db` — schema is created
on first connect (idempotent).

Token format
------------
``vzsa_<43 url-safe characters>`` — the ``vzsa_`` prefix makes tokens
greppable in logs and recognisable to secret scanners (mirrors the
``ghp_`` / ``ghs_`` convention used by GitHub).
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOKEN_PREFIX = "vzsa_"
# 32 random bytes → 43 chars after url-safe base64 encoding.
TOKEN_RANDOM_BYTES = 32

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS service_accounts (
    id           TEXT PRIMARY KEY,
    model_id     TEXT NOT NULL,
    name         TEXT NOT NULL,
    token_hash   TEXT NOT NULL UNIQUE,
    created_by   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    last_used_at TEXT,
    is_active    INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_sa_model
    ON service_accounts (model_id, is_active);
CREATE UNIQUE INDEX IF NOT EXISTS uix_sa_model_name
    ON service_accounts (model_id, name)
    WHERE is_active = 1;
"""


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@contextmanager
def _connect(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    from core.metadata_db import get_api_db_path
    path = get_api_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.executescript(_DDL)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Token primitives
# ---------------------------------------------------------------------------

def _generate_token() -> str:
    """Return a fresh service-account token. Plaintext — store the hash."""
    return TOKEN_PREFIX + secrets.token_urlsafe(TOKEN_RANDOM_BYTES)


def hash_token(token: str) -> str:
    """SHA-256 hex digest of *token*.  Used as the DB lookup key.

    No salt: the random portion is 256 bits of entropy, so collisions and
    pre-image attacks are not realistic threats.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _row_to_dict(row: sqlite3.Row, *, include_token_hash: bool = False) -> dict:
    d = dict(row)
    if not include_token_hash:
        d.pop("token_hash", None)
    d["is_active"] = bool(d["is_active"])
    return d


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_service_account(
    model_id: str,
    name: str,
    created_by: str,
    *,
    db_path: Path | None = None,
) -> dict:
    """Create a new service account for *model_id*.

    Returns a dict with the same fields as ``get_service_account`` plus a
    ``token`` field containing the plaintext token. The plaintext is shown
    here exactly once; subsequent reads return only metadata.
    """
    sa_id = str(uuid4())
    token = _generate_token()
    token_h = hash_token(token)
    now = _now()

    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO service_accounts
               (id, model_id, name, token_hash, created_by, created_at,
                last_used_at, is_active)
               VALUES (?, ?, ?, ?, ?, ?, NULL, 1)""",
            (sa_id, model_id, name, token_h, created_by, now),
        )

    return {
        "id": sa_id,
        "model_id": model_id,
        "name": name,
        "created_by": created_by,
        "created_at": now,
        "last_used_at": None,
        "is_active": True,
        "token": token,
    }


def verify_token(token: str, *, db_path: Path | None = None) -> dict | None:
    """Validate a plaintext token.

    Returns the service-account metadata dict (without ``token`` /
    ``token_hash``) on success and updates ``last_used_at``. Returns
    ``None`` if the token doesn't match an active row.
    """
    if not token or not token.startswith(TOKEN_PREFIX):
        return None
    token_h = hash_token(token)
    now = _now()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM service_accounts WHERE token_hash = ? AND is_active = 1",
            (token_h,),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE service_accounts SET last_used_at = ? WHERE id = ?",
            (now, row["id"]),
        )
    return _row_to_dict(row)


def list_service_accounts(
    model_id: str | None = None,
    *,
    include_inactive: bool = False,
    db_path: Path | None = None,
) -> list[dict]:
    """List service accounts, optionally scoped to one model. Tokens omitted."""
    sql = "SELECT * FROM service_accounts"
    params: list = []
    clauses: list[str] = []
    if model_id is not None:
        clauses.append("model_id = ?")
        params.append(model_id)
    if not include_inactive:
        clauses.append("is_active = 1")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_at DESC"
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_service_account(sa_id: str, *, db_path: Path | None = None) -> dict | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM service_accounts WHERE id = ?", (sa_id,)
        ).fetchone()
    return _row_to_dict(row) if row else None


def revoke_service_account(sa_id: str, *, db_path: Path | None = None) -> bool:
    """Soft-delete: mark inactive. Returns True if a row was affected."""
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE service_accounts SET is_active = 0 WHERE id = ? AND is_active = 1",
            (sa_id,),
        )
        return cur.rowcount > 0
