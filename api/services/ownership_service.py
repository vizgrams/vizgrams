# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Ownership API helpers (VG-250 / VG-252).

Thin wrappers that build the ownership fields included on view, query,
and feature responses. The DB layer (``core/metadata_db``) records the
raw ``created_by`` UUID; this module joins against the ``users`` table
in ``vizgrams_db`` to produce a display name.

Mirrors the shape of ``certification_service.py`` so the two payloads
look the same in the service layer.
"""

from __future__ import annotations

from pathlib import Path

from core import metadata_db
from core.vizgrams_db import get_user_display_name


def owner_payload(owner: dict | None) -> dict:
    """Four-field ownership payload from a raw ``get_owner`` row."""
    if owner is None:
        return _empty()
    user_id = owner.get("created_by")
    display = get_user_display_name(user_id) if user_id else None
    return {
        "created_by": user_id,
        "created_by_display": display,
        "created_via": owner.get("created_via"),
        "created_at": owner.get("created_at"),
    }


def get_owner_payload(model_dir: Path, artifact_type: str, name: str) -> dict:
    """Single-artifact lookup. Use for get_view / get_query / get_feature."""
    return owner_payload(metadata_db.get_owner(model_dir, artifact_type, name))


def list_owner_payloads(
    model_dir: Path, artifact_type: str,
) -> dict[str, dict]:
    """Batched lookup keyed by ``name``. Use in list endpoints to avoid N+1."""
    rows = metadata_db.list_owners(model_dir, artifact_type)
    return {name: owner_payload(row) for name, row in rows.items()}


def _empty() -> dict:
    return {
        "created_by": None,
        "created_by_display": None,
        "created_via": None,
        "created_at": None,
    }
